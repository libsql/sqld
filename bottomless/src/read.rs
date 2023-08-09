use crate::replicator::CompressionKind;
use crate::wal::WalFrameHeader;
use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use std::io::ErrorKind;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

type AsyncByteReader = dyn AsyncRead + Send + Sync;

pub(crate) struct BatchReader {
    reader: Pin<Box<AsyncByteReader>>,
    next_frame_no: u32,
}

impl BatchReader {
    pub fn new(
        init_frame_no: u32,
        content: ByteStream,
        page_size: usize,
        use_compression: CompressionKind,
    ) -> Self {
        let reader =
            BufReader::with_capacity(page_size + WalFrameHeader::SIZE, StreamReader::new(content));
        BatchReader {
            next_frame_no: init_frame_no,
            reader: match use_compression {
                CompressionKind::None => Box::pin(reader),
                CompressionKind::Gzip => {
                    let gzip = GzipDecoder::new(reader);
                    Box::pin(gzip)
                }
            },
        }
    }

    /// Reads next frame header without frame body (WAL page).
    pub(crate) async fn next_frame_header(&mut self) -> Result<Option<WalFrameHeader>> {
        let mut buf = [0u8; WalFrameHeader::SIZE];
        let res = self.reader.read_exact(&mut buf).await;
        match res {
            Ok(_) => Ok(Some(WalFrameHeader::from(buf))),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the next frame stored in a current batch.
    /// Returns a frame number or `None` if no frame was remaining in the buffer.
    pub(crate) async fn next_page(&mut self, page_buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(page_buf).await?;
        self.next_frame_no += 1;
        Ok(())
    }
}

pub async fn upload_s3_multipart(
    client: &aws_sdk_s3::Client,
    key: &str,
    bucket: &str,
    reader: impl AsyncRead + Unpin,
) -> Result<()> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .upload_id
        .ok_or_else(|| anyhow::anyhow!("missing upload_id"))?;

    let parts = upload_s3_parts(client, upload_id.as_str(), key, bucket, reader).await;

    match parts {
        Ok(parts) => {
            client
                .complete_multipart_upload()
                .upload_id(upload_id)
                .bucket(bucket)
                .key(key)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await?;

            Ok(())
        }
        Err(err) => {
            client
                .abort_multipart_upload()
                .upload_id(upload_id)
                .bucket(bucket)
                .key(key)
                .send()
                .await?;

            Err(err)
        }
    }
}

async fn upload_s3_parts(
    client: &aws_sdk_s3::Client,
    upload_id: &str,
    key: &str,
    bucket: &str,
    mut reader: impl AsyncRead + Unpin,
) -> Result<Vec<CompletedPart>> {
    let chunk_sizes = &[
        5 * 1024 * 1024,
        10 * 1024 * 1024,
        25 * 1024 * 1024,
        50 * 1024 * 1024,
        100 * 1024 * 1024,
    ];

    const LAST_PART: i32 = 10_000;
    let mut parts = Vec::new();
    let mut has_reached_eof = false;

    // S3 allows a maximum of 10_000 parts and each part can size from 5 MiB to
    // 5 GiB, except for the last one that has no limits.
    //
    // See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    for part in 0..LAST_PART - 1 {
        // Progressively increase the chunk size every 16 chunks up to the last
        // chunk_size. This allows smaller allocations for small databases.
        //
        // Here's a table of how much data we can chunk:
        // ┌────────────┬──────────────────┬───────────────────────┬──────────────────┐
        // │ Chunk size │ Number of chunks │ Amount for chunk size │ Cumulative total │
        // ├────────────┼──────────────────┼───────────────────────┼──────────────────┤
        // │ 5 MiB      │ 16               │ 80 MiB                │ 80 MiB           │
        // ├────────────┼──────────────────┼───────────────────────┼──────────────────┤
        // │ 10 MiB     │ 16               │ 160 MiB               │ 240 MiB          │
        // ├────────────┼──────────────────┼───────────────────────┼──────────────────┤
        // │ 25 MiB     │ 16               │ 400 MiB               │ 640 MiB          │
        // ├────────────┼──────────────────┼───────────────────────┼──────────────────┤
        // │ 50 MiB     │ 16               │ 800 MiB               │ 1.406 GiB        │
        // ├────────────┼──────────────────┼───────────────────────┼──────────────────┤
        // │ 100 MiB    │ 9935             │ 970.215 GiB           │ 971.621 GiB      │
        // └────────────┴──────────────────┴───────────────────────┴──────────────────┘
        //
        // We can send up to 971 GiB in chunks, which is more than enough for the
        // majority of use cases.
        //
        // The last chunk is reserved for the remaining of the `gzip_reader`
        let chunk_size = chunk_sizes[((part / 16) as usize).min(chunk_sizes.len() - 1)];

        let mut buffer = bytes::BytesMut::with_capacity(chunk_size);
        loop {
            let bytes_written = reader.read_buf(&mut buffer).await?;
            // EOF or buffer is full
            if bytes_written == 0 {
                break;
            }
        }

        // EOF
        if buffer.is_empty() {
            has_reached_eof = true;
            break;
        }

        let part_out = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(ByteStream::from(buffer.freeze()))
            .part_number(part + 1)
            .send()
            .await?;

        parts.push(
            CompletedPart::builder()
                .part_number(part + 1)
                .e_tag(
                    part_out
                        .e_tag
                        .ok_or_else(|| anyhow::anyhow!("e_tag missing from part upload"))?,
                )
                .build(),
        );
    }

    // If the gzip stream has not reached EOF we need to send the last part to S3.
    // Since we don't know the size of the stream and we can't be sure if it fits in
    // memory, we save it into a file to allow streaming.
    //
    // This would only happen to databases that are around ~1 TiB.
    if !has_reached_eof {
        let last_chunk_file = tempfile::NamedTempFile::new()?;
        let mut last_chunk_tokio_file =
            tokio::fs::File::from_std(last_chunk_file.as_file().try_clone()?);

        tokio::io::copy(&mut reader, &mut last_chunk_tokio_file).await?;

        let part_out = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(ByteStream::from_path(last_chunk_file.path()).await?)
            .part_number(LAST_PART)
            .send()
            .await?;

        parts.push(
            CompletedPart::builder()
                .part_number(LAST_PART)
                .e_tag(
                    part_out
                        .e_tag
                        .ok_or_else(|| anyhow::anyhow!("e_tag missing from part upload"))?,
                )
                .build(),
        );
    }

    Ok(parts)
}
