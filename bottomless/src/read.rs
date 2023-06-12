use crate::wal::WalFrameHeader;
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub(crate) struct BatchReader {
    next_frame_no: u32,
    page_size: usize,
    use_compression: bool,
    reader: BufReader<StreamReader<ByteStream, Bytes>>,
}

impl BatchReader {
    pub fn new(
        key: &str,
        content: ByteStream,
        page_size: usize,
        use_compression: bool,
    ) -> Result<Self> {
        if let Some(init_frame_no) = Self::parse_frame(key) {
            let frame_size = page_size + WalFrameHeader::SIZE as usize;
            Ok(BatchReader {
                next_frame_no: init_frame_no,
                page_size,
                use_compression,
                reader: BufReader::with_capacity(frame_size, StreamReader::new(content)),
            })
        } else {
            Err(anyhow!("Failed to parse frame batch: '{}'", key))
        }
    }

    // Parses the frame and page number from given key.
    // Format: <db-name>-<generation>/<first-frame-number>
    fn parse_frame(key: &str) -> Option<u32> {
        let frame_delim = key.rfind('/')?;
        let frame_no = key[(frame_delim + 1)..].parse::<u32>().ok()?;
        Some(frame_no)
    }

    /// Reads next frame header without frame body (WAL page).
    pub(crate) async fn next_frame_header(&mut self) -> Result<Option<WalFrameHeader>> {
        let mut buf = [0u8; WalFrameHeader::SIZE as usize];
        let res = if self.use_compression {
            let mut gzip = GzipDecoder::new(&mut self.reader);
            gzip.read_exact(&mut buf).await
        } else {
            self.reader.read_exact(&mut buf).await
        };
        match res {
            Ok(_) => Ok(Some(WalFrameHeader::from(buf))),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the next frame stored in a current batch.
    /// Returns a frame number or `None` if no frame was remaining in the buffer.
    pub(crate) async fn read_page<W>(&mut self, db_file: &mut W) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let mut page = Vec::with_capacity(self.page_size);
        unsafe { page.set_len(self.page_size) }; // we require to fill entire page anyway
        if self.use_compression {
            let mut gzip = GzipDecoder::new(&mut self.reader);
            gzip.read_exact(&mut page).await?;
        } else {
            self.reader.read_exact(&mut page).await?;
        };
        db_file.write_all(&page).await?;
        self.next_frame_no += 1;
        Ok(())
    }
}
