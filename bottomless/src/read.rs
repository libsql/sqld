use crate::wal::WalFrameHeader;
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use std::io::{ErrorKind, SeekFrom};
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub(crate) struct BatchReader {
    next_frame_no: u32,
    use_compression: bool,
    page_buf: Box<[u8]>, // Box over Vec to be sure its size won't change
    reader: BufReader<StreamReader<ByteStream, Bytes>>,
}

impl BatchReader {
    pub fn new(
        init_frame_no: u32,
        content: ByteStream,
        page_size: usize,
        use_compression: bool,
    ) -> Self {
        let mut buf = Vec::with_capacity(page_size);
        unsafe { buf.set_len(page_size) };
        BatchReader {
            next_frame_no: init_frame_no,
            page_buf: buf.into_boxed_slice(),
            use_compression,
            reader: BufReader::with_capacity(
                page_size + WalFrameHeader::SIZE as usize,
                StreamReader::new(content),
            ),
        }
    }

    fn page_size(&self) -> u64 {
        self.page_buf.len() as u64
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
    pub(crate) async fn restore_page<W>(&mut self, pgno: u32, db_file: &mut W) -> Result<()>
    where
        W: AsyncWrite + AsyncSeek + Unpin,
    {
        if self.use_compression {
            let mut gzip = GzipDecoder::new(&mut self.reader);
            gzip.read_exact(&mut self.page_buf).await?;
        } else {
            self.reader.read_exact(&mut self.page_buf).await?;
        };
        let offset = (pgno - 1) as u64 * self.page_size();
        db_file.seek(SeekFrom::Start(offset)).await?;
        db_file.write_all(&self.page_buf).await?;
        self.next_frame_no += 1;
        Ok(())
    }
}
