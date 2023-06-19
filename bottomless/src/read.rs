use crate::wal::checksum_be;
use crate::wal::WalFrameHeader;
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::primitives::ByteStream;
use std::io::{ErrorKind, SeekFrom};
use std::pin::Pin;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio_util::io::StreamReader;

type AsyncByteReader = dyn AsyncRead + Send + Sync;

pub(crate) struct BatchReader {
    reader: Pin<Box<AsyncByteReader>>,
    prev_crc: u64,
    curr_crc: u64,
    next_frame_no: u32,
    verify_crc: bool,
}

impl BatchReader {
    pub fn new(
        init_frame_no: u32,
        content: ByteStream,
        page_size: usize,
        use_compression: bool,
        prev_crc: Option<u64>,
    ) -> Self {
        let verify_crc = prev_crc.is_some();
        BatchReader {
            next_frame_no: init_frame_no,
            verify_crc,
            prev_crc: prev_crc.unwrap_or(0),
            curr_crc: 0,
            reader: if use_compression {
                let gzip = GzipDecoder::new(BufReader::with_capacity(
                    page_size + WalFrameHeader::SIZE as usize,
                    StreamReader::new(content),
                ));
                Box::pin(gzip)
            } else {
                Box::pin(BufReader::with_capacity(
                    page_size + WalFrameHeader::SIZE as usize,
                    StreamReader::new(content),
                ))
            },
        }
    }

    /// Reads next frame header without frame body (WAL page).
    pub(crate) async fn next_frame_header(&mut self) -> Result<Option<WalFrameHeader>> {
        let mut buf = [0u8; WalFrameHeader::SIZE as usize];
        let res = self.reader.read_exact(&mut buf).await;
        match res {
            Ok(_) => {
                let header = WalFrameHeader::from(buf);
                self.curr_crc = header.crc;
                Ok(Some(header))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the next frame stored in a current batch.
    /// Returns a frame number or `None` if no frame was remaining in the buffer.
    pub(crate) async fn next_page(&mut self, page_buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(page_buf).await?;
        if self.verify_crc {
            let crc = checksum_be(self.prev_crc, page_buf);
            if self.curr_crc == crc {
                self.prev_crc = crc;
            } else {
                return Err(anyhow!(
                    "Frame {} checksum verification failed. Expected: {}, got: {}",
                    self.next_frame_no,
                    self.curr_crc,
                    crc
                ));
            }
        }
        self.next_frame_no += 1;
        Ok(())
    }
}
