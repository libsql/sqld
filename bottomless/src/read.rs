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
    page_buf: Box<[u8]>, // Box over Vec to be sure its size won't change
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
        let mut buf = Vec::with_capacity(page_size);
        unsafe { buf.set_len(page_size) };
        let verify_crc = prev_crc.is_some();
        BatchReader {
            next_frame_no: init_frame_no,
            page_buf: buf.into_boxed_slice(),
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

    fn page_size(&self) -> u64 {
        self.page_buf.len() as u64
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
    pub(crate) async fn restore_page<W>(&mut self, pgno: u32, db_file: &mut W) -> Result<()>
    where
        W: AsyncWrite + AsyncSeek + Unpin,
    {
        self.reader.read_exact(&mut self.page_buf).await?;
        if self.verify_crc {
            let crc = checksum_be(self.prev_crc, &self.page_buf);
            if self.curr_crc == crc {
                self.prev_crc = crc;
            } else {
                return Err(anyhow!(
                    "Checksum verification failed for page: {}. Expected: {}, got: {}",
                    pgno,
                    self.curr_crc,
                    crc
                ));
            }
        }
        let offset = (pgno - 1) as u64 * self.page_size();
        db_file.seek(SeekFrom::Start(offset)).await?;
        db_file.write_all(&self.page_buf).await?;
        self.next_frame_no += 1;
        Ok(())
    }
}
