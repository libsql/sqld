use crate::wal::{crc64, WalFrameHeader};
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::TryStreamExt;
use std::io::{Cursor, ErrorKind};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub(crate) struct BatchReader {
    next_frame_no: u32,
    last_crc: u64,
    use_compression: bool,
    verify_crc: bool,
    reader: BufReader<StreamReader<ByteStream, Bytes>>,
}

impl BatchReader {
    pub fn new(
        key: &str,
        content: ByteStream,
        use_compression: bool,
        verify_crc: bool,
    ) -> Result<Self> {
        if let Some((init_frame_no, init_crc)) = Self::parse_frame_crc(key) {
            Ok(BatchReader {
                next_frame_no: init_frame_no,
                last_crc: init_crc,
                use_compression,
                verify_crc,
                reader: BufReader::new(StreamReader::new(content)),
            })
        } else {
            Err(anyhow!("Failed to parse frame batch: '{}'", key))
        }
    }

    // Parses the frame and page number from given key.
    // Format: <db-name>-<generation>/<frame-number>-<prev-crc64>
    fn parse_frame_crc(key: &str) -> Option<(u32, u64)> {
        let checksum_delim = key.rfind('-')?;
        let frame_delim = key[0..checksum_delim].rfind('/')?;
        let frame_no = key[frame_delim + 1..checksum_delim].parse::<u32>().ok()?;
        let crc = u64::from_str_radix(&key[checksum_delim + 1..], 16).ok()?;
        Some((frame_no, crc))
    }

    /// Reads the next page stored in a current batch. Provided `page_buffer` length must the
    /// the same as expected page size.
    pub async fn next_frame(&mut self, page_buffer: &mut [u8]) -> Result<Option<WalFrameHeader>> {
        let mut frame_header = [0u8; WalFrameHeader::SIZE as usize];
        let header = match self.reader.read_exact(frame_header.as_mut()).await {
            Ok(_) => WalFrameHeader::from(frame_header),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let result = if !self.use_compression {
            self.reader.read_exact(page_buffer).await
        } else {
            let mut gzip = GzipDecoder::new(&mut self.reader);
            gzip.read_exact(page_buffer).await
        };
        match result {
            Ok(n) => {
                if n != page_buffer.len() {
                    return Err(anyhow!(
                        "read non page aligned number of bytes: {}. Page size: {}",
                        n,
                        page_buffer.len()
                    ));
                }
                if self.verify_crc {
                    let crc = crc64(self.last_crc, page_buffer);
                    if crc != header.crc {
                        return Err(anyhow!(
                            "Checksum verification failed for frame {}. Expected: {}. Actual: {}",
                            self.next_frame_no,
                            header.crc,
                            crc,
                        ));
                    }
                }
                self.last_crc = header.crc;
                self.next_frame_no += 1;
                Ok(Some(header))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
