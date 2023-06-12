use anyhow::{anyhow, Result};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const CRC_64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);

pub(crate) fn crc64(prev_crc: u64, data: &[u8]) -> u64 {
    let mut crc = CRC_64.digest_with_initial(prev_crc);
    crc.update(data);
    crc.finalize()
}

#[repr(C)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WalFrameHeader {
    /// Page number
    pub pgno: u32,
    /// For commit records, the size of the database image in pages
    /// after the commit. For all other records, zero.
    pub size_after: u32,
    pub salt: u64,
    pub crc: u64,
}

impl WalFrameHeader {
    pub const SIZE: u64 = 24;
}

impl From<[u8; WalFrameHeader::SIZE as usize]> for WalFrameHeader {
    fn from(value: [u8; WalFrameHeader::SIZE as usize]) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl Into<[u8; WalFrameHeader::SIZE as usize]> for WalFrameHeader {
    fn into(self) -> [u8; WalFrameHeader::SIZE as usize] {
        unsafe { std::mem::transmute(self) }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WalHeader {
    /// Magic number. 0x377f0682 or 0x377f0683
    magic_no: u32,
    /// File format version. Currently 3007000
    version: u32,
    /// Database page size.
    page_size: u32,
    /// Checkpoint sequence number
    checkpoint_seq_no: u32,
    /// Random integer incremented with each checkpoint
    salt_1: u32,
    /// A different random integer changing with each checkpoint
    salt_2: u32,
    /// Checksum for first 24 bytes of header
    crc: u64,
}

impl WalHeader {
    pub const SIZE: u64 = 32;
}

impl From<[u8; WalHeader::SIZE as usize]> for WalHeader {
    fn from(value: [u8; WalHeader::SIZE as usize]) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl Into<[u8; WalHeader::SIZE as usize]> for WalHeader {
    fn into(self) -> [u8; WalHeader::SIZE as usize] {
        unsafe { std::mem::transmute(self) }
    }
}

#[derive(Debug)]
pub(crate) struct WalFileReader {
    file: tokio::fs::File,
    header: WalHeader,
}

impl WalFileReader {
    pub async fn open<P: AsRef<Path>>(fpath: P) -> Result<Option<Self>> {
        let mut file = tokio::fs::File::open(fpath).await?;
        let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        if len < WalHeader::SIZE {
            return Ok(None);
        }
        let header = {
            let mut buf = [0u8; WalHeader::SIZE as usize];
            file.read_exact(buf.as_mut()).await?;
            WalHeader::from(buf)
        };
        Ok(Some(WalFileReader { file, header }))
    }

    /// Returns page size stored in WAL file header.
    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    pub fn frame_size(&self) -> u64 {
        WalFrameHeader::SIZE + (self.page_size() as u64)
    }

    /// Returns checksum stored in WAL file header.
    pub fn checksum(&self) -> u64 {
        self.header.crc
    }

    /// Returns an offset in a WAL file, where the data of a frame with given number starts.
    pub fn offset_in_wal(&self, frame_no: u32) -> u64 {
        WalHeader::SIZE + ((frame_no - 1) as u64) * self.frame_size()
    }

    /// Returns a number of pages stored in current WAL file.
    pub async fn page_count(&self) -> u32 {
        let len = self.file.metadata().await.map(|m| m.len()).unwrap_or(0);
        if len < WalHeader::SIZE {
            0
        } else {
            ((len - WalHeader::SIZE) / self.frame_size()) as u32
        }
    }

    /// Sets a file cursor at the beginning of a frame with given number.
    pub async fn seek_frame(&mut self, frame_no: u32) -> Result<()> {
        let offset = self.offset_in_wal(frame_no);
        self.file.seek(SeekFrom::Start(offset)).await?;
        Ok(())
    }

    /// Reads a header of a WAL frame, without reading the entire page that frame is
    /// responsible for.
    ///
    /// For reading specific frame use [WalFileReader::seek_frame] before calling this method.
    pub async fn read_frame_header(&mut self) -> Result<WalFrameHeader> {
        let mut buf = [0u8; WalFrameHeader::SIZE as usize];
        self.file.read_exact(buf.as_mut()).await?;
        Ok(WalFrameHeader::from(buf))
    }

    /// Reads a header of a WAL frame, filling the frame page data into provided slice buffer.
    /// Slice size must be equal to WAL page size.
    ///
    /// For reading specific frame use [WalFileReader::seek_frame] before calling this method.
    pub async fn read_frame(&mut self, page: &mut [u8]) -> Result<WalFrameHeader> {
        if page.len() != self.page_size() as usize {
            return Err(anyhow!(
                "Cannot read WAL frame page. Expected buffer size is {} bytes, provided buffer has {}",
                self.page_size(),
                page.len()
            ));
        }
        let frame_header = self.read_frame_header().await?;
        self.file.read_exact(page).await?;
        Ok(frame_header)
    }

    /// Reads a range of next consecutive frames, including headers, into given buffer.
    /// Returns a number of frames read this way.
    ///
    /// # Errors
    ///
    /// This function will propagate any WAL file I/O errors.
    /// It will return an error if provided `buf` length is not multiplication of an underlying
    /// WAL frame size.
    /// It will return an error if at least one frame was not fully read.
    pub async fn read_frame_range(&mut self, buf: &mut [u8]) -> Result<usize> {
        let frame_size = self.frame_size() as usize;
        if buf.len() % frame_size != 0 {
            return Err(anyhow!("Provided buffer doesn't fit full frames"));
        }
        let read = self.file.read_exact(buf).await?;
        if read % frame_size != 0 {
            Err(anyhow!("Some of the read frames where not complete"))
        } else {
            Ok(read / frame_size)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::wal::{WalFrameHeader, WalHeader};

    #[test]
    fn wal_frame_header_mem_mapping() {
        let fh = WalFrameHeader {
            pgno: 10020,
            size_after: 20481,
            salt: 0xdeadbeaf,
            crc: 935793,
        };
        let bin: [u8; WalFrameHeader::SIZE as usize] = fh.clone().into();
        let fh2 = WalFrameHeader::from(bin);
        assert_eq!(fh, fh2);
    }

    #[test]
    fn wal_header_mem_mapping() {
        let wh = WalHeader {
            magic_no: 0x377f0682,
            version: 3007000,
            page_size: 4096,
            checkpoint_seq_no: 1,
            salt_1: 1,
            salt_2: 0xa1243b,
            crc: 0x935baad,
        };
        let bin: [u8; WalHeader::SIZE as usize] = wh.clone().into();
        let wh2 = WalHeader::from(bin);
        assert_eq!(wh, wh2);
    }
}
