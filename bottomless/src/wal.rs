use anyhow::{anyhow, Result};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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

    /// In multi-page transactions, only the last page in the transaction contains
    /// the size_after_transaction field. If it's zero, it means it's an uncommited
    /// page.
    pub fn is_committed(&self) -> bool {
        self.size_after != 0
    }
}

impl From<[u8; WalFrameHeader::SIZE as usize]> for WalFrameHeader {
    fn from(v: [u8; WalFrameHeader::SIZE as usize]) -> Self {
        WalFrameHeader {
            pgno: u32::from_be_bytes([v[0], v[1], v[2], v[3]]),
            size_after: u32::from_be_bytes([v[4], v[5], v[6], v[7]]),
            salt: u64::from_be_bytes([v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]]),
            crc: u64::from_be_bytes([v[16], v[17], v[18], v[19], v[20], v[21], v[22], v[23]]),
        }
    }
}

impl Into<[u8; WalFrameHeader::SIZE as usize]> for WalFrameHeader {
    fn into(self) -> [u8; WalFrameHeader::SIZE as usize] {
        let mut result = [0u8; WalFrameHeader::SIZE as usize];
        let d = result.as_mut_ptr() as *mut u8;
        unsafe {
            std::ptr::copy_nonoverlapping(self.pgno.to_be_bytes().as_ptr(), d, 4);
            std::ptr::copy_nonoverlapping(self.size_after.to_be_bytes().as_ptr(), d.add(4), 4);
            std::ptr::copy_nonoverlapping(self.salt.to_be_bytes().as_ptr(), d.add(8), 8);
            std::ptr::copy_nonoverlapping(self.crc.to_be_bytes().as_ptr(), d.add(16), 8);
            result
        }
    }
}

#[repr(C, packed)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WalHeader {
    /// Magic number. 0x377f0682 or 0x377f0683
    pub magic_no: u32,
    /// File format version. Currently 3007000
    pub version: u32,
    /// Database page size.
    pub page_size: u32,
    /// Checkpoint sequence number
    pub checkpoint_seq_no: u32,
    /// Random integer incremented with each checkpoint
    pub salt_1: u32,
    /// A different random integer changing with each checkpoint
    pub salt_2: u32,
    /// Checksum for first 24 bytes of header
    pub crc: u64,
}

impl WalHeader {
    pub const SIZE: u64 = 32;
}

impl From<[u8; WalHeader::SIZE as usize]> for WalHeader {
    fn from(v: [u8; WalHeader::SIZE as usize]) -> Self {
        WalHeader {
            magic_no: u32::from_be_bytes([v[0], v[1], v[2], v[3]]),
            version: u32::from_be_bytes([v[4], v[5], v[6], v[7]]),
            page_size: u32::from_be_bytes([v[8], v[9], v[10], v[11]]),
            checkpoint_seq_no: u32::from_be_bytes([v[12], v[13], v[14], v[15]]),
            salt_1: u32::from_be_bytes([v[16], v[17], v[18], v[19]]),
            salt_2: u32::from_be_bytes([v[20], v[21], v[22], v[23]]),
            crc: u64::from_be_bytes([v[24], v[25], v[26], v[27], v[28], v[29], v[30], v[31]]),
        }
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

    pub fn checksum(&self) -> u64 {
        self.header.crc
    }

    pub fn frame_size(&self) -> u64 {
        WalFrameHeader::SIZE + (self.page_size() as u64)
    }

    /// Returns an offset in a WAL file, where the data of a frame with given number starts.
    pub fn offset_in_wal(&self, frame_no: u32) -> u64 {
        WalHeader::SIZE + ((frame_no - 1) as u64) * self.frame_size()
    }

    /// Returns a number of pages stored in current WAL file.
    pub async fn frame_count(&self) -> u32 {
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

/// Generate or extend an 8 byte checksum based on the data in
///the `page` and the `init` value. `page` size must be multiple of 8.
pub fn checksum_be(init: u64, page: &[u8]) -> u64 {
    debug_assert_eq!(page.len() % 8, 0);
    let mut s1 = (init >> 32) as u32;
    let mut s2 = (init & u32::MAX as u64) as u32;
    let page = unsafe { std::slice::from_raw_parts(page.as_ptr() as *const u32, page.len() / 4) };
    let mut i = 0;
    while i < page.len() {
        s1 = s1.wrapping_add(page[i].to_be()).wrapping_add(s2);
        s2 = s2.wrapping_add(page[i + 1].to_be()).wrapping_add(s1);
        i += 2;
    }
    ((s1 as u64) << 32) | (s2 as u64)
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
        // copied from actual SQLite WAL file
        let source = [
            55, 127, 6, 130, 0, 45, 226, 24, 0, 0, 16, 0, 0, 0, 0, 0, 190, 6, 47, 124, 39, 191, 98,
            92, 81, 22, 9, 209, 101, 96, 160, 157,
        ];
        let expected = WalHeader {
            magic_no: 0x377f0682,
            version: 3007000,
            page_size: 4096,
            checkpoint_seq_no: 0,
            salt_1: 3188076412,
            salt_2: 666853980,
            crc: 5842868361513443485,
        };
        let actual = WalHeader::from(source);
        assert_eq!(actual, expected);
    }
}
