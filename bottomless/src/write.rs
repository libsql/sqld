use crate::wal::{WalFileReader, WalFrameHeader};
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use tokio::io::AsyncWrite;

pub(crate) struct BatchWriter {
    frames: BTreeSet<u32>,
    use_compression: bool,
}

impl BatchWriter {
    pub fn new(use_compression: bool) -> Self {
        BatchWriter {
            use_compression,
            frames: BTreeSet::default(),
        }
    }

    pub fn attach(&mut self, frame_no: u32) {
        self.frames.insert(frame_no);
    }

    pub async fn finalize<W>(self, wal: &mut WalFileReader, writer: &mut W) -> Result<u32>
    where
        W: AsyncWrite + Unpin,
    {
        if self.frames.is_empty() {
            tracing::trace!("Attempting to flush an empty buffer");
            return Ok(0);
        }

        let mut page = vec![0u8; wal.page_size() as usize];
        for frame_no in self.frames {
            wal.seek_frame(frame_no).await?;
            let header = wal.read_frame(&mut page).await?;
            if self.use_compression {
            } else {
            }
        }

        todo!()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Options {
    pub use_compression: bool,
    pub page_size: usize,
}
