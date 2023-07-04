use anyhow::Result;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct TransactionPageCache {
    /// Threshold (in pages) after which, the cache will start flushing pages on disk.
    restore_page_swap: u32,
    page_size: u32,
    /// Recovery file used to flushing pages on disk. Reusable between transactions.
    cache: Cache,
    recovery_fpath: Arc<str>,
}

impl TransactionPageCache {
    pub fn new(restore_page_swap: u32, page_size: u32, recovery_fpath: Arc<str>) -> Self {
        TransactionPageCache {
            restore_page_swap,
            page_size,
            recovery_fpath,
            cache: Cache::Memory(BTreeMap::new()),
        }
    }

    pub async fn insert(&mut self, pgno: u32, page: &[u8]) -> Result<()> {
        match &mut self.cache {
            Cache::Memory(map) => {
                let len = map.len();
                match map.entry(pgno) {
                    Entry::Vacant(_) if len > self.restore_page_swap as usize => {
                        let page_size = self.page_size;
                        if let Cache::Disk { index, file } = self.swap().await? {
                            Self::persist(index, file, pgno, page_size, page).await
                        } else {
                            Ok(())
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(page.into());
                        Ok(())
                    }
                    Entry::Occupied(mut e) => {
                        let buf = e.get_mut();
                        buf.copy_from_slice(page);
                        Ok(())
                    }
                }
            }
            Cache::Disk { index, file } => {
                Self::persist(index, file, pgno, self.page_size, page).await
            }
        }
    }

    async fn persist(
        index: &mut BTreeMap<u32, u64>,
        file: &mut File,
        pgno: u32,
        page_size: u32,
        page: &[u8],
    ) -> Result<()> {
        let end = (index.len() as u64) * (page_size as u64);
        match index.entry(pgno) {
            Entry::Vacant(e) => {
                file.seek(SeekFrom::End(0)).await?;
                file.write_all(page).await?;
                e.insert(end);
            }
            Entry::Occupied(e) => {
                let offset = *e.get();
                file.seek(SeekFrom::Start(offset)).await?;
                file.write_all(page).await?;
            }
        }
        Ok(())
    }

    /// Swaps current memory cache onto disk.
    async fn swap(&mut self) -> Result<&mut Cache> {
        if let Cache::Disk { .. } = self.cache {
            return Ok(&mut self.cache); // already swapped
        }
        tracing::trace!("Swapping transaction pages to file {}", self.recovery_fpath);
        let mut index = BTreeMap::new();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&*self.recovery_fpath)
            .await?;
        if let Cache::Memory(old) = &self.cache {
            let mut end = 0u64;
            for (&pgno, page) in old {
                file.write_all(page).await?;
                index.insert(pgno, end);
                end += page.len() as u64;
            }
        }
        self.cache = Cache::Disk { index, file };
        Ok(&mut self.cache)
    }

    pub async fn flush(mut self, db_file: &mut File) -> Result<()> {
        use tokio::io::AsyncReadExt;
        match &mut self.cache {
            Cache::Memory(map) => {
                for (&pgno, page) in map.iter() {
                    let offset = (pgno - 1) as u64 * (self.page_size as u64);
                    db_file.seek(SeekFrom::Start(offset)).await?;
                    db_file.write_all(page).await?;
                }
            }
            Cache::Disk { index, file } => {
                for (&pgno, &off) in index.iter() {
                    let offset = (pgno - 1) as u64 * (self.page_size as u64);
                    db_file.seek(SeekFrom::Start(offset)).await?;
                    let mut f = file.try_clone().await?;
                    f.seek(SeekFrom::Start(off)).await?;
                    let mut page = f.take(self.page_size as u64);
                    tokio::io::copy(&mut page, db_file).await?;
                }
                file.shutdown().await?;
                db_file.flush().await?;
                tokio::fs::remove_file(&*self.recovery_fpath).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Cache {
    /// Map storing page number and pages themselves in memory.
    Memory(BTreeMap<u32, Vec<u8>>),
    /// Map storing page number and offsets in transaction recovery file.
    Disk {
        index: BTreeMap<u32, u64>,
        file: File,
    },
}
