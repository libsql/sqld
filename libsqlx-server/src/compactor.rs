use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytemuck::{bytes_of, pod_read_unaligned, try_from_bytes, Pod, Zeroable};
use bytes::{Bytes, BytesMut};
use heed::byteorder::BigEndian;
use heed_types::{SerdeBincode, U64};
use libsqlx::libsql::LogFile;
use libsqlx::{Frame, FrameNo};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::sync::watch;
use tokio::task::block_in_place;
use uuid::Uuid;

use crate::meta::DatabaseId;
use crate::snapshot_store::SnapshotStore;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactionJob {
    /// Id of the database whose log needs to be compacted
    pub database_id: DatabaseId,
    /// path to the log to compact
    pub log_id: Uuid,
}

pub struct CompactionQueue {
    env: heed::Env,
    queue: heed::Database<U64<BigEndian>, SerdeBincode<CompactionJob>>,
    next_id: AtomicU64,
    notify: watch::Sender<Option<u64>>,
    db_path: PathBuf,
    pub snapshot_store: Arc<SnapshotStore>,
}

impl CompactionQueue {
    const COMPACTION_QUEUE_DB_NAME: &str = "compaction_queue_db";
    pub fn new(
        env: heed::Env,
        db_path: PathBuf,
        snapshot_store: Arc<SnapshotStore>,
    ) -> color_eyre::Result<Self> {
        let mut txn = env.write_txn()?;
        let queue = env.create_database(&mut txn, Some(Self::COMPACTION_QUEUE_DB_NAME))?;
        let next_id = match queue.last(&mut txn)? {
            Some((id, _)) => id + 1,
            None => 0,
        };
        txn.commit()?;

        let (notify, _) = watch::channel((next_id > 0).then(|| next_id - 1));
        Ok(Self {
            env,
            queue,
            next_id: next_id.into(),
            notify,
            db_path,
            snapshot_store,
        })
    }

    pub fn push(&self, job: &CompactionJob) {
        tracing::debug!("new compaction job available: {job:?}");
        let mut txn = self.env.write_txn().unwrap();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.queue.put(&mut txn, &id, job).unwrap();
        txn.commit().unwrap();
        self.notify.send_replace(Some(id));
    }

    pub async fn peek(&self) -> (u64, CompactionJob) {
        let id = self.next_id.load(Ordering::Relaxed);
        let txn = block_in_place(|| self.env.read_txn().unwrap());
        match block_in_place(|| self.queue.first(&txn).unwrap()) {
            Some(job) => job,
            None => {
                drop(txn);
                self.notify
                    .subscribe()
                    .wait_for(|x| x.map(|x| x >= id).unwrap_or_default())
                    .await
                    .unwrap();
                block_in_place(|| {
                    let txn = self.env.read_txn().unwrap();
                    self.queue.first(&txn).unwrap().unwrap()
                })
            }
        }
    }

    fn complete(&self, txn: &mut heed::RwTxn, job_id: u64) {
        block_in_place(|| {
            self.queue.delete(txn, &job_id).unwrap();
        });
    }

    async fn compact(&self) -> color_eyre::Result<()> {
        let (job_id, job) = self.peek().await;
        tracing::debug!("starting new compaction job: {job:?}");
        let to_compact_path = self.snapshot_queue_dir().join(job.log_id.to_string());
        let (start_fno, end_fno) = tokio::task::spawn_blocking({
            let to_compact_path = to_compact_path.clone();
            let db_path = self.db_path.clone();
            move || {
                let log = LogFile::new(to_compact_path)?;
                let (start_fno, end_fno, iter) =
                    log.rev_deduped().expect("compaction job with no frames!");
                let mut builder = SnapshotBuilder::new(
                    &db_path,
                    job.database_id,
                    job.log_id,
                    start_fno,
                    end_fno,
                )?;
                for frame in iter {
                    let frame = frame?;
                    builder.push_frame(frame)?;
                }
                builder.finish()
            }
        })
        .await??;

        let mut txn = self.env.write_txn()?;
        self.complete(&mut txn, job_id);
        self.snapshot_store
            .register(&mut txn, job.database_id, start_fno, end_fno, job.log_id);
        txn.commit()?;

        std::fs::remove_file(to_compact_path)?;

        Ok(())
    }

    pub fn snapshot_queue_dir(&self) -> PathBuf {
        self.db_path.join("snapshot_queue")
    }
}

pub async fn run_compactor_loop(compactor: Arc<CompactionQueue>) -> color_eyre::Result<()> {
    loop {
        compactor.compact().await?;
    }
}

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
/// header of a snapshot file
pub struct SnapshotFileHeader {
    /// id of the database
    pub db_id: DatabaseId,
    /// first frame in the snapshot
    pub start_frame_no: u64,
    /// end frame in the snapshot
    pub end_frame_no: u64,
    /// number of frames in the snapshot
    pub frame_count: u64,
    /// safe of the database after applying the snapshot
    pub size_after: u32,
    pub _pad: u32,
}

/// An utility to build a snapshots from log frames
pub struct SnapshotBuilder {
    pub header: SnapshotFileHeader,
    snapshot_id: Uuid,
    snapshot_file: BufWriter<NamedTempFile>,
    db_path: PathBuf,
    last_seen_frame_no: u64,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct SnapshotFrameHeader {
    pub frame_no: FrameNo,
    pub page_no: u32,
    _pad: u32,
}

#[derive(Clone)]
pub struct SnapshotFrame {
    data: Bytes,
}

impl SnapshotFrame {
    const SIZE: usize = size_of::<SnapshotFrameHeader>() + 4096;

    pub fn try_from_bytes(data: Bytes) -> crate::Result<Self> {
        if data.len() != Self::SIZE {
            color_eyre::eyre::bail!("invalid snapshot frame")
        }

        Ok(Self { data })
    }

    pub fn header(&self) -> Cow<SnapshotFrameHeader> {
        let data = &self.data[..size_of::<SnapshotFrameHeader>()];
        try_from_bytes(data)
            .map(Cow::Borrowed)
            .unwrap_or_else(|_| Cow::Owned(pod_read_unaligned(data)))
    }

    pub(crate) fn page(&self) -> &[u8] {
        &self.data[size_of::<SnapshotFrameHeader>()..]
    }
}

impl SnapshotBuilder {
    pub fn new(
        db_path: &Path,
        db_id: DatabaseId,
        snapshot_id: Uuid,
        start_fno: FrameNo,
        end_fno: FrameNo,
    ) -> color_eyre::Result<Self> {
        let temp_dir = db_path.join("tmp");
        let mut target = BufWriter::new(NamedTempFile::new_in(&temp_dir)?);
        // reserve header space
        target.write_all(&[0; size_of::<SnapshotFileHeader>()])?;

        Ok(Self {
            header: SnapshotFileHeader {
                db_id,
                start_frame_no: start_fno,
                end_frame_no: end_fno,
                frame_count: 0,
                size_after: 0,
                _pad: 0,
            },
            snapshot_file: target,
            db_path: db_path.to_path_buf(),
            last_seen_frame_no: u64::MAX,
            snapshot_id,
        })
    }

    pub fn push_frame(&mut self, frame: Frame) -> color_eyre::Result<()> {
        assert!(frame.header().frame_no < self.last_seen_frame_no);
        self.last_seen_frame_no = frame.header().frame_no;

        if frame.header().frame_no == self.header.end_frame_no {
            self.header.size_after = frame.header().size_after;
        }

        let header = SnapshotFrameHeader {
            frame_no: frame.header().frame_no,
            page_no: frame.header().page_no,
            _pad: 0,
        };

        self.snapshot_file.write_all(bytes_of(&header))?;
        self.snapshot_file.write_all(frame.page())?;

        self.header.frame_count += 1;

        Ok(())
    }

    /// Persist the snapshot, and returns the name and size is frame on the snapshot.
    pub fn finish(mut self) -> color_eyre::Result<(FrameNo, FrameNo)> {
        self.snapshot_file.flush()?;
        let file = self.snapshot_file.into_inner()?;
        file.as_file().write_all_at(bytes_of(&self.header), 0)?;

        let path = self
            .db_path
            .join("snapshots")
            .join(self.snapshot_id.to_string());
        file.persist(path)?;

        Ok((self.header.start_frame_no, self.header.end_frame_no))
    }
}

pub struct SnapshotFile {
    pub file: File,
    pub header: SnapshotFileHeader,
}

impl SnapshotFile {
    pub fn open(path: &Path) -> color_eyre::Result<Self> {
        let file = File::open(path)?;
        let mut header_buf = [0; size_of::<SnapshotFileHeader>()];
        file.read_exact_at(&mut header_buf, 0)?;
        let header: SnapshotFileHeader = pod_read_unaligned(&header_buf);

        Ok(Self { file, header })
    }

    /// Iterator on the frames contained in the snapshot file, in reverse frame_no order.
    pub fn frames_iter(&self) -> impl Iterator<Item = crate::Result<SnapshotFrame>> + '_ {
        let mut current_offset = 0;
        std::iter::from_fn(move || {
            if current_offset >= self.header.frame_count {
                return None;
            }
            let read_offset = size_of::<SnapshotFileHeader>() as u64
                + current_offset * SnapshotFrame::SIZE as u64;
            current_offset += 1;
            let mut buf = BytesMut::zeroed(SnapshotFrame::SIZE);
            match self.file.read_exact_at(&mut buf, read_offset as _) {
                Ok(_) => Some(Ok(SnapshotFrame { data: buf.freeze() })),
                Err(e) => Some(Err(e.into())),
            }
        })
    }

    /// Like `frames_iter`, but stops as soon as a frame with frame_no <= `frame_no` is reached
    pub fn frames_iter_from(
        &self,
        frame_no: u64,
    ) -> impl Iterator<Item = crate::Result<SnapshotFrame>> + '_ {
        let mut iter = self.frames_iter();
        std::iter::from_fn(move || match iter.next() {
            Some(Ok(frame)) => {
                if frame.header().frame_no < frame_no {
                    None
                } else {
                    Some(Ok(frame))
                }
            }
            other => other,
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::init_dirs;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn create_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        init_dirs(temp.path()).await.unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(100)
            .map_size(1000 * 4096)
            .open(temp.path().join("meta"))
            .unwrap();
        let snapshot_store = SnapshotStore::new(temp.path().to_path_buf(), env.clone()).unwrap();
        let store = Arc::new(snapshot_store);
        let queue = CompactionQueue::new(env, temp.path().to_path_buf(), store.clone()).unwrap();
        let log_id = Uuid::new_v4();
        let database_id = DatabaseId::random();

        let log_path = temp.path().join("snapshot_queue").join(log_id.to_string());
        tokio::fs::copy("assets/test/simple-log", &log_path)
            .await
            .unwrap();

        let log_file = LogFile::new(log_path).unwrap();
        let expected_start_frameno = log_file.header().start_frame_no;
        let expected_end_frameno =
            log_file.header().start_frame_no + log_file.header().frame_count - 1;
        let mut expected_page_content = log_file
            .frames_iter()
            .unwrap()
            .map(|f| f.unwrap().header().page_no)
            .collect::<HashSet<_>>();

        queue.push(&CompactionJob {
            database_id,
            log_id,
        });

        queue.compact().await.unwrap();

        let snapshot_path = temp.path().join("snapshots").join(log_id.to_string());
        assert!(snapshot_path.exists());

        let snapshot_file = SnapshotFile::open(&snapshot_path).unwrap();
        assert_eq!(snapshot_file.header.start_frame_no, expected_start_frameno);
        assert_eq!(snapshot_file.header.end_frame_no, expected_end_frameno);
        assert!(snapshot_file
            .frames_iter()
            .all(|f| expected_page_content.remove(&f.unwrap().header().page_no)));
        assert!(expected_page_content.is_empty());

        assert_eq!(
            snapshot_file
                .frames_iter()
                .map(Result::unwrap)
                .map(|f| f.header().frame_no)
                .reduce(|prev, new| {
                    assert!(new < prev);
                    new
                })
                .unwrap(),
            0
        );

        assert_eq!(store.locate(database_id, 0).unwrap().snapshot_id, log_id);
    }
}
