use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytemuck::{bytes_of, Pod, Zeroable};
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
    snapshot_store: Arc<SnapshotStore>,
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
        let (snapshot_id, start_fno, end_fno) = tokio::task::spawn_blocking({
            let to_compact_path = to_compact_path.clone();
            let db_path = self.db_path.clone();
            move || {
                let mut builder = SnapshotBuilder::new(&db_path, job.database_id)?;
                let log = LogFile::new(to_compact_path)?;
                for frame in log.rev_deduped() {
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
            .register(&mut txn, job.database_id, start_fno, end_fno, snapshot_id);
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
    snapshot_file: BufWriter<NamedTempFile>,
    db_path: PathBuf,
    last_seen_frame_no: u64,
}

impl SnapshotBuilder {
    pub fn new(db_path: &Path, db_id: DatabaseId) -> color_eyre::Result<Self> {
        let temp_dir = db_path.join("tmp");
        let mut target = BufWriter::new(NamedTempFile::new_in(&temp_dir)?);
        // reserve header space
        target.write_all(&[0; size_of::<SnapshotFileHeader>()])?;

        Ok(Self {
            header: SnapshotFileHeader {
                db_id,
                start_frame_no: u64::MAX,
                end_frame_no: u64::MIN,
                frame_count: 0,
                size_after: 0,
                _pad: 0,
            },
            snapshot_file: target,
            db_path: db_path.to_path_buf(),
            last_seen_frame_no: u64::MAX,
        })
    }

    pub fn push_frame(&mut self, frame: Frame) -> color_eyre::Result<()> {
        assert!(frame.header().frame_no < self.last_seen_frame_no);
        self.last_seen_frame_no = frame.header().frame_no;
        if frame.header().frame_no < self.header.start_frame_no {
            self.header.start_frame_no = frame.header().frame_no;
        }

        if frame.header().frame_no > self.header.end_frame_no {
            self.header.end_frame_no = frame.header().frame_no;
            self.header.size_after = frame.header().size_after;
        }

        self.snapshot_file.write_all(frame.as_slice())?;
        self.header.frame_count += 1;

        Ok(())
    }

    /// Persist the snapshot, and returns the name and size is frame on the snapshot.
    pub fn finish(mut self) -> color_eyre::Result<(Uuid, FrameNo, FrameNo)> {
        self.snapshot_file.flush()?;
        let file = self.snapshot_file.into_inner()?;
        file.as_file().write_all_at(bytes_of(&self.header), 0)?;
        let snapshot_id = Uuid::new_v4();

        let path = self.db_path.join("snapshots").join(snapshot_id.to_string());
        file.persist(path)?;

        Ok((
            snapshot_id,
            self.header.start_frame_no,
            self.header.end_frame_no,
        ))
    }
}
