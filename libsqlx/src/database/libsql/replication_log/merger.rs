use std::path::Path;
use std::sync::mpsc;
use std::thread::JoinHandle;

use crate::database::frame::Frame;

use super::snapshot::{
    parse_snapshot_name, snapshot_dir_path, snapshot_list, SnapshotBuilder, SnapshotFile,
    MAX_SNAPSHOT_NUMBER, SNAPHOT_SPACE_AMPLIFICATION_FACTOR,
};

pub struct SnapshotMerger {
    /// Sending part of a channel of (snapshot_name, snapshot_frame_count, db_page_count) to the merger thread
    sender: mpsc::Sender<(String, u64, u32)>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl SnapshotMerger {
    pub fn new(db_path: &Path, db_id: u128) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel();

        let db_path = db_path.to_path_buf();
        let handle =
            std::thread::spawn(move || Self::run_snapshot_merger_loop(receiver, &db_path, db_id));

        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    fn should_compact(snapshots: &[(String, u64)], db_page_count: u32) -> bool {
        let snapshots_size: u64 = snapshots.iter().map(|(_, s)| *s).sum();
        snapshots_size >= SNAPHOT_SPACE_AMPLIFICATION_FACTOR * db_page_count as u64
            || snapshots.len() > MAX_SNAPSHOT_NUMBER
    }

    fn run_snapshot_merger_loop(
        receiver: mpsc::Receiver<(String, u64, u32)>,
        db_path: &Path,
        db_id: u128,
    ) -> anyhow::Result<()> {
        let mut snapshots = Self::init_snapshot_info_list(db_path)?;
        while let Ok((name, size, db_page_count)) = receiver.recv() {
            snapshots.push((name, size));
            if Self::should_compact(&snapshots, db_page_count) {
                let compacted_snapshot_info = Self::merge_snapshots(&snapshots, db_path, db_id)?;
                snapshots.clear();
                snapshots.push(compacted_snapshot_info);
            }
        }

        Ok(())
    }

    /// Reads the snapshot dir and returns the list of snapshots along with their size, sorted in
    /// chronological order.
    ///
    /// TODO: if the process was killed in the midst of merging snapshot, then the compacted snapshot
    /// can exist alongside the snapshots it's supposed to have compacted. This is the place to
    /// perform the cleanup.
    fn init_snapshot_info_list(db_path: &Path) -> anyhow::Result<Vec<(String, u64)>> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        if !snapshot_dir_path.exists() {
            return Ok(Vec::new());
        }

        let mut temp = Vec::new();
        for snapshot_name in snapshot_list(db_path)? {
            let snapshot_path = snapshot_dir_path.join(&snapshot_name);
            let snapshot = SnapshotFile::open(&snapshot_path)?;
            temp.push((
                snapshot_name,
                snapshot.header.frame_count,
                snapshot.header.start_frame_no,
            ))
        }

        temp.sort_by_key(|(_, _, id)| *id);

        Ok(temp
            .into_iter()
            .map(|(name, count, _)| (name, count))
            .collect())
    }

    fn merge_snapshots(
        snapshots: &[(String, u64)],
        db_path: &Path,
        db_id: u128,
    ) -> anyhow::Result<(String, u64)> {
        let mut builder = SnapshotBuilder::new(db_path, db_id)?;
        let snapshot_dir_path = snapshot_dir_path(db_path);
        for (name, _) in snapshots.iter().rev() {
            let snapshot = SnapshotFile::open(&snapshot_dir_path.join(name))?;
            let iter = snapshot.frames_iter().map(|b| Frame::try_from_bytes(b?));
            builder.append_frames(iter)?;
        }

        let (_, start_frame_no, _) = parse_snapshot_name(&snapshots[0].0).unwrap();
        let (_, _, end_frame_no) = parse_snapshot_name(&snapshots.last().unwrap().0).unwrap();

        builder.header.start_frame_no = start_frame_no;
        builder.header.end_frame_no = end_frame_no;

        let compacted_snapshot_infos = builder.finish()?;

        for (name, _) in snapshots.iter() {
            std::fs::remove_file(&snapshot_dir_path.join(name))?;
        }

        Ok(compacted_snapshot_infos)
    }

    pub fn register_snapshot(
        &mut self,
        snapshot_name: String,
        snapshot_frame_count: u64,
        db_page_count: u32,
    ) -> anyhow::Result<()> {
        if self
            .sender
            .send((snapshot_name, snapshot_frame_count, db_page_count))
            .is_err()
        {
            if let Some(handle) = self.handle.take() {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("snapshot merger thread panicked"))??;
            }

            anyhow::bail!("failed to register snapshot with log merger: thread exited");
        }

        Ok(())
    }
}
