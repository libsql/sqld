use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use bytemuck::{bytes_of, pod_read_unaligned, Pod, Zeroable};
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use regex::Regex;
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::database::frame::Frame;

use super::logger::LogFile;
use super::FrameNo;

/// This is the ratio of the space required to store snapshot vs size of the actual database.
/// When this ratio is exceeded, compaction is triggered.
pub const SNAPHOT_SPACE_AMPLIFICATION_FACTOR: u64 = 2;
/// The maximum amount of snapshot allowed before a compaction is required
pub const MAX_SNAPSHOT_NUMBER: usize = 32;

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct SnapshotFileHeader {
    /// id of the database
    pub db_id: u128,
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

pub struct SnapshotFile {
    pub file: File,
    pub header: SnapshotFileHeader,
}

/// returns (db_id, start_frame_no, end_frame_no) for the given snapshot name
pub fn parse_snapshot_name(name: &str) -> Option<(Uuid, u64, u64)> {
    static SNAPSHOT_FILE_MATCHER: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r#"(?x)
            # match database id
            (\w{8}-\w{4}-\w{4}-\w{4}-\w{12})-
            # match start frame_no
            (\d*)-
            # match end frame_no
            (\d*).snap"#,
        )
        .unwrap()
    });
    let Some(captures) = SNAPSHOT_FILE_MATCHER.captures(name) else { return None};
    let db_id = captures.get(1).unwrap();
    let start_index: u64 = captures.get(2).unwrap().as_str().parse().unwrap();
    let end_index: u64 = captures.get(3).unwrap().as_str().parse().unwrap();

    Some((
        Uuid::from_str(db_id.as_str()).unwrap(),
        start_index,
        end_index,
    ))
}

pub fn snapshot_list(db_path: &Path) -> anyhow::Result<impl Iterator<Item = String>> {
    let mut entries = std::fs::read_dir(snapshot_dir_path(db_path))?;
    Ok(std::iter::from_fn(move || {
        for entry in entries.by_ref() {
            let Ok(entry) = entry else { continue; };
            let path = entry.path();
            let Some(name) = path.file_name() else {continue;};
            let Some(name_str) = name.to_str() else { continue;};

            return Some(name_str.to_string());
        }
        None
    }))
}

/// Return snapshot file containing "logically" frame_no
pub fn find_snapshot_file(
    db_path: &Path,
    frame_no: FrameNo,
) -> anyhow::Result<Option<SnapshotFile>> {
    let snapshot_dir_path = snapshot_dir_path(db_path);
    for name in snapshot_list(db_path)? {
        let Some((_, start_frame_no, end_frame_no)) = parse_snapshot_name(&name) else { continue; };
        // we're looking for the frame right after the last applied frame on the replica
        if (start_frame_no..=end_frame_no).contains(&frame_no) {
            let snapshot_path = snapshot_dir_path.join(&name);
            tracing::debug!("found snapshot for frame {frame_no} at {snapshot_path:?}");
            let snapshot_file = SnapshotFile::open(&snapshot_path)?;
            return Ok(Some(snapshot_file));
        }
    }

    Ok(None)
}

impl SnapshotFile {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut header_buf = [0; size_of::<SnapshotFileHeader>()];
        file.read_exact_at(&mut header_buf, 0)?;
        let header: SnapshotFileHeader = pod_read_unaligned(&header_buf);

        Ok(Self { file, header })
    }

    /// Iterator on the frames contained in the snapshot file, in reverse frame_no order.
    pub fn frames_iter(&self) -> impl Iterator<Item = crate::Result<Bytes>> + '_ {
        let mut current_offset = 0;
        std::iter::from_fn(move || {
            if current_offset >= self.header.frame_count {
                return None;
            }
            let read_offset = size_of::<SnapshotFileHeader>() as u64
                + current_offset * LogFile::FRAME_SIZE as u64;
            current_offset += 1;
            let mut buf = BytesMut::zeroed(LogFile::FRAME_SIZE);
            match self.file.read_exact_at(&mut buf, read_offset as _) {
                Ok(_) => Some(Ok(buf.freeze())),
                Err(e) => Some(Err(e.into())),
            }
        })
    }

    /// Like `frames_iter`, but stops as soon as a frame with frame_no <= `frame_no` is reached
    pub fn frames_iter_from(
        &self,
        frame_no: u64,
    ) -> impl Iterator<Item = crate::Result<Bytes>> + '_ {
        let mut iter = self.frames_iter();
        std::iter::from_fn(move || match iter.next() {
            Some(Ok(bytes)) => match Frame::try_from_bytes(bytes.clone()) {
                Ok(frame) => {
                    if frame.header().frame_no < frame_no {
                        None
                    } else {
                        Some(Ok(bytes))
                    }
                }
                Err(e) => Some(Err(e)),
            },
            other => other,
        })
    }
}

/// An utility to build a snapshots from log frames
pub struct SnapshotBuilder {
    seen_pages: HashSet<u32>,
    pub header: SnapshotFileHeader,
    snapshot_file: BufWriter<NamedTempFile>,
    db_path: PathBuf,
    last_seen_frame_no: u64,
}

pub fn snapshot_dir_path(db_path: &Path) -> PathBuf {
    db_path.join("snapshots")
}

impl SnapshotBuilder {
    pub fn new(db_path: &Path, db_id: u128) -> anyhow::Result<Self> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        std::fs::create_dir_all(&snapshot_dir_path)?;
        let mut target = BufWriter::new(NamedTempFile::new_in(&snapshot_dir_path)?);
        // reserve header space
        target.write_all(&[0; size_of::<SnapshotFileHeader>()])?;

        Ok(Self {
            seen_pages: HashSet::new(),
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

    /// append frames to the snapshot. Frames must be in decreasing frame_no order.
    pub fn append_frames(
        &mut self,
        frames: impl Iterator<Item = crate::Result<Frame>>,
    ) -> anyhow::Result<()> {
        // We iterate on the frames starting from the end of the log and working our way backward. We
        // make sure that only the most recent version of each file is present in the resulting
        // snapshot.
        //
        // The snapshot file contains the most recent version of each page, in descending frame
        // number order. That last part is important for when we read it later on.
        for frame in frames {
            let frame = frame?;
            assert!(frame.header().frame_no < self.last_seen_frame_no);
            self.last_seen_frame_no = frame.header().frame_no;
            if frame.header().frame_no < self.header.start_frame_no {
                self.header.start_frame_no = frame.header().frame_no;
            }

            if frame.header().frame_no > self.header.end_frame_no {
                self.header.end_frame_no = frame.header().frame_no;
                self.header.size_after = frame.header().size_after;
            }

            if !self.seen_pages.contains(&frame.header().page_no) {
                self.seen_pages.insert(frame.header().page_no);
                self.snapshot_file.write_all(frame.as_slice())?;
                self.header.frame_count += 1;
            }
        }

        Ok(())
    }

    /// Persist the snapshot, and returns the name and size is frame on the snapshot.
    pub fn finish(mut self) -> anyhow::Result<(String, u64)> {
        self.snapshot_file.flush()?;
        let file = self.snapshot_file.into_inner()?;
        file.as_file().write_all_at(bytes_of(&self.header), 0)?;
        let snapshot_name = format!(
            "{}-{}-{}.snap",
            Uuid::from_u128(self.header.db_id),
            self.header.start_frame_no,
            self.header.end_frame_no,
        );

        file.persist(snapshot_dir_path(&self.db_path).join(&snapshot_name))?;

        Ok((snapshot_name, self.header.frame_count))
    }
}

// #[cfg(test)]
// mod test {
//     use std::fs::read;
//     use std::{thread, time::Duration};
//
//     use bytemuck::pod_read_unaligned;
//     use bytes::Bytes;
//     use tempfile::tempdir;
//
//     use crate::database::frame::FrameHeader;
//     use crate::database::libsql::replication_log::logger::WalPage;
//
//     use super::*;
//
//     #[test]
//     fn compact_file_create_snapshot() {
//         let temp = tempfile::NamedTempFile::new().unwrap();
//         let mut log_file = LogFile::new(temp.as_file().try_clone().unwrap(), 0).unwrap();
//         let db_id = Uuid::new_v4();
//         log_file.header.db_id = db_id.as_u128();
//         log_file.write_header().unwrap();
//
//         // add 50 pages, each one in two versions
//         for _ in 0..2 {
//             for i in 0..25 {
//                 let data = std::iter::repeat(0).take(4096).collect::<Bytes>();
//                 let page = WalPage {
//                     page_no: i,
//                     size_after: i + 1,
//                     data,
//                 };
//                 log_file.push_page(&page).unwrap();
//             }
//         }
//
//         log_file.commit().unwrap();
//
//         let dump_dir = tempdir().unwrap();
//         let compactor = LogCompactor::new(dump_dir.path(), db_id.as_u128()).unwrap();
//         compactor
//             .compact(log_file, temp.path().to_path_buf(), 25)
//             .unwrap();
//
//         thread::sleep(Duration::from_secs(1));
//
//         let snapshot_path =
//             snapshot_dir_path(dump_dir.path()).join(format!("{}-{}-{}.snap", db_id, 0, 49));
//         let snapshot = read(&snapshot_path).unwrap();
//         let header: SnapshotFileHeader =
//             pod_read_unaligned(&snapshot[..std::mem::size_of::<SnapshotFileHeader>()]);
//
//         assert_eq!(header.start_frame_no, 0);
//         assert_eq!(header.end_frame_no, 49);
//         assert_eq!(header.frame_count, 25);
//         assert_eq!(header.db_id, db_id.as_u128());
//         assert_eq!(header.size_after, 25);
//
//         let mut seen_frames = HashSet::new();
//         let mut seen_page_no = HashSet::new();
//         let data = &snapshot[std::mem::size_of::<SnapshotFileHeader>()..];
//         data.chunks(LogFile::FRAME_SIZE).for_each(|f| {
//             let frame = Frame::try_from_bytes(Bytes::copy_from_slice(f)).unwrap();
//             assert!(!seen_frames.contains(&frame.header().frame_no));
//             assert!(!seen_page_no.contains(&frame.header().page_no));
//             seen_page_no.insert(frame.header().page_no);
//             seen_frames.insert(frame.header().frame_no);
//             assert!(frame.header().frame_no >= 25);
//         });
//
//         assert_eq!(seen_frames.len(), 25);
//         assert_eq!(seen_page_no.len(), 25);
//
//         let snapshot_file = SnapshotFile::open(&snapshot_path).unwrap();
//
//         let frames = snapshot_file.frames_iter_from(0);
//         let mut expected_frame_no = 49;
//         for frame in frames {
//             let frame = frame.unwrap();
//             let header: FrameHeader = pod_read_unaligned(&frame[..size_of::<FrameHeader>()]);
//             assert_eq!(header.frame_no, expected_frame_no);
//             expected_frame_no -= 1;
//         }
//
//         assert_eq!(expected_frame_no, 24);
//     }
// }
