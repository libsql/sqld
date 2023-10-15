use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::io::{Seek, Write, SeekFrom};
use std::mem::size_of;
use std::path::{PathBuf, Path};
use std::pin::Pin;
use std::task::{Poll, Context};

use bytemuck::{Zeroable, Pod, bytes_of};
use futures::StreamExt;
use futures_core::Stream;
use walkdir::{WalkDir, DirEntry};

use crate::LIBSQL_PAGE_SIZE;
use crate::replication::frame::{FrameHeader, FrameMut};
use crate::replication::snapshot::{SnapshotFileHeader, SnapshotFile};
use crate::replication::{FrameNo, frame::Frame};
use crate::namespace::NamespaceName;

pub struct Compactor<W> {
    wal: W,
    seen: HashSet<u32>,
}

impl<W: Wal> Compactor<W> {
    /// calls f on the deduplicated frames of the WAL, in reverse frame order.
    /// returns the (start_frame_no, end_frame_no, page_count, size_after)
    fn frames_with<F>(mut self, mut f: F) -> (FrameNo, FrameNo, u64, u32)
        where F: FnMut(&LibsqlFrame, FrameNo)
    {
        let wal_frame_count = self.wal.frame_count();
        let mut snapshot_frame_count = 0;
        let mut size_after = 0;

        while let Some(frame) = self.wal.next_frame_back() {
            let frame_no = wal_frame_count - self.seen.len();
            if !self.seen.contains(&frame.header.page_number) {
                self.seen.insert(frame.header.page_number);
                f(frame, frame_no as FrameNo);
                if snapshot_frame_count == 0 {
                    size_after = frame.header.size_after;
                }
                snapshot_frame_count += 1;
            }
        }   

        let start_frame_no = self.wal.start_frame_no();
        let end_frame_no = start_frame_no + self.wal.frame_count() as u64;

        (start_frame_no, end_frame_no, snapshot_frame_count, size_after)
    }
}

pub trait Wal {
    /// Returns the start frame_no for the current WAL
    fn start_frame_no(&self) -> FrameNo;
    /// Returns the next frame from the WAL, starting from the end of the WAL.
    fn next_frame_back<'a>(&'a mut self) -> Option<&'a LibsqlFrame>;
    /// returns the number of frames in the WAL
    fn frame_count(&self) -> usize;
}

pub trait Storage {
    type LocateFrameStream: Stream<Item = Frame>;

    fn checkpoint_wal<W>(
        &self,
        namespace: NamespaceName,
        reader: Compactor<W>,
    )
        where W: Wal + Send;

    fn locate(&self, namespace: NamespaceName, frame_no: FrameNo) -> Self::LocateFrameStream;
}

pub struct FsStorage {
    path: PathBuf,
}

impl Storage for FsStorage {
    type LocateFrameStream = FsFrameStreamer;

    fn checkpoint_wal<W>(
        &self,
        namespace: NamespaceName,
        compactor: Compactor<W>,
    )
        where W: Wal + Send
    {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.seek(std::io::SeekFrom::Start(size_of::<SnapshotFileHeader>() as _)).unwrap();
        let (start_frame_no, end_frame_no, frame_count, size_after) = compactor.frames_with(|frame, frame_no| {
            let frame_header = FrameHeader {
                frame_no,
                checksum: 0,
                page_no: frame.header.page_number,
                size_after: 0,
            };

            file.write_all(bytes_of(&frame_header)).unwrap();
            file.write_all(&frame.data).unwrap();
        });

        let snapshot_header = SnapshotFileHeader {
            namespace_id: namespace.id(),
            start_frame_no,
            end_frame_no,
            frame_count,
            size_after,
            _pad: 0,
        };

        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(bytes_of(&snapshot_header)).unwrap();

        let ns_snapshot_path = self.path.join(namespace.as_str());

        create_dir_all(&ns_snapshot_path).unwrap();

        let snapshot_name = format!("{start_frame_no}-{end_frame_no}");
        let snapshot_path = ns_snapshot_path.join(snapshot_name);

        file.persist(snapshot_path).unwrap();
    }

    fn locate(&self, namespace: NamespaceName, mut frame_no: FrameNo) -> Self::LocateFrameStream {
        let path = self.path.join(namespace.as_str());
        FsFrameStreamer {
            frame_no,
            path,
            current_snapshot: None,
        }
    }
}

struct FsFrameStreamer {
    frame_no: FrameNo,
    path: PathBuf,
    current_snapshot: Option<Pin<Box<dyn Stream<Item = crate::Result<FrameMut>>>>>,
}

impl Stream for FsFrameStreamer {
    type Item = Frame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_snapshot.is_none() {
            let Some(snapshot) = locate_snapshot_file(&self.path, self.frame_no) else { return Poll::Ready(None) };
            let stream = snapshot.into_stream_from(self.frame_no).into();
            self.current_snapshot.replace(Box::pin(stream));
        }

        let current = self.current_snapshot.as_mut().unwrap();

        match current.poll_next_unpin(cx)
    }
}

fn locate_snapshot_file(dir: &Path, frame_no: FrameNo) -> Option<SnapshotFile> {
    for entry in WalkDir::new(dir) {
        let entry = entry.unwrap();
        let mut split = entry.file_name().to_str().unwrap().split("-");
        let start_fno: FrameNo = split.next().unwrap().parse().unwrap();
        let end_fno: FrameNo = split.next().unwrap().parse().unwrap();
        if (start_fno..=end_fno).contains(&frame_no) {
            // FIXME: there is a chance that the snapshot we're trying to open was deleted, we
            // should try to relocate the next snapshot
            return Some(SnapshotFile::open(entry.path()).unwrap())
        }
    }

    None
}

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct LibsqlFrame {
    header: LibsqlFrameHeader,
    data: [u8; LIBSQL_PAGE_SIZE as usize],
}

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct LibsqlFrameHeader {
    page_number: u32,
    size_after: u32,
    salt1: u32,
    salt2: u32,
    checksum1: u32,
    checksum2: u32,
}

#[cfg(test)]
mod test {
    use super::*;
}
