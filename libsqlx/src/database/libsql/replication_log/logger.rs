use std::collections::HashSet;
use std::ffi::{c_int, c_void, CStr};
use std::fs::{remove_dir_all, File, OpenOptions};
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::bail;
use bytemuck::{bytes_of, pod_read_unaligned, Pod, Zeroable};
use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use rusqlite::ffi::{
    libsql_wal as Wal, sqlite3, PgHdr, SQLITE_CHECKPOINT_TRUNCATE, SQLITE_IOERR, SQLITE_OK,
};
use sqld_libsql_bindings::ffi::types::{
    XWalCheckpointFn, XWalFrameFn, XWalSavePointUndoFn, XWalUndoFn,
};
use sqld_libsql_bindings::ffi::PageHdrIter;
use sqld_libsql_bindings::init_static_wal_method;
use sqld_libsql_bindings::wal_hook::WalHook;
use uuid::Uuid;

use crate::database::frame::{Frame, FrameHeader};
#[cfg(feature = "bottomless")]
use crate::libsql::ffi::SQLITE_IOERR_WRITE;

use super::{FrameNo, WAL_MAGIC, WAL_PAGE_SIZE};

init_static_wal_method!(REPLICATION_METHODS, ReplicationLoggerHook);

#[derive(PartialEq, Eq)]
struct Version([u16; 4]);

impl Version {
    fn current() -> Self {
        let major = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
        let minor = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap();
        let patch = env!("CARGO_PKG_VERSION_PATCH").parse().unwrap();
        Self([0, major, minor, patch])
    }
}

pub enum ReplicationLoggerHook {}

#[derive(Clone)]
pub struct ReplicationLoggerHookCtx {
    pub(crate) buffer: Vec<WalPage>,
    pub(crate) logger: Arc<ReplicationLogger>,
    #[cfg(feature = "bottomless")]
    bottomless_replicator: Option<Arc<std::sync::Mutex<bottomless::replicator::Replicator>>>,
}

/// This implementation of WalHook intercepts calls to `on_frame`, and writes them to a
/// shadow wal. Writing to the shadow wal is done in three steps:
/// i. append the new pages at the offset pointed by header.start_frame_no + header.frame_count
/// ii. call the underlying implementation of on_frames
/// iii. if the call of the underlying method was successfull, update the log header to the new
/// frame count.
///
/// If either writing to the database of to the shadow wal fails, it must be noop.
unsafe impl WalHook for ReplicationLoggerHook {
    type Context = ReplicationLoggerHookCtx;

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"replication_logger_hook\0").unwrap()
    }

    fn on_frames(
        wal: &mut Wal,
        page_size: c_int,
        page_headers: *mut PgHdr,
        ntruncate: u32,
        is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        assert_eq!(page_size, 4096);
        let wal_ptr = wal as *mut _;
        #[cfg(feature = "bottomless")]
        let last_valid_frame = wal.hdr.mxFrame;
        #[cfg(feature = "bottomless")]
        let _frame_checksum = wal.hdr.aFrameCksum;
        let ctx = Self::wal_extract_ctx(wal);

        for (page_no, data) in PageHdrIter::new(page_headers, page_size as _) {
            ctx.write_frame(page_no, data)
        }
        if let Err(e) = ctx.flush(ntruncate) {
            tracing::error!("error writing to replication log: {e}");
            // returning IO_ERR ensure that xUndo will be called by sqlite.
            return SQLITE_IOERR;
        }

        let rc = unsafe {
            orig(
                wal_ptr,
                page_size,
                page_headers,
                ntruncate,
                is_commit,
                sync_flags,
            )
        };

        // FIXME: instead of block_on, we should consider replicating asynchronously in the background,
        // e.g. by sending the data to another fiber by an unbounded channel (which allows sync insertions).
        #[allow(clippy::await_holding_lock)] // uncontended -> only gets called under a libSQL write lock
        #[cfg(feature = "bottomless")]
        if rc == 0 {
            let runtime = tokio::runtime::Handle::current();
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                match runtime.block_on(async move {
                    let mut replicator = replicator.lock().unwrap();
                    replicator.register_last_valid_frame(last_valid_frame);
                    // In theory it's enough to set the page size only once, but in practice
                    // it's a very cheap operation anyway, and the page is not always known
                    // upfront and can change dynamically.
                    // FIXME: changing the page size in the middle of operation is *not*
                    // supported by bottomless storage.
                    replicator.set_page_size(page_size as usize)?;
                    let frame_count = PageHdrIter::new(page_headers, page_size as usize).count();
                    replicator.submit_frames(frame_count as u32);
                    Ok::<(), anyhow::Error>(())
                }) {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::error!("error writing to bottomless: {e}");
                        return SQLITE_IOERR_WRITE;
                    }
                }
            }
        }

        if is_commit != 0 && rc == 0 {
            if let Err(e) = ctx.commit() {
                // If we reach this point, it means that we have commited a transaction to sqlite wal,
                // but failed to commit it to the shadow WAL, which leaves us in an inconsistent state.
                tracing::error!(
                    "fatal error: log failed to commit: inconsistent replication log: {e}"
                );
                std::process::abort();
            }

            if let Err(e) = ctx
                .logger
                .log_file
                .write()
                .maybe_compact(&mut *ctx.logger.compactor.lock())
            {
                tracing::error!("fatal error: {e}, exiting");
                std::process::abort()
            }
        }

        rc
    }

    fn on_undo(
        wal: &mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        undo_ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        let ctx = Self::wal_extract_ctx(wal);
        ctx.rollback();

        #[cfg(feature = "bottomless")]
        tracing::error!(
            "fixme: implement bottomless undo for {:?}",
            ctx.bottomless_replicator
        );

        unsafe { orig(wal, func, undo_ctx) }
    }

    fn on_savepoint_undo(wal: &mut Wal, wal_data: *mut u32, orig: XWalSavePointUndoFn) -> i32 {
        let rc = unsafe { orig(wal, wal_data) };
        if rc != SQLITE_OK {
            return rc;
        };

        #[cfg(feature = "bottomless")]
        {
            let ctx = Self::wal_extract_ctx(wal);
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let last_valid_frame = unsafe { *wal_data };
                let mut replicator = replicator.lock().unwrap();
                let prev_valid_frame = replicator.peek_last_valid_frame();
                tracing::trace!(
                    "Savepoint: rolling back from frame {prev_valid_frame} to {last_valid_frame}",
                );
                replicator.rollback_to_frame(last_valid_frame);
            }
        }

        rc
    }

    #[allow(clippy::too_many_arguments)]
    fn on_checkpoint(
        wal: &mut Wal,
        db: *mut sqlite3,
        emode: i32,
        busy_handler: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
        busy_arg: *mut c_void,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
        frames_in_wal: *mut i32,
        backfilled_frames: *mut i32,
        orig: XWalCheckpointFn,
    ) -> i32 {
        #[cfg(feature = "bottomless")]
        {
            tracing::trace!("bottomless checkpoint");

            /* In order to avoid partial checkpoints, passive checkpoint
             ** mode is not allowed. Only TRUNCATE checkpoints are accepted,
             ** because these are guaranteed to block writes, copy all WAL pages
             ** back into the main database file and reset the frame number.
             ** In order to avoid autocheckpoint on close (that's too often),
             ** checkpoint attempts weaker than TRUNCATE are ignored.
             */
            if emode < SQLITE_CHECKPOINT_TRUNCATE {
                tracing::trace!("Ignoring a checkpoint request weaker than TRUNCATE");
                return SQLITE_OK;
            }
        }
        let rc = unsafe {
            orig(
                wal,
                db,
                emode,
                busy_handler,
                busy_arg,
                sync_flags,
                n_buf,
                z_buf,
                frames_in_wal,
                backfilled_frames,
            )
        };

        if rc != SQLITE_OK {
            return rc;
        }

        #[allow(clippy::await_holding_lock)] // uncontended -> only gets called under a libSQL write lock
        #[cfg(feature = "bottomless")]
        {
            let ctx = Self::wal_extract_ctx(wal);
            let runtime = tokio::runtime::Handle::current();
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let mut replicator = replicator.lock().unwrap();
                if replicator.commits_in_current_generation() == 0 {
                    tracing::debug!("No commits happened in this generation, not snapshotting");
                    return SQLITE_OK;
                }
                let last_known_frame = replicator.last_known_frame();
                replicator.request_flush();
                if let Err(e) = runtime.block_on(replicator.wait_until_committed(last_known_frame))
                {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm {} frames backup: {}",
                        last_known_frame,
                        e
                    );
                    return SQLITE_IOERR_WRITE;
                }
                replicator.new_generation();
                if let Err(e) =
                    runtime.block_on(async move { replicator.snapshot_main_db_file().await })
                {
                    tracing::error!("Failed to snapshot the main db file during checkpoint: {e}");
                    return SQLITE_IOERR_WRITE;
                }
            }
        }
        SQLITE_OK
    }
}

#[derive(Clone)]
pub struct WalPage {
    pub page_no: u32,
    /// 0 for non-commit frames
    pub size_after: u32,
    pub data: Bytes,
}

impl ReplicationLoggerHookCtx {
    pub fn new(
        logger: Arc<ReplicationLogger>,
        #[cfg(feature = "bottomless")] bottomless_replicator: Option<
            Arc<std::sync::Mutex<bottomless::replicator::Replicator>>,
        >,
    ) -> Self {
        #[cfg(feature = "bottomless")]
        tracing::trace!("bottomless replication enabled: {bottomless_replicator:?}");
        Self {
            buffer: Default::default(),
            logger,
            #[cfg(feature = "bottomless")]
            bottomless_replicator,
        }
    }

    fn write_frame(&mut self, page_no: u32, data: &[u8]) {
        let entry = WalPage {
            page_no,
            size_after: 0,
            data: Bytes::copy_from_slice(data),
        };
        self.buffer.push(entry);
    }

    /// write buffered pages to the logger, without commiting.
    fn flush(&mut self, size_after: u32) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.buffer.last_mut().unwrap().size_after = size_after;
            self.logger.write_pages(&self.buffer)?;
            self.buffer.clear();
        }

        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        let new_frame_no = self.logger.commit()?;
        let _ = (self.logger.new_frame_notifier)(new_frame_no);
        Ok(())
    }

    fn rollback(&mut self) {
        self.logger.log_file.write().rollback();
        self.buffer.clear();
    }
}

/// Represent a LogFile, and operations that can be performed on it.
/// A log file must only ever be opened by a single instance of LogFile, since it caches the file
/// header.
#[derive(Debug)]
pub struct LogFile {
    file: File,
    /// Path of the LogFile
    path: PathBuf,
    pub header: LogFileHeader,
    /// number of frames in the log that have not been commited yet. On commit the header's frame
    /// count is incremented by that ammount. New pages are written after the last
    /// header.frame_count + uncommit_frame_count.
    /// On rollback, this is reset to 0, so that everything that was written after the previous
    /// header.frame_count is ignored and can be overwritten
    pub(crate) uncommitted_frame_count: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum LogReadError {
    #[error("could not fetch log entry, snapshot required")]
    SnapshotRequired,
    #[error("requested entry is ahead of log")]
    Ahead,
    #[error(transparent)]
    Error(#[from] crate::error::Error),
}

impl LogFile {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn new(path: PathBuf) -> crate::Result<Self> {
        // FIXME: we should probably take a lock on this file, to prevent anybody else to write to
        // it.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let file_end = file.metadata()?.len();

        if file_end == 0 {
            let db_id = Uuid::new_v4();
            let header = LogFileHeader {
                version: 2,
                start_frame_no: 0,
                magic: WAL_MAGIC,
                page_size: WAL_PAGE_SIZE,
                db_id: db_id.as_u128(),
                frame_count: 0,
                sqld_version: Version::current().0,
                _pad: 0,
            };

            let mut this = Self {
                path,
                file,
                header,
                uncommitted_frame_count: 0,
            };

            this.write_header()?;

            Ok(this)
        } else {
            let header = Self::read_header(&file)?;
            Ok(Self {
                file,
                header,
                uncommitted_frame_count: 0,
                path,
            })
        }
    }

    pub fn can_compact(&mut self) -> bool {
        self.header.frame_count > 0 && self.uncommitted_frame_count == 0
    }

    pub fn read_header(file: &File) -> crate::Result<LogFileHeader> {
        let mut buf = [0; size_of::<LogFileHeader>()];
        file.read_exact_at(&mut buf, 0)?;
        let header: LogFileHeader = pod_read_unaligned(&buf);
        if header.magic != WAL_MAGIC {
            return Err(crate::error::Error::InvalidLogHeader);
        }

        Ok(header)
    }

    pub fn header(&self) -> &LogFileHeader {
        &self.header
    }

    pub fn commit(&mut self) -> crate::Result<()> {
        self.header.frame_count += self.uncommitted_frame_count;
        self.uncommitted_frame_count = 0;
        self.write_header()?;

        Ok(())
    }

    fn rollback(&mut self) {
        self.uncommitted_frame_count = 0;
    }

    pub fn write_header(&mut self) -> crate::Result<()> {
        self.file.write_all_at(bytes_of(&self.header), 0)?;
        self.file.flush()?;

        Ok(())
    }

    /// Returns an iterator over the WAL frame headers
    pub fn frames_iter(&self) -> anyhow::Result<impl Iterator<Item = crate::Result<Frame>> + '_> {
        let mut current_frame_offset = 0;
        Ok(std::iter::from_fn(move || {
            if current_frame_offset >= self.header.frame_count {
                return None;
            }
            let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
            current_frame_offset += 1;
            Some(self.read_frame_byte_offset(read_byte_offset))
        }))
    }

    /// Returns an iterator over the WAL frame headers
    pub fn rev_frames_iter(&self) -> impl Iterator<Item = crate::Result<Frame>> + '_ {
        let mut current_frame_offset = self.header.frame_count;

        std::iter::from_fn(move || {
            if current_frame_offset == 0 {
                return None;
            }
            current_frame_offset -= 1;
            let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
            let frame = self.read_frame_byte_offset(read_byte_offset);
            Some(frame)
        })
    }

    /// If the log contains any frames, returns (start_frameno, end_frameno, iter), where iter, is
    /// a deduplicated reversed iterator over the frames in the log
    pub fn rev_deduped(
        &self,
    ) -> Option<(
        FrameNo,
        FrameNo,
        impl Iterator<Item = crate::Result<Frame>> + '_,
    )> {
        let mut iter = self.rev_frames_iter();
        let mut seen = HashSet::new();
        let start_fno = self.header().start_frame_no;
        let end_fno = self.header().last_frame_no()?;
        let iter = std::iter::from_fn(move || loop {
            match iter.next()? {
                Ok(frame) => {
                    if !seen.contains(&frame.header().page_no) {
                        seen.insert(frame.header().page_no);
                        return Some(Ok(frame));
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        });

        Some((start_fno, end_fno, iter))
    }

    pub fn push_page(&mut self, page: &WalPage) -> crate::Result<()> {
        let frame = Frame::from_parts(
            &FrameHeader {
                frame_no: self.next_frame_no(),
                page_no: page.page_no,
                size_after: page.size_after,
            },
            &page.data,
        );

        let byte_offset = self.next_byte_offset();
        tracing::trace!(
            "writing frame {} at offset {byte_offset}",
            frame.header().frame_no
        );
        self.file.write_all_at(frame.as_slice(), byte_offset)?;

        self.uncommitted_frame_count += 1;

        Ok(())
    }

    /// offset in bytes at which to write the next frame
    fn next_byte_offset(&self) -> u64 {
        Self::absolute_byte_offset(self.header().frame_count + self.uncommitted_frame_count)
    }

    fn next_frame_no(&self) -> FrameNo {
        self.header().start_frame_no + self.header().frame_count + self.uncommitted_frame_count
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<LogFileHeader>() as u64 + nth * Self::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameNo) -> crate::Result<Option<u64>> {
        if id < self.header.start_frame_no
            || id > self.header.start_frame_no + self.header.frame_count
        {
            return Ok(None);
        }
        Ok(Self::absolute_byte_offset(id - self.header.start_frame_no).into())
    }

    /// Returns bytes representing a WalFrame for frame `frame_no`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame(&self, frame_no: FrameNo) -> std::result::Result<Frame, LogReadError> {
        if frame_no < self.header.start_frame_no {
            return Err(LogReadError::SnapshotRequired);
        }

        if frame_no >= self.header.start_frame_no + self.header.frame_count {
            return Err(LogReadError::Ahead);
        }

        let frame = self.read_frame_byte_offset(self.byte_offset(frame_no)?.unwrap())?;

        Ok(frame)
    }

    fn maybe_compact(&mut self, compactor: &mut dyn LogCompactor) -> anyhow::Result<()> {
        if self.can_compact() && compactor.should_compact(self) {
            return self.do_compaction(compactor);
        }

        Ok(())
    }

    fn do_compaction(&mut self, compactor: &mut dyn LogCompactor) -> anyhow::Result<()> {
        tracing::info!("performing log compaction");
        let log_id = Uuid::new_v4();
        let temp_log_path = compactor.snapshot_dir().join(log_id.to_string());
        let last_frame = self
            .rev_frames_iter()
            .next()
            .expect("there should be at least one frame to perform compaction")?;
        let size_after = last_frame.header().size_after;
        assert!(size_after != 0);

        let mut new_log_file = LogFile::new(temp_log_path.clone())?;
        let new_header = LogFileHeader {
            start_frame_no: self.header.start_frame_no + self.header.frame_count,
            frame_count: 0,
            ..self.header
        };
        new_log_file.header = new_header;
        new_log_file.write_header().unwrap();
        // swap old and new log
        atomic_rename(&temp_log_path, &self.path).unwrap();
        std::mem::swap(&mut new_log_file.path, &mut self.path);
        let _ = std::mem::replace(self, new_log_file);
        compactor.compact(log_id).unwrap();

        Ok(())
    }

    fn read_frame_byte_offset(&self, offset: u64) -> crate::Result<Frame> {
        let mut buffer = BytesMut::zeroed(LogFile::FRAME_SIZE);
        self.file.read_exact_at(&mut buffer, offset)?;
        let buffer = buffer.freeze();

        Frame::try_from_bytes(buffer)
    }

    fn last_commited_frame_no(&self) -> Option<FrameNo> {
        if self.header.frame_count == 0 {
            None
        } else {
            Some(self.header.start_frame_no + self.header.frame_count - 1)
        }
    }

    fn reset(self) -> crate::Result<Self> {
        // truncate file
        self.file.set_len(0)?;
        Self::new(self.path)
    }

    /// return the size in bytes of the log
    pub fn size(&self) -> usize {
        size_of::<LogFileHeader>() + Frame::SIZE * self.header().frame_count as usize
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use std::ffi::CString;
    use std::os::unix::prelude::OsStrExt;

    use nix::libc::renamex_np;
    use nix::libc::RENAME_SWAP;

    let p1 = CString::new(p1.as_ref().as_os_str().as_bytes())?;
    let p2 = CString::new(p2.as_ref().as_os_str().as_bytes())?;
    unsafe {
        let ret = renamex_np(p1.as_ptr(), p2.as_ptr(), RENAME_SWAP);

        if ret != 0 {
            bail!(
                "failed to perform snapshot file swap: {ret}, errno: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use anyhow::Context;
    use nix::fcntl::{renameat2, RenameFlags};

    renameat2(
        None,
        p1.as_ref(),
        None,
        p2.as_ref(),
        RenameFlags::RENAME_EXCHANGE,
    )
    .context("failed to perform snapshot file swap")?;

    Ok(())
}

#[derive(Debug, Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct LogFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    pub magic: u64,
    _pad: u64,
    /// Uuid of the database associated with this log.
    pub db_id: u128,
    /// Frame_no of the first frame in the log
    pub start_frame_no: FrameNo,
    /// entry count in file
    pub frame_count: u64,
    /// Wal file version number, currently: 2
    pub version: u32,
    /// page size: 4096
    pub page_size: i32,
    /// sqld version when creating this log
    pub sqld_version: [u16; 4],
}

impl LogFileHeader {
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        if self.start_frame_no == 0 && self.frame_count == 0 {
            None
        } else {
            Some(self.start_frame_no + self.frame_count - 1)
        }
    }

    fn sqld_version(&self) -> Version {
        Version(self.sqld_version)
    }
}

pub struct Generation {
    pub id: Uuid,
    pub start_index: u64,
}

impl Generation {
    fn new(start_index: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            start_index,
        }
    }
}

pub trait LogCompactor: Sync + Send + 'static {
    /// returns whether the passed log file should be compacted. If this method returns true,
    /// compact should be called next.
    fn should_compact(&self, log: &LogFile) -> bool;
    /// Compact the given snapshot
    fn compact(
        &mut self,
        log_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>>;

    fn snapshot_dir(&self) -> PathBuf;
}

#[cfg(test)]
impl LogCompactor for () {
    fn compact(
        &mut self,
        _log_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        Ok(())
    }

    fn should_compact(&self, _file: &LogFile) -> bool {
        false
    }

    fn snapshot_dir(&self) -> PathBuf {
        todo!()
    }
}

pub type FrameNotifierCb = Box<dyn Fn(FrameNo) + Send + Sync + 'static>;

pub struct ReplicationLogger {
    pub log_file: RwLock<LogFile>,
    compactor: Box<Mutex<dyn LogCompactor + Send>>,
    /// a notifier channel other tasks can subscribe to, and get notified when new frames become
    /// available.
    pub new_frame_notifier: FrameNotifierCb,
}

impl ReplicationLogger {
    pub fn open(
        db_path: &Path,
        dirty: bool,
        compactor: impl LogCompactor,
        new_frame_notifier: FrameNotifierCb,
    ) -> crate::Result<Self> {
        let log_path = db_path.join("wallog");
        let data_path = db_path.join("data");

        let fresh = !log_path.exists();

        let log_file = LogFile::new(log_path)?;
        let header = log_file.header();

        let should_recover = if dirty {
            tracing::info!("Replication log is dirty, recovering from database file.");
            true
        } else if header.version < 2 || header.sqld_version() != Version::current() {
            tracing::info!("replication log version not compatible with current sqld version, recovering from database file.");
            true
        } else if fresh && data_path.exists() {
            tracing::info!("replication log not found, recovering from database file.");
            true
        } else {
            false
        };

        if should_recover {
            Self::recover(log_file, data_path, compactor, new_frame_notifier)
        } else {
            Self::from_log_file(log_file, compactor, new_frame_notifier)
        }
    }

    fn from_log_file(
        log_file: LogFile,
        compactor: impl LogCompactor,
        new_frame_notifier: FrameNotifierCb,
    ) -> crate::Result<Self> {
        let this = Self {
            compactor: Box::new(Mutex::new(compactor)),
            log_file: RwLock::new(log_file),
            new_frame_notifier,
        };

        if let Some(last_frame) = this.log_file.read().last_commited_frame_no() {
            (this.new_frame_notifier)(last_frame);
        }

        Ok(this)
    }

    fn recover(
        log_file: LogFile,
        mut data_path: PathBuf,
        compactor: impl LogCompactor,
        new_frame_notifier: FrameNotifierCb,
    ) -> crate::Result<Self> {
        // It is necessary to checkpoint before we restore the replication log, since the WAL may
        // contain pages that are not in the database file.
        checkpoint_db(&data_path)?;
        let mut log_file = log_file.reset()?;
        let snapshot_path = data_path.parent().unwrap().join("snapshots");
        // best effort, there may be no snapshots
        let _ = remove_dir_all(snapshot_path);

        let data_file = File::open(&data_path)?;
        let size = data_path.metadata()?.len();
        assert!(
            size % WAL_PAGE_SIZE as u64 == 0,
            "database file size is not a multiple of page size"
        );
        let num_page = size / WAL_PAGE_SIZE as u64;
        let mut buf = [0; WAL_PAGE_SIZE as usize];
        let mut page_no = 1; // page numbering starts at 1
        for i in 0..num_page {
            data_file.read_exact_at(&mut buf, i * WAL_PAGE_SIZE as u64)?;
            log_file.push_page(&WalPage {
                page_no,
                size_after: if i == num_page - 1 { num_page as _ } else { 0 },
                data: Bytes::copy_from_slice(&buf),
            })?;
            log_file.commit()?;

            page_no += 1;
        }

        assert!(data_path.pop());

        Self::from_log_file(log_file, compactor, new_frame_notifier)
    }

    pub fn database_id(&self) -> anyhow::Result<Uuid> {
        Ok(Uuid::from_u128((self.log_file.read()).header().db_id))
    }

    /// Write pages to the log, without updating the file header.
    /// Returns the new frame count and checksum to commit
    fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<()> {
        let mut log_file = self.log_file.write();
        for page in pages.iter() {
            log_file.push_page(page)?;
        }

        Ok(())
    }

    /// commit the current transaction and returns the new top frame number
    fn commit(&self) -> anyhow::Result<FrameNo> {
        let mut log_file = self.log_file.write();
        log_file.commit()?;
        Ok(log_file
            .header()
            .last_frame_no()
            .expect("there should be at least one frame after commit"))
    }

    pub fn get_frame(&self, frame_no: FrameNo) -> Result<Frame, LogReadError> {
        self.log_file.read().frame(frame_no)
    }

    pub fn compact(&self) {
        let mut log_file = self.log_file.write();
        if log_file.can_compact() {
            log_file.do_compaction(&mut *self.compactor.lock()).unwrap();
        }
    }
}

fn checkpoint_db(data_path: &Path) -> crate::Result<()> {
    unsafe {
        let conn = rusqlite::Connection::open(data_path)?;
        conn.pragma_query(None, "page_size", |row| {
            let page_size = row.get::<_, i32>(0).unwrap();
            assert_eq!(
                page_size, WAL_PAGE_SIZE,
                "invalid database file, expected page size to be {}, but found {} instead",
                WAL_PAGE_SIZE, page_size
            );
            Ok(())
        })?;
        let mut num_checkpointed: c_int = 0;
        let rc = rusqlite::ffi::sqlite3_wal_checkpoint_v2(
            conn.handle(),
            std::ptr::null(),
            SQLITE_CHECKPOINT_TRUNCATE,
            &mut num_checkpointed as *mut _,
            std::ptr::null_mut(),
        );

        // TODO: ensure correct page size
        assert!(
            rc == 0 && num_checkpointed >= 0,
            "failed to checkpoint database while recovering replication log"
        );

        conn.execute("VACUUM", ())?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), false, (), Box::new(|_| ())).unwrap();

        let frames = (0..10)
            .map(|i| WalPage {
                page_no: i,
                size_after: 0,
                data: Bytes::from(vec![i as _; 4096]),
            })
            .collect::<Vec<_>>();
        logger.write_pages(&frames).unwrap();
        logger.commit().unwrap();

        let log_file = logger.log_file.write();
        for i in 0..10 {
            let frame = log_file.frame(i).unwrap();
            assert_eq!(frame.header().page_no, i as u32);
            assert!(frame.page().iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            log_file.header.start_frame_no + log_file.header.frame_count,
            10
        );
    }

    #[test]
    fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), false, (), Box::new(|_| ())).unwrap();
        let log_file = logger.log_file.write();
        assert!(matches!(log_file.frame(1), Err(LogReadError::Ahead)));
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), false, (), Box::new(|_| ())).unwrap();
        let entry = WalPage {
            page_no: 0,
            size_after: 0,
            data: vec![0; 3].into(),
        };

        logger.write_pages(&[entry]).unwrap();
        logger.commit().unwrap();
    }

    #[test]
    fn log_file_test_rollback() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let mut log_file = LogFile::new(f.path().to_path_buf()).unwrap();
        (0..5)
            .map(|i| WalPage {
                page_no: i,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .for_each(|p| {
                log_file.push_page(&p).unwrap();
            });

        assert_eq!(log_file.frames_iter().unwrap().count(), 0);

        log_file.commit().unwrap();

        (0..5)
            .map(|i| WalPage {
                page_no: i,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .for_each(|p| {
                log_file.push_page(&p).unwrap();
            });

        log_file.rollback();
        assert_eq!(log_file.frames_iter().unwrap().count(), 5);

        log_file
            .push_page(&WalPage {
                page_no: 42,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .unwrap();

        assert_eq!(log_file.frames_iter().unwrap().count(), 5);
        log_file.commit().unwrap();
        assert_eq!(log_file.frames_iter().unwrap().count(), 6);
    }
}
