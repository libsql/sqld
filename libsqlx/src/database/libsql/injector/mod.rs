use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::OpenFlags;

use crate::database::frame::Frame;
use crate::database::libsql::injector::hook::{
    INJECTOR_METHODS, LIBSQL_INJECT_FATAL, LIBSQL_INJECT_OK, LIBSQL_INJECT_OK_TXN,
};
use crate::database::{FrameNo, InjectError};
use crate::seal::Seal;

use hook::InjectorHookCtx;

mod headers;
mod hook;

pub type FrameBuffer = Arc<Mutex<VecDeque<Frame>>>;

pub struct Injector {
    /// The injector is in a transaction state
    is_txn: bool,
    /// Buffer for holding current transaction frames
    buffer: FrameBuffer,
    /// Maximum capacity of the frame buffer
    capacity: usize,
    /// Injector connection
    // connection must be dropped before the hook context
    connection: sqld_libsql_bindings::Connection<'static>,
    /// Pointer to the hook
    _hook_ctx: Seal<Box<InjectorHookCtx>>,
}

impl crate::database::Injector for Injector {
    fn inject(&mut self, frame: Frame) -> Result<Option<FrameNo>, InjectError> {
        let res = self.inject_frame(frame).unwrap();
        Ok(res)
    }

    fn clear(&mut self) {
        self.buffer.lock().clear();
    }
}

/// Methods from this trait are called before and after performing a frame injection.
/// This trait trait is used to record the last committed frame_no to the log.
/// The implementer can persist the pre and post commit frame no, and compare them in the event of
/// a crash; if the pre and post commit frame_no don't match, then the log may be corrupted.
pub trait InjectorCommitHandler: Send + Sync + 'static {
    fn pre_commit(&mut self, frame_no: FrameNo) -> anyhow::Result<()>;
    fn post_commit(&mut self, frame_no: FrameNo) -> anyhow::Result<()>;
}

impl<T: InjectorCommitHandler> InjectorCommitHandler for Box<T> {
    fn pre_commit(&mut self, frame_no: FrameNo) -> anyhow::Result<()> {
        self.as_mut().pre_commit(frame_no)
    }

    fn post_commit(&mut self, frame_no: FrameNo) -> anyhow::Result<()> {
        self.as_mut().post_commit(frame_no)
    }
}

impl InjectorCommitHandler for () {
    fn pre_commit(&mut self, _frame_no: FrameNo) -> anyhow::Result<()> {
        Ok(())
    }

    fn post_commit(&mut self, _frame_no: FrameNo) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Injector {
    pub fn new(
        path: &Path,
        injector_commit_handler: Box<dyn InjectorCommitHandler>,
        buffer_capacity: usize,
    ) -> crate::Result<Self> {
        let buffer = FrameBuffer::default();
        let ctx = InjectorHookCtx::new(buffer.clone(), injector_commit_handler);
        let mut ctx = Box::new(ctx);
        let connection = sqld_libsql_bindings::Connection::open(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            &INJECTOR_METHODS,
            // safety: hook is dropped after connection
            unsafe { &mut *(ctx.as_mut() as *mut _) },
        )?;

        Ok(Self {
            is_txn: false,
            buffer,
            capacity: buffer_capacity,
            connection,
            _hook_ctx: Seal::new(ctx),
        })
    }

    /// Inject on frame into the log. If this was a commit frame, returns Ok(Some(FrameNo)).
    pub(crate) fn inject_frame(&mut self, frame: Frame) -> crate::Result<Option<FrameNo>> {
        let frame_close_txn = frame.header().size_after != 0;
        self.buffer.lock().push_back(frame);
        if frame_close_txn || self.buffer.lock().len() >= self.capacity {
            if !self.is_txn {
                self.begin_txn();
            }
            return self.flush();
        }

        Ok(None)
    }

    /// Flush the buffer to libsql WAL.
    /// Trigger a dummy write, and flush the cache to trigger a call to xFrame. The buffer's frame
    /// are then injected into the wal.
    fn flush(&mut self) -> crate::Result<Option<FrameNo>> {
        let last_frame_no = match self.buffer.lock().back() {
            Some(f) => f.header().frame_no,
            None => {
                tracing::trace!("nothing to inject");
                return Ok(None);
            }
        };
        self.connection
            .execute("INSERT INTO __DUMMY__ VALUES (42)", ())?;
        // force call to xframe
        match self.connection.cache_flush() {
            Ok(_) => panic!("replication hook was not called"),
            Err(e) => {
                if let Some(e) = e.sqlite_error() {
                    if e.extended_code == LIBSQL_INJECT_OK {
                        // refresh schema
                        self.connection
                            .pragma_update(None, "writable_schema", "reset")?;
                        self.commit();
                        self.is_txn = false;
                        assert!(self.buffer.lock().is_empty());
                        return Ok(Some(last_frame_no));
                    } else if e.extended_code == LIBSQL_INJECT_OK_TXN {
                        self.is_txn = true;
                        assert!(self.buffer.lock().is_empty());
                        return Ok(None);
                    } else if e.extended_code == LIBSQL_INJECT_FATAL {
                        todo!("handle fatal error");
                    }
                }

                todo!("handle fatal error");
            }
        }
    }

    fn commit(&mut self) {
        // TODO: error?
        let _ = self.connection.execute("COMMIT", ());
    }

    fn begin_txn(&mut self) {
        self.connection.execute("BEGIN IMMEDIATE", ()).unwrap();
        self.connection
            .execute("CREATE TABLE __DUMMY__ (__dummy__)", ())
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::database::libsql::injector::Injector;
    use crate::database::libsql::replication_log::logger::LogFile;

    #[test]
    fn test_simple_inject_frames() {
        let log = LogFile::new(PathBuf::from("assets/test/simple_wallog")).unwrap();
        let temp = tempfile::tempdir().unwrap();

        let mut injector = Injector::new(temp.path(), Box::new(()), 10).unwrap();
        for frame in log.frames_iter().unwrap() {
            let frame = frame.unwrap();
            injector.inject_frame(frame).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_frames_split_txn() {
        let log = LogFile::new(PathBuf::from("assets/test/simple_wallog")).unwrap();
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector = Injector::new(temp.path(), Box::new(()), 1).unwrap();
        for frame in log.frames_iter().unwrap() {
            let frame = frame.unwrap();
            injector.inject_frame(frame).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_partial_txn_isolated() {
        let log = LogFile::new(PathBuf::from("assets/test/simple_wallog")).unwrap();
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector = Injector::new(temp.path(), Box::new(()), 10).unwrap();
        let mut iter = log.frames_iter().unwrap();

        assert!(injector
            .inject_frame(iter.next().unwrap().unwrap())
            .unwrap()
            .is_none());
        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();
        assert!(conn
            .query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .is_err());

        while injector
            .inject_frame(iter.next().unwrap().unwrap())
            .unwrap()
            .is_none()
        {}

        // reset schema
        conn.pragma_update(None, "writable_schema", "reset")
            .unwrap();
        conn.query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .unwrap();
    }
}
