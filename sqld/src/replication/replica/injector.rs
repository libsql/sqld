use std::fs::File;
use std::path::{Path, PathBuf};

use rusqlite::OpenFlags;
use sqld_libsql_bindings::open_with_regular_wal;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::replication::FrameNo;

use super::hook::{Frames, InjectorHook};
use super::meta::ReplicationMeta;

#[derive(Debug)]
struct FrameApplyOp {
    frames: Frames,
    ret: oneshot::Sender<anyhow::Result<FrameNo>>,
}

pub struct FrameInjectorHandle {
    handle: JoinHandle<anyhow::Result<()>>,
    sender: mpsc::Sender<FrameApplyOp>,
}

impl FrameInjectorHandle {
    pub async fn new(
        db_path: PathBuf,
        merger: impl FnOnce(Option<ReplicationMeta>) -> anyhow::Result<ReplicationMeta> + Send + 'static,
    ) -> anyhow::Result<(Self, FrameNo)> {
        let (sender, mut receiver) = mpsc::channel(16);
        let (ret, init_ok) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let maybe_injector = FrameInjector::new_with_merger(&db_path, merger);

            let mut injector = match maybe_injector {
                Ok((hook, last_applied_frame_no)) => {
                    ret.send(Ok(last_applied_frame_no)).unwrap();
                    hook
                }
                Err(e) => {
                    ret.send(Err(e)).unwrap();
                    return Ok(());
                }
            };

            while let Some(FrameApplyOp { frames, ret }) = receiver.blocking_recv() {
                let res = injector.inject_frames(frames);
                if ret.send(res).is_err() {
                    anyhow::bail!("frame application result must not be ignored.");
                }
            }
            Ok(())
        });

        let last_applied_frame_no = init_ok.await??;

        Ok((Self { handle, sender }, last_applied_frame_no))
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.sender);
        if let Err(e) = self.handle.await {
            // propagate panic
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                return Err(e)?;
            }
        }

        Ok(())
    }

    pub async fn inject_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        assert!(!frames.is_empty());
        let (ret, rcv) = oneshot::channel();
        self.sender.send(FrameApplyOp { frames, ret }).await?;
        rcv.await?
    }
}

pub struct FrameInjector {
    conn: rusqlite::Connection,
    hook: InjectorHook,
}

impl FrameInjector {
    /// returns the replication hook and the currently applied frame_no
    pub fn new_with_merger(
        db_path: &Path,
        merger: impl FnOnce(Option<ReplicationMeta>) -> anyhow::Result<ReplicationMeta>,
    ) -> anyhow::Result<(Self, FrameNo)> {
        let (meta, file) = ReplicationMeta::read_from_path(db_path)?;
        let meta = merger(meta)?;

        Ok((Self::init(db_path, file, meta)?, meta.current_frame_no()))
    }

    pub fn new(db_path: &Path, hook: InjectorHook) -> anyhow::Result<Self> {
        let conn = open_with_regular_wal(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            hook.clone(),
            false, // bottomless replication is not enabled for replicas
        )?;

        Ok(Self { conn, hook })
    }

    pub fn init(db_path: &Path, meta_file: File, meta: ReplicationMeta) -> anyhow::Result<Self> {
        let hook = InjectorHook::new(meta_file, meta);
        Self::new(db_path, hook)
    }

    /// sets the injector's frames to the provided frames, trigger a dummy write, and collect the
    /// injection result.
    fn inject_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        self.hook.set_frames(frames);

        let _ = dbg!(self
            .conn
            .execute("create table if not exists __dummy__ (dummy)", ()));

        self.hook.take_result()
    }
}
