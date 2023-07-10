use std::path::PathBuf;
use std::sync::Arc;

use libsqlx::libsql::{LibsqlDatabase, LogCompactor, LogFile, PrimaryType};
use libsqlx::Database as _;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{block_in_place, JoinSet};

use crate::hrana;
use crate::hrana::http::handle_pipeline;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};

use self::config::{AllocConfig, DbConfig};

pub mod config;

type ExecFn = Box<dyn FnOnce(&mut dyn libsqlx::Connection) + Send>;

#[derive(Clone)]
pub struct ConnectionId {
    id: u32,
    close_sender: mpsc::Sender<()>,
}

pub enum AllocationMessage {
    NewConnection(oneshot::Sender<ConnectionHandle>),
    HranaPipelineReq {
        req: PipelineRequestBody,
        ret: oneshot::Sender<crate::Result<PipelineResponseBody>>,
    }
}

pub enum Database {
    Primary(libsqlx::libsql::LibsqlDatabase<PrimaryType>),
}

struct Compactor;

impl LogCompactor for Compactor {
    fn should_compact(&self, _log: &LogFile) -> bool {
        false
    }

    fn compact(
        &self,
        _log: LogFile,
        _path: std::path::PathBuf,
        _size_after: u32,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        todo!()
    }
}

impl Database {
    pub fn from_config(config: &AllocConfig, path: PathBuf) -> Self {
        match config.db_config {
            DbConfig::Primary {} => {
                let db = LibsqlDatabase::new_primary(path, Compactor, false).unwrap();
                Self::Primary(db)
            }
            DbConfig::Replica { .. } => todo!(),
        }
    }

    fn connect(&self) -> Box<dyn libsqlx::Connection + Send> {
        match self {
            Database::Primary(db) => Box::new(db.connect().unwrap()),
        }
    }
}

pub struct Allocation {
    pub inbox: mpsc::Receiver<AllocationMessage>,
    pub database: Database,
    /// spawned connection futures, returning their connection id on completion.
    pub connections_futs: JoinSet<u32>,
    pub next_conn_id: u32,
    pub max_concurrent_connections: u32,

    pub hrana_server: Arc<hrana::http::Server>,
}

pub struct ConnectionHandle {
    exec: mpsc::Sender<ExecFn>,
    exit: oneshot::Sender<()>,
}

impl ConnectionHandle {
    pub async fn exec<F, R>(&self, f: F) -> crate::Result<R>
        where F: for<'a> FnOnce(&'a mut (dyn libsqlx::Connection + 'a)) -> R + Send + 'static,
              R: Send + 'static,
    {
        let (sender, ret) = oneshot::channel();
        let cb = move |conn: &mut dyn libsqlx::Connection| {
            let res = f(conn);
            let _ = sender.send(res);
        };

        self.exec.send(Box::new(cb)).await.unwrap();

        Ok(ret.await?)
    }
}

impl Allocation {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        AllocationMessage::NewConnection(ret) => {
                            let _ =ret.send(self.new_conn().await);
                        },
                        AllocationMessage::HranaPipelineReq { req, ret} => {
                            let res = handle_pipeline(&self.hrana_server.clone(), req, || async {
                                let conn= self.new_conn().await;
                                dbg!();
                                Ok(conn)
                            }).await;
                            let _ = ret.send(res);
                        }
                    }
                },
                maybe_id = self.connections_futs.join_next() => {
                    if let Some(Ok(_id)) = maybe_id {
                        // self.connections.remove_entry(&id);
                    }
                },
                else => break,
            }
        }
    }

    async fn new_conn(&mut self) -> ConnectionHandle {
        dbg!();
        let id = self.next_conn_id();
        dbg!();
        let conn = block_in_place(|| self.database.connect());
        dbg!();
        let (close_sender, exit) = oneshot::channel();
        let (exec_sender, exec_receiver) = mpsc::channel(1);
        let conn = Connection {
            id,
            conn,
            exit,
            exec: exec_receiver,
        };

        dbg!();
        self.connections_futs.spawn(conn.run());
        dbg!();

        ConnectionHandle {
            exec: exec_sender,
            exit: close_sender,
        }

    }

    fn next_conn_id(&mut self) -> u32 {
        loop {
            self.next_conn_id = self.next_conn_id.wrapping_add(1);
            return self.next_conn_id;
            // if !self.connections.contains_key(&self.next_conn_id) {
            //     return self.next_conn_id;
            // }
        }
    }
}

struct Connection {
    id: u32,
    conn: Box<dyn libsqlx::Connection + Send>,
    exit: oneshot::Receiver<()>,
    exec: mpsc::Receiver<ExecFn>,
}

impl Connection {
    async fn run(mut self) -> u32 {
        loop {
            tokio::select! {
                _ = &mut self.exit => break,
                Some(exec) = self.exec.recv() => {
                    tokio::task::block_in_place(|| exec(&mut *self.conn));
                }
            }
        }

        self.id
    }
}
