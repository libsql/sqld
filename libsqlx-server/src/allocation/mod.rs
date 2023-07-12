use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use libsqlx::libsql::{LibsqlDatabase, LogCompactor, LogFile, PrimaryType, ReplicaType};
use libsqlx::proxy::WriteProxyDatabase;
use libsqlx::{Database as _, DescribeResponse, Frame, InjectableDatabase, Injector, FrameNo};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{block_in_place, JoinSet};
use tokio::time::timeout;

use crate::hrana;
use crate::hrana::http::handle_pipeline;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Dispatch;
use crate::linc::proto::{Enveloppe, Message, Frames};
use crate::linc::{Inbound, NodeId, Outbound};
use crate::meta::DatabaseId;

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
    },
    Inbound(Inbound),
}

pub struct DummyDb;
pub struct DummyConn;

impl libsqlx::Connection for DummyConn {
    fn execute_program(
        &mut self,
        _pgm: libsqlx::program::Program,
        _result_builder: &mut dyn libsqlx::result_builder::ResultBuilder,
    ) -> libsqlx::Result<()> {
        todo!()
    }

    fn describe(&self, _sql: String) -> libsqlx::Result<DescribeResponse> {
        todo!()
    }
}

impl libsqlx::Database for DummyDb {
    type Connection = DummyConn;

    fn connect(&self) -> Result<Self::Connection, libsqlx::error::Error> {
        todo!()
    }
}

type ProxyDatabase = WriteProxyDatabase<LibsqlDatabase<ReplicaType>, DummyDb>;

pub enum Database {
    Primary(LibsqlDatabase<PrimaryType>),
    Replica {
        db: ProxyDatabase,
        injector_handle: mpsc::Sender<Frames>,
        primary_node_id: NodeId,
        last_received_frame_ts: Option<Instant>,
    },
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

const MAX_INJECTOR_BUFFER_CAP: usize = 32;

struct Replicator {
    dispatcher: Arc<dyn Dispatch>,
    req_id: u32,
    last_committed: FrameNo,
    next_seq: u32,
    database_id: DatabaseId,
    primary_node_id: NodeId,
    injector: Box<dyn Injector + Send + 'static>,
    receiver: mpsc::Receiver<Frames>,
}

impl Replicator {
    async fn run(mut self) {
        loop {
            match timeout(Duration::from_secs(5), self.receiver.recv()).await {
                Ok(Some(Frames {
                    req_id,
                    seq,
                    frames,
                })) => {
                    // ignore frames from a previous call to Replicate
                    if req_id != self.req_id { continue }
                    if seq != self.next_seq { 
                        // this is not the batch of frame we were expecting, drop what we have, and
                        // ask again from last checkpoint
                        self.query_replicate().await;
                        continue;
                    };
                    self.next_seq += 1;
                    for bytes in frames {
                        let frame = Frame::try_from_bytes(bytes).unwrap();
                        block_in_place(|| {
                            if let Some(last_committed) = self.injector.inject(frame).unwrap() {
                                self.last_committed = last_committed;
                            }
                        });
                    }
                }
                Err(_) => self.query_replicate().await,
                Ok(None) => break,
            }
        }
    }

    async fn query_replicate(&mut self) {
        self.req_id += 1;
        self.next_seq = 0;
        // clear buffered, uncommitted frames
        self.injector.clear();
        self.dispatcher
            .dispatch(Outbound {
                to: self.primary_node_id,
                enveloppe: Enveloppe {
                    database_id: Some(self.database_id),
                    message: Message::Replicate {
                        next_frame_no: self.last_committed + 1,
                        req_id: self.req_id - 1,
                    },
                },
            })
        .await;
    }
}

impl Database {
    pub fn from_config(config: &AllocConfig, path: PathBuf, dispatcher: Arc<dyn Dispatch>) -> Self {
        match config.db_config {
            DbConfig::Primary {} => {
                let db = LibsqlDatabase::new_primary(path, Compactor, false).unwrap();
                Self::Primary(db)
            }
            DbConfig::Replica { primary_node_id } => {
                let rdb = LibsqlDatabase::new_replica(path, MAX_INJECTOR_BUFFER_CAP, ()).unwrap();
                let wdb = DummyDb;
                let mut db = WriteProxyDatabase::new(rdb, wdb, Arc::new(|_| ()));
                let injector = db.injector().unwrap();
                let (sender, receiver) = mpsc::channel(16);
                let database_id = DatabaseId::from_name(&config.db_name);

                let replicator = Replicator {
                    dispatcher,
                    req_id: 0,
                    last_committed: 0, // TODO: load the last commited from meta file
                    next_seq: 0,
                    database_id,
                    primary_node_id,
                    injector,
                    receiver,
                };

                tokio::spawn(replicator.run());

                Self::Replica {
                    db,
                    injector_handle: sender,
                    primary_node_id,
                    last_received_frame_ts: None,
                }
            }
        }
    }

    fn connect(&self) -> Box<dyn libsqlx::Connection + Send> {
        match self {
            Database::Primary(db) => Box::new(db.connect().unwrap()),
            Database::Replica { db, .. } => Box::new(db.connect().unwrap()),
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
    /// handle to the message bus, to send messages
    pub dispatcher: Arc<dyn Dispatch>,
    pub db_name: String,
}

pub struct ConnectionHandle {
    exec: mpsc::Sender<ExecFn>,
    exit: oneshot::Sender<()>,
}

impl ConnectionHandle {
    pub async fn exec<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: for<'a> FnOnce(&'a mut (dyn libsqlx::Connection + 'a)) -> R + Send + 'static,
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
                                Ok(conn)
                            }).await;
                            let _ = ret.send(res);
                        }
                        AllocationMessage::Inbound(msg) => {
                            self.handle_inbound(msg).await;
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

    async fn handle_inbound(&mut self, msg: Inbound) {
        debug_assert_eq!(
            msg.enveloppe.database_id,
            Some(DatabaseId::from_name(&self.db_name))
        );

        match msg.enveloppe.message {
            Message::Handshake { .. } => todo!(),
            Message::ReplicationHandshake { .. } => todo!(),
            Message::ReplicationHandshakeResponse { .. } => todo!(),
            Message::Replicate { .. } => todo!(),
            Message::Frames(frames) => match &mut self.database {
                Database::Replica {
                    injector_handle,
                    last_received_frame_ts,
                    ..
                } => {
                    *last_received_frame_ts = Some(Instant::now());
                    injector_handle.send(frames).await;
                }
                Database::Primary(_) => todo!("handle primary receiving txn"),
            },
            Message::ProxyRequest { .. } => todo!(),
            Message::ProxyResponse { .. } => todo!(),
            Message::CancelRequest { .. } => todo!(),
            Message::CloseConnection { .. } => todo!(),
            Message::Error(_) => todo!(),
        }
    }

    async fn new_conn(&mut self) -> ConnectionHandle {
        let id = self.next_conn_id();
        let conn = block_in_place(|| self.database.connect());
        let (close_sender, exit) = oneshot::channel();
        let (exec_sender, exec_receiver) = mpsc::channel(1);
        let conn = Connection {
            id,
            conn,
            exit,
            exec: exec_receiver,
        };

        self.connections_futs.spawn(conn.run());

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
