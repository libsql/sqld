use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::poll_fn;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Instant;

use either::Either;
use libsqlx::libsql::LibsqlDatabase;
use libsqlx::program::Program;
use libsqlx::proxy::WriteProxyDatabase;
use libsqlx::{Database as _, InjectableDatabase};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{block_in_place, JoinSet};
use tokio::time::Interval;

use crate::allocation::primary::FrameStreamer;
use crate::compactor::CompactionQueue;
use crate::hrana;
use crate::hrana::http::handle_pipeline;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Dispatch;
use crate::linc::proto::{Frames, Message};
use crate::linc::{Inbound, NodeId};
use crate::meta::DatabaseId;

use self::config::{AllocConfig, DbConfig};
use self::primary::compactor::Compactor;
use self::primary::{PrimaryConnection, PrimaryDatabase, ProxyResponseBuilder};
use self::replica::{ProxyDatabase, RemoteDb, ReplicaConnection, Replicator};

pub mod config;
mod primary;
mod replica;

/// Maximum number of frame a Frame message is allowed to contain
const FRAMES_MESSAGE_MAX_COUNT: usize = 5;
/// Maximum number of frames in the injector buffer
const MAX_INJECTOR_BUFFER_CAPACITY: usize = 32;

type ExecFn = Box<dyn FnOnce(&mut dyn libsqlx::Connection) + Send>;

pub enum AllocationMessage {
    HranaPipelineReq {
        req: PipelineRequestBody,
        ret: oneshot::Sender<crate::Result<PipelineResponseBody>>,
    },
    Inbound(Inbound),
}

pub enum Database {
    Primary {
        db: PrimaryDatabase,
        compact_interval: Option<Pin<Box<Interval>>>,
    },
    Replica {
        db: ProxyDatabase,
        injector_handle: mpsc::Sender<Frames>,
        primary_id: NodeId,
        last_received_frame_ts: Option<Instant>,
    },
}

impl Database {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Self::Primary {
            compact_interval: Some(ref mut interval),
            db,
        } = self
        {
            ready!(interval.poll_tick(cx));
            tracing::debug!("attempting periodic log compaction");
            let db = db.db.clone();
            tokio::task::spawn_blocking(move || {
                db.compact_log();
            });
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

impl Database {
    pub fn from_config(
        config: &AllocConfig,
        path: PathBuf,
        dispatcher: Arc<dyn Dispatch>,
        compaction_queue: Arc<CompactionQueue>,
    ) -> Self {
        let database_id = DatabaseId::from_name(&config.db_name);

        match config.db_config {
            DbConfig::Primary {
                max_log_size,
                replication_log_compact_interval,
            } => {
                let (sender, receiver) = tokio::sync::watch::channel(0);
                let db = LibsqlDatabase::new_primary(
                    path,
                    Compactor::new(
                        max_log_size,
                        replication_log_compact_interval,
                        compaction_queue.clone(),
                        database_id,
                    ),
                    false,
                    Box::new(move |fno| {
                        let _ = sender.send(fno);
                    }),
                )
                .unwrap();

                let compact_interval = replication_log_compact_interval.map(|d| {
                    let mut i = tokio::time::interval(d / 2);
                    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    Box::pin(i)
                });

                Self::Primary {
                    db: PrimaryDatabase {
                        db: Arc::new(db),
                        replica_streams: HashMap::new(),
                        frame_notifier: receiver,
                        snapshot_store: compaction_queue.snapshot_store.clone(),
                    },
                    compact_interval,
                }
            }
            DbConfig::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
            } => {
                let rdb =
                    LibsqlDatabase::new_replica(path, MAX_INJECTOR_BUFFER_CAPACITY, ()).unwrap();
                let wdb = RemoteDb {
                    proxy_request_timeout_duration,
                };
                let mut db = WriteProxyDatabase::new(rdb, wdb, Arc::new(|_| ()));
                let injector = db.injector().unwrap();
                let (sender, receiver) = mpsc::channel(16);

                let replicator = Replicator::new(
                    dispatcher,
                    0,
                    database_id,
                    primary_node_id,
                    injector,
                    receiver,
                );

                tokio::spawn(replicator.run());

                Self::Replica {
                    db,
                    injector_handle: sender,
                    primary_id: primary_node_id,
                    last_received_frame_ts: None,
                }
            }
        }
    }

    fn connect(&self, connection_id: u32, alloc: &Allocation) -> impl ConnectionHandler {
        match self {
            Database::Primary {
                db: PrimaryDatabase { db, .. },
                ..
            } => Either::Right(PrimaryConnection {
                conn: db.connect().unwrap(),
            }),
            Database::Replica { db, primary_id, .. } => Either::Left(ReplicaConnection {
                conn: db.connect().unwrap(),
                connection_id,
                next_req_id: 0,
                primary_node_id: *primary_id,
                database_id: DatabaseId::from_name(&alloc.db_name),
                dispatcher: alloc.dispatcher.clone(),
            }),
        }
    }

    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary { .. })
    }
}

pub struct Allocation {
    pub inbox: mpsc::Receiver<AllocationMessage>,
    pub database: Database,
    /// spawned connection futures, returning their connection id on completion.
    pub connections_futs: JoinSet<(NodeId, u32)>,
    pub next_conn_id: u32,
    pub max_concurrent_connections: u32,
    pub connections: HashMap<NodeId, HashMap<u32, ConnectionHandle>>,

    pub hrana_server: Arc<hrana::http::Server>,
    /// handle to the message bus
    pub dispatcher: Arc<dyn Dispatch>,
    pub db_name: String,
}

#[derive(Clone)]
pub struct ConnectionHandle {
    exec: mpsc::Sender<ExecFn>,
    inbound: mpsc::Sender<Inbound>,
}

impl ConnectionHandle {
    pub async fn exec<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: for<'a> FnOnce(&'a mut dyn libsqlx::Connection) -> R + Send + 'static,
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
            let fut = poll_fn(|cx| self.database.poll(cx));
            tokio::select! {
                _ = fut => (),
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        AllocationMessage::HranaPipelineReq { req, ret } => {
                            let server = self.hrana_server.clone();
                            handle_pipeline(server, req, ret, || async {
                                let conn = self.new_conn(None).await;
                                Ok(conn)
                            }).await.unwrap();
                        }
                        AllocationMessage::Inbound(msg) => {
                            self.handle_inbound(msg).await;
                        }
                    }
                },
                maybe_id = self.connections_futs.join_next(), if !self.connections_futs.is_empty() => {
                    if let Some(Ok((node_id, conn_id))) = maybe_id {
                        self.connections.get_mut(&node_id).map(|m| m.remove(&conn_id));
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
            Message::Handshake { .. } => unreachable!("handshake should have been caught earlier"),
            Message::ReplicationHandshake { .. } => todo!(),
            Message::ReplicationHandshakeResponse { .. } => todo!(),
            Message::Replicate {
                req_no,
                next_frame_no,
            } => match &mut self.database {
                Database::Primary {
                    db:
                        PrimaryDatabase {
                            db,
                            replica_streams,
                            frame_notifier,
                            snapshot_store,
                            ..
                        },
                    ..
                } => {
                    let streamer = FrameStreamer {
                        logger: db.logger(),
                        database_id: DatabaseId::from_name(&self.db_name),
                        node_id: msg.from,
                        next_frame_no,
                        req_no,
                        seq_no: 0,
                        dipatcher: self.dispatcher.clone() as _,
                        notifier: frame_notifier.clone(),
                        buffer: Vec::new(),
                        snapshot_store: snapshot_store.clone(),
                    };

                    match replica_streams.entry(msg.from) {
                        Entry::Occupied(mut e) => {
                            let (old_req_no, old_handle) = e.get_mut();
                            // ignore req_no older that the current req_no
                            if *old_req_no < req_no {
                                let handle = tokio::spawn(streamer.run());
                                let old_handle = std::mem::replace(old_handle, handle);
                                *old_req_no = req_no;
                                old_handle.abort();
                            }
                        }
                        Entry::Vacant(e) => {
                            let handle = tokio::spawn(streamer.run());
                            // For some reason, not yielding causes the task not to be spawned
                            tokio::task::yield_now().await;
                            e.insert((req_no, handle));
                        }
                    }
                }
                Database::Replica { .. } => todo!("not a primary!"),
            },
            Message::Frames(frames) => match &mut self.database {
                Database::Replica {
                    injector_handle,
                    last_received_frame_ts,
                    ..
                } => {
                    *last_received_frame_ts = Some(Instant::now());
                    injector_handle.send(frames).await.unwrap();
                }
                Database::Primary {
                    db: PrimaryDatabase { .. },
                    ..
                } => todo!("handle primary receiving txn"),
            },
            Message::ProxyRequest {
                connection_id,
                req_id,
                program,
            } => {
                self.handle_proxy(msg.from, connection_id, req_id, program)
                    .await
            }
            Message::ProxyResponse(ref r) => {
                if let Some(conn) = self
                    .connections
                    .get(&self.dispatcher.node_id())
                    .and_then(|m| m.get(&r.connection_id).cloned())
                {
                    conn.inbound.send(msg).await.unwrap();
                }
            }
            Message::CancelRequest { .. } => todo!(),
            Message::CloseConnection { .. } => todo!(),
            Message::Error(_) => todo!(),
        }
    }

    async fn handle_proxy(
        &mut self,
        to: NodeId,
        connection_id: u32,
        req_id: u32,
        program: Program,
    ) {
        let dispatcher = self.dispatcher.clone();
        let database_id = DatabaseId::from_name(&self.db_name);
        let exec = |conn: ConnectionHandle| async move {
            let _ = conn
                .exec(move |conn| {
                    let builder = ProxyResponseBuilder::new(
                        dispatcher,
                        database_id,
                        to,
                        req_id,
                        connection_id,
                    );
                    conn.execute_program(&program, Box::new(builder)).unwrap();
                })
                .await;
        };

        if self.database.is_primary() {
            match self
                .connections
                .get(&to)
                .and_then(|m| m.get(&connection_id).cloned())
            {
                Some(handle) => {
                    tokio::spawn(exec(handle));
                }
                None => {
                    let handle = self.new_conn(Some((to, connection_id))).await;
                    tokio::spawn(exec(handle));
                }
            }
        }
    }

    async fn new_conn(&mut self, remote: Option<(NodeId, u32)>) -> ConnectionHandle {
        let conn_id = self.next_conn_id();
        let conn = block_in_place(|| self.database.connect(conn_id, self));
        let (exec_sender, exec_receiver) = mpsc::channel(1);
        let (inbound_sender, inbound_receiver) = mpsc::channel(1);
        let id = remote.unwrap_or((self.dispatcher.node_id(), conn_id));
        let conn = Connection {
            id,
            conn,
            exec: exec_receiver,
            inbound: inbound_receiver,
        };

        self.connections_futs.spawn(conn.run());
        let handle = ConnectionHandle {
            exec: exec_sender,
            inbound: inbound_sender,
        };
        self.connections
            .entry(id.0)
            .or_insert_with(HashMap::new)
            .insert(id.1, handle.clone());
        handle
    }

    fn next_conn_id(&mut self) -> u32 {
        loop {
            self.next_conn_id = self.next_conn_id.wrapping_add(1);
            if self
                .connections
                .get(&self.dispatcher.node_id())
                .and_then(|m| m.get(&self.next_conn_id))
                .is_none()
            {
                return self.next_conn_id;
            }
        }
    }
}

#[async_trait::async_trait]
trait ConnectionHandler: Send {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()>;
    async fn handle_exec(&mut self, exec: ExecFn);
    async fn handle_inbound(&mut self, msg: Inbound);
}

#[async_trait::async_trait]
impl<L, R> ConnectionHandler for Either<L, R>
where
    L: ConnectionHandler,
    R: ConnectionHandler,
{
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Either::Left(l) => l.poll_ready(cx),
            Either::Right(r) => r.poll_ready(cx),
        }
    }

    async fn handle_exec(&mut self, exec: ExecFn) {
        match self {
            Either::Left(l) => l.handle_exec(exec).await,
            Either::Right(r) => r.handle_exec(exec).await,
        }
    }
    async fn handle_inbound(&mut self, msg: Inbound) {
        match self {
            Either::Left(l) => l.handle_inbound(msg).await,
            Either::Right(r) => r.handle_inbound(msg).await,
        }
    }
}

struct Connection<C> {
    id: (NodeId, u32),
    conn: C,
    exec: mpsc::Receiver<ExecFn>,
    inbound: mpsc::Receiver<Inbound>,
}

impl<C: ConnectionHandler> Connection<C> {
    async fn run(mut self) -> (NodeId, u32) {
        loop {
            let fut =
                futures::future::join(self.exec.recv(), poll_fn(|cx| self.conn.poll_ready(cx)));
            tokio::select! {
                Some(inbound) = self.inbound.recv() => {
                    self.conn.handle_inbound(inbound).await;
                }
                (Some(exec), _) = fut => {
                    self.conn.handle_exec(exec).await;
                },
                else => break,
            }
        }

        self.id
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use libsqlx::result_builder::ResultBuilder;
    use tokio::sync::Notify;

    use crate::allocation::replica::ReplicaConnection;
    use crate::linc::bus::Bus;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn proxy_request_timeout() {
        let bus = Arc::new(Bus::new(0, |_, _| async {}));
        let _queue = bus.connect(1); // pretend connection to node 1
        let tmp = tempfile::TempDir::new().unwrap();
        let read_db = LibsqlDatabase::new_replica(tmp.path().to_path_buf(), 1, ()).unwrap();
        let write_db = RemoteDb {
            proxy_request_timeout_duration: Duration::from_millis(100),
        };
        let db = WriteProxyDatabase::new(read_db, write_db, Arc::new(|_| ()));
        let conn = db.connect().unwrap();
        let conn = ReplicaConnection {
            conn,
            connection_id: 0,
            next_req_id: 0,
            primary_node_id: 1,
            database_id: DatabaseId::random(),
            dispatcher: bus,
        };

        let (exec_sender, exec) = mpsc::channel(1);
        let (_inbound_sender, inbound) = mpsc::channel(1);
        let connection = Connection {
            id: (0, 0),
            conn,
            exec,
            inbound,
        };

        let handle = tokio::spawn(connection.run());

        let notify = Arc::new(Notify::new());
        struct Builder(Arc<Notify>);
        impl ResultBuilder for Builder {
            fn finnalize_error(&mut self, _e: String) {
                self.0.notify_waiters()
            }
        }

        let builder = Box::new(Builder(notify.clone()));
        exec_sender
            .send(Box::new(move |conn| {
                conn.execute_program(&Program::seq(&["create table test (c)"]), builder)
                    .unwrap();
            }))
            .await
            .unwrap();

        notify.notified().await;

        handle.abort();
    }
}
