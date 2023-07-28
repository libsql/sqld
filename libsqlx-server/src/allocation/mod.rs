use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::poll_fn;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use either::Either;
use libsqlx::libsql::LibsqlDatabase;
use libsqlx::program::Program;
use libsqlx::proxy::WriteProxyDatabase;
use libsqlx::result_builder::ResultBuilder;
use libsqlx::{Database as _, InjectableDatabase};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{block_in_place, JoinSet};
use tokio::time::Interval;

use crate::allocation::primary::FrameStreamer;
use crate::allocation::timeout_notifier::timeout_monitor;
use crate::compactor::CompactionQueue;
use crate::hrana;
use crate::hrana::http::handle_pipeline;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Dispatch;
use crate::linc::proto::{Frames, Message};
use crate::linc::{Inbound, NodeId};
use crate::meta::DatabaseId;
use crate::replica_commit_store::ReplicaCommitStore;

use self::config::{AllocConfig, DbConfig};
use self::primary::compactor::Compactor;
use self::primary::{PrimaryConnection, PrimaryDatabase, ProxyResponseBuilder};
use self::replica::{ProxyDatabase, RemoteDb, ReplicaConnection, Replicator};
use self::timeout_notifier::TimeoutMonitor;

pub mod config;
mod primary;
mod replica;
mod timeout_notifier;

/// Maximum number of frame a Frame message is allowed to contain
const FRAMES_MESSAGE_MAX_COUNT: usize = 5;
/// Maximum number of frames in the injector buffer
const MAX_INJECTOR_BUFFER_CAPACITY: usize = 32;

pub enum ConnectionMessage {
    Execute {
        pgm: Program,
        builder: Box<dyn ResultBuilder>,
    },
    Describe,
}

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
        transaction_timeout_duration: Duration,
    },
    Replica {
        db: ProxyDatabase,
        injector_handle: mpsc::Sender<Frames>,
        primary_id: NodeId,
        last_received_frame_ts: Option<Instant>,
        transaction_timeout_duration: Duration,
    },
}

impl Database {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Self::Primary {
            compact_interval: Some(ref mut interval),
            db,
            ..
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

    fn txn_timeout_duration(&self) -> Duration {
        match self {
            Database::Primary {
                transaction_timeout_duration,
                ..
            } => *transaction_timeout_duration,
            Database::Replica {
                transaction_timeout_duration,
                ..
            } => *transaction_timeout_duration,
        }
    }
}

impl Database {
    pub fn from_config(
        config: &AllocConfig,
        path: PathBuf,
        dispatcher: Arc<dyn Dispatch>,
        compaction_queue: Arc<CompactionQueue>,
        replica_commit_store: Arc<ReplicaCommitStore>,
    ) -> Self {
        let database_id = DatabaseId::from_name(&config.db_name);

        match config.db_config {
            DbConfig::Primary {
                max_log_size,
                replication_log_compact_interval,
                transaction_timeout_duration,
            } => {
                let (sender, receiver) = tokio::sync::watch::channel(None);
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
                        let _ = sender.send(Some(fno));
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
                    transaction_timeout_duration,
                }
            }
            DbConfig::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
                transaction_timeout_duration,
            } => {
                let next_frame_no =
                    block_in_place(|| replica_commit_store.get_commit_index(database_id))
                        .map(|fno| fno + 1)
                        .unwrap_or(0);

                let commit_callback = Arc::new(move |fno| {
                    replica_commit_store.commit(database_id, fno);
                });

                let rdb = LibsqlDatabase::new_replica(
                    path,
                    MAX_INJECTOR_BUFFER_CAPACITY,
                    commit_callback,
                )
                .unwrap();

                let wdb = RemoteDb {
                    proxy_request_timeout_duration,
                };
                let mut db = WriteProxyDatabase::new(rdb, wdb, Arc::new(|_| ()));
                let injector = db.injector().unwrap();
                let (sender, receiver) = mpsc::channel(16);

                let replicator = Replicator::new(
                    dispatcher,
                    next_frame_no,
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
                    transaction_timeout_duration,
                }
            }
        }
    }

    fn connect(
        &self,
        connection_id: u32,
        alloc: &Allocation,
        on_txn_status_change_cb: impl Fn(bool) + Send + Sync + 'static,
    ) -> impl ConnectionHandler {
        match self {
            Database::Primary {
                db: PrimaryDatabase { db, .. },
                ..
            } => {
                let mut conn = db.connect().unwrap();
                conn.set_on_txn_status_change_cb(on_txn_status_change_cb);
                Either::Right(PrimaryConnection { conn })
            }
            Database::Replica { db, primary_id, .. } => {
                let mut conn = db.connect().unwrap();
                conn.reader_mut()
                    .set_on_txn_status_change_cb(on_txn_status_change_cb);
                Either::Left(ReplicaConnection {
                    conn,
                    connection_id,
                    next_req_id: 0,
                    primary_node_id: *primary_id,
                    database_id: DatabaseId::from_name(&alloc.db_name),
                    dispatcher: alloc.dispatcher.clone(),
                })
            }
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
    messages: mpsc::Sender<ConnectionMessage>,
    inbound: mpsc::Sender<Inbound>,
}

impl ConnectionHandle {
    pub async fn execute(
        &self,
        pgm: Program,
        builder: Box<dyn ResultBuilder>,
    ) -> crate::Result<()> {
        self.messages
            .send(ConnectionMessage::Execute { pgm, builder })
            .await
            .unwrap();
        Ok(())
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
            let builder =
                ProxyResponseBuilder::new(dispatcher, database_id, to, req_id, connection_id);
            conn.execute(program, Box::new(builder)).await.unwrap();
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
        let (timeout_monitor, notifier) = timeout_monitor();
        let timeout = self.database.txn_timeout_duration();
        let conn = block_in_place(|| {
            self.database.connect(conn_id, self, move |is_txn| {
                if is_txn {
                    notifier.timeout_at(Instant::now() + timeout);
                } else {
                    notifier.disable();
                }
            })
        });

        let (messages_sender, messages_receiver) = mpsc::channel(1);
        let (inbound_sender, inbound_receiver) = mpsc::channel(1);
        let id = remote.unwrap_or((self.dispatcher.node_id(), conn_id));
        let conn = Connection {
            id,
            conn,
            messages: messages_receiver,
            inbound: inbound_receiver,
            last_txn_timedout: false,
            timeout_monitor,
        };

        self.connections_futs.spawn(conn.run());
        let handle = ConnectionHandle {
            messages: messages_sender,
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
    async fn handle_conn_message(&mut self, exec: ConnectionMessage);
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

    async fn handle_conn_message(&mut self, msg: ConnectionMessage) {
        match self {
            Either::Left(l) => l.handle_conn_message(msg).await,
            Either::Right(r) => r.handle_conn_message(msg).await,
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
    messages: mpsc::Receiver<ConnectionMessage>,
    inbound: mpsc::Receiver<Inbound>,
    last_txn_timedout: bool,
    timeout_monitor: TimeoutMonitor,
}

impl<C: ConnectionHandler> Connection<C> {
    async fn run(mut self) -> (NodeId, u32) {
        loop {
            let message_ready =
                futures::future::join(self.messages.recv(), poll_fn(|cx| self.conn.poll_ready(cx)));

            tokio::select! {
                _ = &mut self.timeout_monitor => {
                    self.last_txn_timedout = true;
                }
                Some(inbound) = self.inbound.recv() => {
                    self.conn.handle_inbound(inbound).await;
                }
                (Some(msg), _) = message_ready => {
                    if self.last_txn_timedout {
                        self.last_txn_timedout = false;
                        match msg {
                            ConnectionMessage::Execute { mut builder, .. } => {
                                let _ = builder.finnalize_error("transaction has timed out".into());
                            },
                            ConnectionMessage::Describe => todo!(),
                        }
                    } else {
                        self.conn.handle_conn_message(msg).await;
                    }
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

    use heed::EnvOpenOptions;
    use libsqlx::result_builder::{ResultBuilder, StepResultsBuilder};
    use tempfile::tempdir;
    use tokio::sync::Notify;

    use crate::allocation::replica::ReplicaConnection;
    use crate::linc::bus::Bus;
    use crate::replica_commit_store::ReplicaCommitStore;
    use crate::snapshot_store::SnapshotStore;
    use crate::{init_dirs, replica_commit_store};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn proxy_request_timeout() {
        let bus = Arc::new(Bus::new(0, |_, _| async {}));
        let _queue = bus.connect(1); // pretend connection to node 1
        let tmp = tempfile::TempDir::new().unwrap();
        let read_db =
            LibsqlDatabase::new_replica(tmp.path().to_path_buf(), 1, Arc::new(|_| ())).unwrap();
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

        let (messages_sender, messages) = mpsc::channel(1);
        let (_inbound_sender, inbound) = mpsc::channel(1);
        let (timeout_monitor, _) = timeout_monitor();
        let connection = Connection {
            id: (0, 0),
            conn,
            messages,
            inbound,
            timeout_monitor,
            last_txn_timedout: false,
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
        let msg = ConnectionMessage::Execute {
            pgm: Program::seq(&["create table test (c)"]),
            builder,
        };
        messages_sender.send(msg).await.unwrap();

        notify.notified().await;

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn txn_timeout() {
        let bus = Arc::new(Bus::new(0, |_, _| async {}));
        let tmp = tempdir().unwrap();
        init_dirs(tmp.path()).await.unwrap();
        let config = AllocConfig {
            max_conccurent_connection: 10,
            db_name: "test/db".to_owned(),
            db_config: DbConfig::Primary {
                max_log_size: 100000,
                replication_log_compact_interval: None,
                transaction_timeout_duration: Duration::from_millis(100),
            },
        };
        let (sender, inbox) = mpsc::channel(10);
        let env = EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(4096 * 100)
            .open(tmp.path())
            .unwrap();
        let store = Arc::new(SnapshotStore::new(tmp.path().to_path_buf(), env.clone()).unwrap());
        let queue =
            Arc::new(CompactionQueue::new(env.clone(), tmp.path().to_path_buf(), store).unwrap());
        let replica_commit_store = Arc::new(ReplicaCommitStore::new(env.clone()));
        let mut alloc = Allocation {
            inbox,
            database: Database::from_config(
                &config,
                tmp.path().to_path_buf(),
                bus.clone(),
                queue,
                replica_commit_store,
            ),
            connections_futs: JoinSet::new(),
            next_conn_id: 0,
            max_concurrent_connections: config.max_conccurent_connection,
            hrana_server: Arc::new(hrana::http::Server::new(None)),
            dispatcher: bus, // TODO: handle self URL?
            db_name: config.db_name,
            connections: HashMap::new(),
        };

        let conn = alloc.new_conn(None).await;
        tokio::spawn(alloc.run());

        let (snd, rcv) = oneshot::channel();
        let builder = StepResultsBuilder::new(snd);
        conn.execute(Program::seq(&["begin"]), Box::new(builder))
            .await
            .unwrap();
        rcv.await.unwrap().unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (snd, rcv) = oneshot::channel();
        let builder = StepResultsBuilder::new(snd);
        conn.execute(Program::seq(&["create table test (x)"]), Box::new(builder))
            .await
            .unwrap();
        assert!(rcv.await.unwrap().is_err());
    }
}
