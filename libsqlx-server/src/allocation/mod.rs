use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::poll_fn;
use std::mem::size_of;
use std::ops::Deref;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use either::Either;
use futures::Future;
use libsqlx::libsql::{LibsqlDatabase, LogCompactor, LogFile, PrimaryType, ReplicaType};
use libsqlx::program::Program;
use libsqlx::proxy::{WriteProxyConnection, WriteProxyDatabase};
use libsqlx::result_builder::{Column, QueryBuilderConfig, ResultBuilder};
use libsqlx::{
    Database as _, DescribeResponse, Frame, FrameNo, InjectableDatabase, Injector, LogReadError,
    ReplicationLogger,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{block_in_place, JoinSet};
use tokio::time::{timeout, Sleep};

use crate::hrana;
use crate::hrana::http::handle_pipeline;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Dispatch;
use crate::linc::proto::{
    BuilderStep, Enveloppe, Frames, Message, ProxyResponse, StepError, Value,
};
use crate::linc::{Inbound, NodeId, Outbound};
use crate::meta::DatabaseId;

use self::config::{AllocConfig, DbConfig};

pub mod config;

/// the maximum number of frame a Frame messahe is allowed to contain
const FRAMES_MESSAGE_MAX_COUNT: usize = 5;

type ProxyConnection =
    WriteProxyConnection<libsqlx::libsql::LibsqlConnection<ReplicaType>, RemoteConn>;
type ExecFn = Box<dyn FnOnce(&mut dyn libsqlx::Connection) + Send>;

pub enum AllocationMessage {
    HranaPipelineReq {
        req: PipelineRequestBody,
        ret: oneshot::Sender<crate::Result<PipelineResponseBody>>,
    },
    Inbound(Inbound),
}

pub struct RemoteDb {
    proxy_request_timeout_duration: Duration,
}

#[derive(Clone)]
pub struct RemoteConn {
    inner: Arc<RemoteConnInner>,
}

struct Request {
    id: Option<u32>,
    builder: Box<dyn ResultBuilder>,
    pgm: Option<Program>,
    next_seq_no: u32,
    timeout: Pin<Box<Sleep>>,
}

pub struct RemoteConnInner {
    current_req: Mutex<Option<Request>>,
    request_timeout_duration: Duration,
}

impl Deref for RemoteConn {
    type Target = RemoteConnInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl libsqlx::Connection for RemoteConn {
    fn execute_program(
        &mut self,
        program: &libsqlx::program::Program,
        builder: Box<dyn ResultBuilder>,
    ) -> libsqlx::Result<()> {
        // When we need to proxy a query, we place it in the current request slot. When we are
        // back in a async context, we'll send it to the primary, and asynchrously drive the
        // builder.
        let mut lock = self.inner.current_req.lock();
        *lock = match *lock {
            Some(_) => unreachable!("conccurent request on the same connection!"),
            None => Some(Request {
                id: None,
                builder,
                pgm: Some(program.clone()),
                next_seq_no: 0,
                timeout: Box::pin(tokio::time::sleep(self.inner.request_timeout_duration)),
            }),
        };

        Ok(())
    }

    fn describe(&self, _sql: String) -> libsqlx::Result<DescribeResponse> {
        unreachable!("Describe request should not be proxied")
    }
}

impl libsqlx::Database for RemoteDb {
    type Connection = RemoteConn;

    fn connect(&self) -> Result<Self::Connection, libsqlx::error::Error> {
        Ok(RemoteConn {
            inner: Arc::new(RemoteConnInner {
                current_req: Default::default(),
                request_timeout_duration: self.proxy_request_timeout_duration,
            }),
        })
    }
}

pub type ProxyDatabase = WriteProxyDatabase<LibsqlDatabase<ReplicaType>, RemoteDb>;

pub struct PrimaryDatabase {
    pub db: LibsqlDatabase<PrimaryType>,
    pub replica_streams: HashMap<NodeId, (u32, tokio::task::JoinHandle<()>)>,
    pub frame_notifier: tokio::sync::watch::Receiver<FrameNo>,
}

struct ProxyResponseBuilder {
    dispatcher: Arc<dyn Dispatch>,
    buffer: Vec<BuilderStep>,
    to: NodeId,
    database_id: DatabaseId,
    req_id: u32,
    connection_id: u32,
    next_seq_no: u32,
}

const MAX_STEP_BATCH_SIZE: usize = 100_000_000; // ~100kb

impl ProxyResponseBuilder {
    fn maybe_send(&mut self) {
        // FIXME: this is stupid: compute current buffer size on the go instead
        let size = self
            .buffer
            .iter()
            .map(|s| match s {
                BuilderStep::FinishStep(_, _) => 2 * 8,
                BuilderStep::StepError(StepError(s)) => s.len(),
                BuilderStep::ColsDesc(ref d) => d
                    .iter()
                    .map(|c| c.name.len() + c.decl_ty.as_ref().map(|t| t.len()).unwrap_or_default())
                    .sum(),
                BuilderStep::Finnalize { .. } => 9,
                BuilderStep::AddRowValue(v) => match v {
                    crate::linc::proto::Value::Text(s) | crate::linc::proto::Value::Blob(s) => {
                        s.len()
                    }
                    _ => size_of::<Value>(),
                },
                _ => 8,
            })
            .sum::<usize>();

        if size > MAX_STEP_BATCH_SIZE {
            self.send()
        }
    }

    fn send(&mut self) {
        let msg = Outbound {
            to: self.to,
            enveloppe: Enveloppe {
                database_id: Some(self.database_id),
                message: Message::ProxyResponse(crate::linc::proto::ProxyResponse {
                    connection_id: self.connection_id,
                    req_id: self.req_id,
                    row_steps: std::mem::take(&mut self.buffer),
                    seq_no: self.next_seq_no,
                }),
            },
        };

        self.next_seq_no += 1;
        tokio::runtime::Handle::current().block_on(self.dispatcher.dispatch(msg));
    }
}

impl ResultBuilder for ProxyResponseBuilder {
    fn init(
        &mut self,
        _config: &libsqlx::result_builder::QueryBuilderConfig,
    ) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::Init);
        self.maybe_send();
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::BeginStep);
        self.maybe_send();
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::FinishStep(
            affected_row_count,
            last_insert_rowid,
        ));
        self.maybe_send();
        Ok(())
    }

    fn step_error(
        &mut self,
        error: libsqlx::error::Error,
    ) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer
            .push(BuilderStep::StepError(StepError(error.to_string())));
        self.maybe_send();
        Ok(())
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = libsqlx::result_builder::Column>,
    ) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer
            .push(BuilderStep::ColsDesc(cols.map(Into::into).collect()));
        self.maybe_send();
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::BeginRows);
        self.maybe_send();
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::BeginRow);
        self.maybe_send();
        Ok(())
    }

    fn add_row_value(
        &mut self,
        v: libsqlx::result_builder::ValueRef,
    ) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::AddRowValue(v.into()));
        self.maybe_send();
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::FinishRow);
        self.maybe_send();
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer.push(BuilderStep::FinishRows);
        self.maybe_send();
        Ok(())
    }

    fn finnalize(
        &mut self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, libsqlx::result_builder::QueryResultBuilderError> {
        self.buffer
            .push(BuilderStep::Finnalize { is_txn, frame_no });
        self.send();
        Ok(true)
    }
}

pub enum Database {
    Primary(PrimaryDatabase),
    Replica {
        db: ProxyDatabase,
        injector_handle: mpsc::Sender<Frames>,
        primary_id: NodeId,
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
    next_frame_no: FrameNo,
    next_seq: u32,
    database_id: DatabaseId,
    primary_node_id: NodeId,
    injector: Box<dyn Injector + Send + 'static>,
    receiver: mpsc::Receiver<Frames>,
}

impl Replicator {
    async fn run(mut self) {
        self.query_replicate().await;
        loop {
            match timeout(Duration::from_secs(5), self.receiver.recv()).await {
                Ok(Some(Frames {
                    req_no: req_id,
                    seq_no: seq,
                    frames,
                })) => {
                    // ignore frames from a previous call to Replicate
                    if req_id != self.req_id {
                        tracing::debug!(req_id, self.req_id, "wrong req_id");
                        continue;
                    }
                    if seq != self.next_seq {
                        // this is not the batch of frame we were expecting, drop what we have, and
                        // ask again from last checkpoint
                        tracing::debug!(seq, self.next_seq, "wrong seq");
                        self.query_replicate().await;
                        continue;
                    };
                    self.next_seq += 1;

                    tracing::debug!("injecting {} frames", frames.len());

                    for bytes in frames {
                        let frame = Frame::try_from_bytes(bytes).unwrap();
                        block_in_place(|| {
                            if let Some(last_committed) = self.injector.inject(frame).unwrap() {
                                tracing::debug!(last_committed);
                                self.next_frame_no = last_committed + 1;
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
                        next_frame_no: self.next_frame_no,
                        req_no: self.req_id,
                    },
                },
            })
            .await;
    }
}

struct FrameStreamer {
    logger: Arc<ReplicationLogger>,
    database_id: DatabaseId,
    node_id: NodeId,
    next_frame_no: FrameNo,
    req_no: u32,
    seq_no: u32,
    dipatcher: Arc<dyn Dispatch>,
    notifier: tokio::sync::watch::Receiver<FrameNo>,
    buffer: Vec<Bytes>,
}

impl FrameStreamer {
    async fn run(mut self) {
        loop {
            match block_in_place(|| self.logger.get_frame(self.next_frame_no)) {
                Ok(frame) => {
                    if self.buffer.len() > FRAMES_MESSAGE_MAX_COUNT {
                        self.send_frames().await;
                    }
                    self.buffer.push(frame.bytes());
                    self.next_frame_no += 1;
                }
                Err(LogReadError::Ahead) => {
                    tracing::debug!("frame {} not yet avaiblable", self.next_frame_no);
                    if !self.buffer.is_empty() {
                        self.send_frames().await;
                    }
                    if self
                        .notifier
                        .wait_for(|fno| *fno >= self.next_frame_no)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(LogReadError::Error(_)) => todo!("handle log read error"),
                Err(LogReadError::SnapshotRequired) => todo!("handle reading from snapshot"),
            }
        }
    }

    async fn send_frames(&mut self) {
        let frames = std::mem::take(&mut self.buffer);
        let outbound = Outbound {
            to: self.node_id,
            enveloppe: Enveloppe {
                database_id: Some(self.database_id),
                message: Message::Frames(Frames {
                    req_no: self.req_no,
                    seq_no: self.seq_no,
                    frames,
                }),
            },
        };
        self.seq_no += 1;
        self.dipatcher.dispatch(outbound).await;
    }
}

impl Database {
    pub fn from_config(config: &AllocConfig, path: PathBuf, dispatcher: Arc<dyn Dispatch>) -> Self {
        match config.db_config {
            DbConfig::Primary {} => {
                let (sender, receiver) = tokio::sync::watch::channel(0);
                let db = LibsqlDatabase::new_primary(
                    path,
                    Compactor,
                    false,
                    Box::new(move |fno| {
                        let _ = sender.send(fno);
                    }),
                )
                .unwrap();

                Self::Primary(PrimaryDatabase {
                    db,
                    replica_streams: HashMap::new(),
                    frame_notifier: receiver,
                })
            }
            DbConfig::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
            } => {
                let rdb = LibsqlDatabase::new_replica(path, MAX_INJECTOR_BUFFER_CAP, ()).unwrap();
                let wdb = RemoteDb {
                    proxy_request_timeout_duration,
                };
                let mut db = WriteProxyDatabase::new(rdb, wdb, Arc::new(|_| ()));
                let injector = db.injector().unwrap();
                let (sender, receiver) = mpsc::channel(16);
                let database_id = DatabaseId::from_name(&config.db_name);

                let replicator = Replicator {
                    dispatcher,
                    req_id: 0,
                    next_frame_no: 0, // TODO: load the last commited from meta file
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
                    primary_id: primary_node_id,
                    last_received_frame_ts: None,
                }
            }
        }
    }

    fn connect(&self, connection_id: u32, alloc: &Allocation) -> impl ConnectionHandler {
        match self {
            Database::Primary(PrimaryDatabase { db, .. }) => Either::Right(PrimaryConnection {
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
        matches!(self, Self::Primary(..))
    }
}

struct PrimaryConnection {
    conn: libsqlx::libsql::LibsqlConnection<PrimaryType>,
}

#[async_trait::async_trait]
impl ConnectionHandler for PrimaryConnection {
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }

    async fn handle_exec(&mut self, exec: ExecFn) {
        block_in_place(|| exec(&mut self.conn));
    }

    async fn handle_inbound(&mut self, _msg: Inbound) {
        tracing::debug!("primary connection received message, ignoring.")
    }
}

struct ReplicaConnection {
    conn: ProxyConnection,
    connection_id: u32,
    next_req_id: u32,
    primary_node_id: NodeId,
    database_id: DatabaseId,
    dispatcher: Arc<dyn Dispatch>,
}

impl ReplicaConnection {
    fn handle_proxy_response(&mut self, resp: ProxyResponse) {
        let mut lock = self.conn.writer().inner.current_req.lock();
        let finnalized = match *lock {
            Some(ref mut req) if req.id == Some(resp.req_id) && resp.seq_no == req.next_seq_no => {
                self.next_req_id += 1;
                // TODO: pass actual config
                let config = QueryBuilderConfig { max_size: None };
                let mut finnalized = false;
                for step in resp.row_steps.into_iter() {
                    if finnalized {
                        break;
                    };
                    match step {
                        BuilderStep::Init => req.builder.init(&config).unwrap(),
                        BuilderStep::BeginStep => req.builder.begin_step().unwrap(),
                        BuilderStep::FinishStep(affected_row_count, last_insert_rowid) => req
                            .builder
                            .finish_step(affected_row_count, last_insert_rowid)
                            .unwrap(),
                        BuilderStep::StepError(e) => req
                            .builder
                            .step_error(todo!("handle proxy step error"))
                            .unwrap(),
                        BuilderStep::ColsDesc(cols) => req
                            .builder
                            .cols_description(&mut cols.iter().map(|c| Column {
                                name: &c.name,
                                decl_ty: c.decl_ty.as_deref(),
                            }))
                            .unwrap(),
                        BuilderStep::BeginRows => req.builder.begin_rows().unwrap(),
                        BuilderStep::BeginRow => req.builder.begin_row().unwrap(),
                        BuilderStep::AddRowValue(v) => req.builder.add_row_value((&v).into()).unwrap(),
                        BuilderStep::FinishRow => req.builder.finish_row().unwrap(),
                        BuilderStep::FinishRows => req.builder.finish_rows().unwrap(),
                        BuilderStep::Finnalize { is_txn, frame_no } => {
                            let _ = req.builder.finnalize(is_txn, frame_no).unwrap();
                            finnalized = true;
                        },
                        BuilderStep::FinnalizeError(e) => { 
                            req.builder.finnalize_error(e);
                            finnalized = true;
                        }
                    }
                }
                finnalized
            }
            Some(_) => todo!("error processing response"),
            None => {
                tracing::error!("received builder message, but there is no pending request");
                false
            }
        };

        if finnalized {
            *lock = None;
        }
    }
}

#[async_trait::async_trait]
impl ConnectionHandler for ReplicaConnection {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // we are currently handling a request on this connection
        // self.conn.writer().current_req.timeout.poll()
        let mut req = self.conn.writer().current_req.lock();
        let should_abort_query = match &mut *req {
            Some(ref mut req) => {
                match req.timeout.as_mut().poll(cx) {
                    Poll::Ready(_) => {
                        req.builder.finnalize_error("request timed out".to_string());
                        true
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            None => return Poll::Ready(()),
        };

        if should_abort_query {
            *req = None
        }

        Poll::Ready(())
    }

    async fn handle_exec(&mut self, exec: ExecFn) {
        block_in_place(|| exec(&mut self.conn));
        let msg = {
            let mut lock = self.conn.writer().inner.current_req.lock();
            match *lock {
                Some(ref mut req) if req.id.is_none() => {
                    let program = req
                        .pgm
                        .take()
                        .expect("unsent request should have a program");
                    let req_id = self.next_req_id;
                    self.next_req_id += 1;
                    req.id = Some(req_id);

                    let msg = Outbound {
                        to: self.primary_node_id,
                        enveloppe: Enveloppe {
                            database_id: Some(self.database_id),
                            message: Message::ProxyRequest {
                                connection_id: self.connection_id,
                                req_id,
                                program,
                            },
                        },
                    };

                    Some(msg)
                }
                _ => None,
            }
        };

        if let Some(msg) = msg {
            self.dispatcher.dispatch(msg).await;
        }
    }

    async fn handle_inbound(&mut self, msg: Inbound) {
        match msg.enveloppe.message {
            Message::ProxyResponse(resp) => {
                self.handle_proxy_response(resp);
            }
            _ => (), // ignore anything else
        }
    }
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
            tokio::select! {
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
            Message::Handshake { .. } => unreachable!("handshake should have been caught earlier"),
            Message::ReplicationHandshake { .. } => todo!(),
            Message::ReplicationHandshakeResponse { .. } => todo!(),
            Message::Replicate {
                req_no,
                next_frame_no,
            } => match &mut self.database {
                Database::Primary(PrimaryDatabase {
                    db,
                    replica_streams,
                    frame_notifier,
                    ..
                }) => {
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
                Database::Primary(PrimaryDatabase { .. }) => todo!("handle primary receiving txn"),
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
        node_id: NodeId,
        connection_id: u32,
        req_id: u32,
        program: Program,
    ) {
        let dispatcher = self.dispatcher.clone();
        let database_id = DatabaseId::from_name(&self.db_name);
        let exec = |conn: ConnectionHandle| async move {
            let _ = conn
                .exec(move |conn| {
                    let builder = ProxyResponseBuilder {
                        dispatcher,
                        req_id,
                        buffer: Vec::new(),
                        to: node_id,
                        database_id,
                        connection_id,
                        next_seq_no: 0,
                    };
                    conn.execute_program(&program, Box::new(builder)).unwrap();
                })
                .await;
        };

        if self.database.is_primary() {
            match self
                .connections
                .get(&node_id)
                .and_then(|m| m.get(&connection_id).cloned())
            {
                Some(handle) => {
                    tokio::spawn(exec(handle));
                }
                None => {
                    let handle = self.new_conn(Some((node_id, connection_id))).await;
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

struct Connection<C> {
    id: (NodeId, u32),
    conn: C,
    exec: mpsc::Receiver<ExecFn>,
    inbound: mpsc::Receiver<Inbound>,
}

#[async_trait::async_trait]
trait ConnectionHandler: Send {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()>;
    async fn handle_exec(&mut self, exec: ExecFn);
    async fn handle_inbound(&mut self, msg: Inbound);
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
    use tokio::sync::Notify;

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
