#![allow(dead_code)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tonic::transport::ClientTlsConfig;

use crate::replication::frame::{Frame, FrameHeader};
use crate::replication::primary::logger::WalPage;
use crate::replication::{FrameNo, ReplicationLogger, CRC_64_GO_ISO, WAL_PAGE_SIZE};
use crate::stats::Stats;
use crate::Result;
use crate::{auth::Authenticated, error::Error, query::QueryResult, query_analysis::State};

use self::init::InitState;
use self::primary::PrimaryState;
use self::replica::ReplicaState;

use super::{factory::DbFactory, Database, Program};

mod init;
mod primary;
mod replica;

struct BackboneReplicationLogger {
    buffer: RwLock<Vec<Frame>>,
    current_frame_no: AtomicU64,
    current_checksum: AtomicU64,
    sender: mpsc::Sender<(Vec<Frame>, oneshot::Sender<anyhow::Result<()>>)>,
}
impl BackboneReplicationLogger {
    fn new(
        current_frame_no: FrameNo,
        current_checksum: u64,
        sender: mpsc::Sender<(Vec<Frame>, oneshot::Sender<anyhow::Result<()>>)>,
    ) -> Self {
        Self {
            buffer: RwLock::new(Vec::new()),
            current_frame_no: current_frame_no.into(),
            current_checksum: current_checksum.into(),
            sender,
        }
    }
}

impl ReplicationLogger for BackboneReplicationLogger {
    fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<(u64, u64)> {
        let mut buffer = self.buffer.write();
        let mut current_frame_no =
            self.current_frame_no.load(Ordering::Relaxed) + buffer.len() as u64;
        let mut current_checksum = self.current_checksum.load(Ordering::Relaxed);
        for page in pages.iter() {
            assert_eq!(page.data.len(), WAL_PAGE_SIZE as usize);
            let mut digest = CRC_64_GO_ISO.digest_with_initial(current_checksum);
            digest.update(&page.data);
            let checksum = digest.finalize();
            let header = FrameHeader {
                frame_no: current_frame_no,
                checksum,
                page_no: page.page_no,
                size_after: page.size_after,
            };

            let frame = Frame::from_parts(&header, &page.data);

            buffer.push(frame);

            current_frame_no += 1;
            current_checksum = checksum;
        }
        self.current_frame_no
            .store(current_frame_no, Ordering::Relaxed);

        // don't care about those values
        Ok((0, 0))
    }

    fn commit(
        &self,
        new_frame_count: u64,
        new_current_checksum: u64,
    ) -> anyhow::Result<crate::replication::FrameNo> {
        // don't care
        assert_eq!((0, 0), (new_frame_count, new_current_checksum));

        let data = std::mem::take(&mut *self.buffer.write());
        let frame_no = data.last().unwrap().header().frame_no;
        let (sender, ret) = oneshot::channel();
        self.sender.blocking_send((data, sender))?;
        ret.blocking_recv()??;

        Ok(frame_no)
    }

    // no-op
    fn maybe_compact(&self, _size_after: u32) {}
}

pub enum Role<'a> {
    Init(InitState<'a>),
    Primary(PrimaryState<'a>),
    Replica(ReplicaState<'a>),
}

impl<'a> Role<'a> {
    pub fn transition(
        role: impl Into<Role<'a>>,
        meta: MetaMessage,
        offset: i64,
    ) -> anyhow::Result<Self> {
        let backbone = role.into().backbone();
        backbone.term = meta.term;
        if meta.primary_infos.id == backbone.config.node_id {
            // we are the new primary
            let primary = PrimaryState::new(backbone, offset as _)?;
            Ok(Role::Primary(primary))
        } else {
            Ok(Role::Replica(ReplicaState::new(
                backbone,
                meta.primary_infos,
            )))
        }
    }

    fn backbone(self) -> &'a mut BackboneDatabase {
        let role = match self {
            Role::Init(i) => i.backbone,
            Role::Primary(p) => p.backbone,
            Role::Replica(r) => r.backbone,
        };
        role
    }
}

impl<'a> From<InitState<'a>> for Role<'a> {
    fn from(state: InitState<'a>) -> Self {
        Self::Init(state)
    }
}

impl<'a> From<PrimaryState<'a>> for Role<'a> {
    fn from(state: PrimaryState<'a>) -> Self {
        Self::Primary(state)
    }
}

impl<'a> From<ReplicaState<'a>> for Role<'a> {
    fn from(state: ReplicaState<'a>) -> Self {
        Self::Replica(state)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Id of the node
    id: String,
    /// address and port on which the node is listening for rpc call
    addr: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaMessage {
    primary_infos: NodeInfo,
    term: u64,
}

struct Connections {
    factory: Box<dyn DbFactory>,
    connections: HashMap<u64, Arc<dyn Database>>,
    futs: JoinSet<()>,
}

impl Connections {
    fn new(factory: Box<dyn DbFactory>) -> Self {
        Self {
            factory,
            connections: HashMap::new(),
            futs: JoinSet::new(),
        }
    }

    async fn handle_op(&mut self, id: u64, op: Message) -> anyhow::Result<()> {
        match op {
            Message::Open => {
                if self.connections.contains_key(&id) {
                    todo!("connection already exist");
                }
                let db = self.factory.create().await.unwrap();
                self.connections.insert(id, db);
            }
            Message::Program { pgm, auth, ret } => match self.connections.get(&id) {
                Some(conn) => {
                    let conn = conn.clone();
                    self.futs.spawn(async move {
                        let res = conn.execute_program(pgm, auth).await;
                        let _ = ret.send(res);
                    });
                }
                None => {
                    todo!("connection closed");
                }
            },
            Message::Close => {
                self.connections.remove(&id);
            }
        }
        Ok(())
    }
}

pub struct BackboneDatabaseConfig {
    pub db_path: PathBuf,
    pub kafka_bootstrap_servers: Vec<SocketAddr>,
    pub extensions: Vec<PathBuf>,
    pub stats: Stats,
    pub cluster_id: String,
    pub node_id: String,
    pub rpc_tls_config: Option<ClientTlsConfig>,
    pub rpc_server_addr: String,
}

pub struct BackboneDatabase {
    config: BackboneDatabaseConfig,
    db_ops_receiver: mpsc::Receiver<(u64, Message)>,
    pub last_frame_no: watch::Sender<FrameNo>,
    term: u64,
}

impl BackboneDatabase {
    pub fn new(config: BackboneDatabaseConfig) -> (Self, BackboneDbHandleFactory) {
        let (sender, db_ops_receiver) = mpsc::channel(256);
        let factory = BackboneDbHandleFactory::new(sender);
        let (last_frame_no, _) = watch::channel(0);
        (
            Self {
                config,
                db_ops_receiver,
                last_frame_no,
                term: 0,
            },
            factory,
        )
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut state = Role::Init(InitState::new(&mut self)?);
        loop {
            dbg!();
            state = match state {
                Role::Init(s) => s.run().await?,
                Role::Primary(s) => match s.run().await? {
                    Some(s) => s,
                    None => return Ok(()),
                },
                Role::Replica(s) => s.run().await?,
            }
        }
    }
}

pub struct BackboneDbHandleFactory {
    sender: mpsc::Sender<(u64, Message)>,
    next_id: AtomicU64,
}

impl BackboneDbHandleFactory {
    fn new(sender: mpsc::Sender<(u64, Message)>) -> Self {
        Self {
            sender,
            next_id: 0.into(),
        }
    }
}

enum Message {
    Open,
    Program {
        pgm: Program,
        auth: Authenticated,
        ret: oneshot::Sender<Result<(Vec<Option<QueryResult>>, State)>>,
    },
    Close,
}

#[async_trait::async_trait]
impl DbFactory for BackboneDbHandleFactory {
    async fn create(&self) -> std::result::Result<Arc<dyn Database>, Error> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let sender = self.sender.clone();
        sender
            .send((id, Message::Open))
            .await
            .map_err(|_| Error::Internal("Failed to open database connection".into()))?;
        Ok(Arc::new(BackboneDbHandle { id, sender }))
    }
}

pub struct BackboneDbHandle {
    sender: mpsc::Sender<(u64, Message)>,
    id: u64,
}

impl Drop for BackboneDbHandle {
    fn drop(&mut self) {
        // drop in a separate task
        let sender = self.sender.clone();
        let id = self.id;
        tokio::spawn(async move {
            let _ = sender.send((id, Message::Close)).await;
        });
    }
}

#[async_trait::async_trait]
impl Database for BackboneDbHandle {
    async fn execute_program(
        &self,
        pgm: Program,
        auth: Authenticated,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        let (ret, rcv) = oneshot::channel();
        let _ = self
            .sender
            .send((self.id, Message::Program { pgm, auth, ret }))
            .await;
        rcv.await.unwrap()
    }
}
