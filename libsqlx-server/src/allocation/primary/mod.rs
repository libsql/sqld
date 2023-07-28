use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use libsqlx::libsql::{LibsqlDatabase, PrimaryType};
use libsqlx::result_builder::ResultBuilder;
use libsqlx::{Connection, Frame, FrameHeader, FrameNo, LogReadError, ReplicationLogger};
use tokio::task::block_in_place;

use crate::linc::bus::Dispatch;
use crate::linc::proto::{BuilderStep, Enveloppe, Frames, Message, StepError, Value};
use crate::linc::{Inbound, NodeId, Outbound};
use crate::meta::DatabaseId;
use crate::snapshot_store::SnapshotStore;

use super::{ConnectionHandler, ConnectionMessage, FRAMES_MESSAGE_MAX_COUNT};

pub mod compactor;

const MAX_STEP_BATCH_SIZE: usize = 100_000_000; // ~100kb
                                                //
pub struct PrimaryDatabase {
    pub db: Arc<LibsqlDatabase<PrimaryType>>,
    pub replica_streams: HashMap<NodeId, (u32, tokio::task::JoinHandle<()>)>,
    pub frame_notifier: tokio::sync::watch::Receiver<Option<FrameNo>>,
    pub snapshot_store: Arc<SnapshotStore>,
}

pub struct ProxyResponseBuilder {
    dispatcher: Arc<dyn Dispatch>,
    buffer: Vec<BuilderStep>,
    database_id: DatabaseId,
    to: NodeId,
    req_id: u32,
    connection_id: u32,
    next_seq_no: u32,
}

impl ProxyResponseBuilder {
    pub fn new(
        dispatcher: Arc<dyn Dispatch>,
        database_id: DatabaseId,
        to: NodeId,
        req_id: u32,
        connection_id: u32,
    ) -> Self {
        Self {
            dispatcher,
            buffer: Vec::new(),
            database_id,
            to,
            req_id,
            connection_id,
            next_seq_no: 0,
        }
    }

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

pub struct FrameStreamer {
    pub logger: Arc<ReplicationLogger>,
    pub database_id: DatabaseId,
    pub node_id: NodeId,
    pub next_frame_no: FrameNo,
    pub req_no: u32,
    pub seq_no: u32,
    pub dipatcher: Arc<dyn Dispatch>,
    pub notifier: tokio::sync::watch::Receiver<Option<FrameNo>>,
    pub buffer: Vec<Bytes>,
    pub snapshot_store: Arc<SnapshotStore>,
}

impl FrameStreamer {
    pub async fn run(mut self) {
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
                        .wait_for(|fno| fno.map(|f| f >= self.next_frame_no).unwrap_or(false))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(LogReadError::Error(_)) => todo!("handle log read error"),
                Err(LogReadError::SnapshotRequired) => self.send_snapshot().await,
            }
        }
    }

    async fn send_snapshot(&mut self) {
        tracing::debug!("sending frames from snapshot");
        loop {
            match self
                .snapshot_store
                .locate_file(self.database_id, self.next_frame_no)
            {
                Some(file) => {
                    let mut iter = file.frames_iter_from(self.next_frame_no).peekable();

                    while let Some(frame) = block_in_place(|| iter.next()) {
                        let frame = frame.unwrap();
                        // TODO: factorize in maybe_send
                        if self.buffer.len() > FRAMES_MESSAGE_MAX_COUNT {
                            self.send_frames().await;
                        }
                        let size_after = iter
                            .peek()
                            .is_none()
                            .then_some(file.header.size_after)
                            .unwrap_or(0);
                        let frame = Frame::from_parts(
                            &FrameHeader {
                                frame_no: frame.header().frame_no,
                                page_no: frame.header().page_no,
                                size_after,
                            },
                            frame.page(),
                        );
                        self.next_frame_no = frame.header().frame_no + 1;
                        self.buffer.push(frame.bytes());

                        tokio::task::yield_now().await;
                    }

                    break;
                }
                None => {
                    // snapshot is not ready yet, wait a bit
                    // FIXME: notify when snapshot becomes ready instead of using loop
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
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

pub struct PrimaryConnection {
    pub conn: libsqlx::libsql::LibsqlConnection<PrimaryType>,
}

#[async_trait::async_trait]
impl ConnectionHandler for PrimaryConnection {
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }

    async fn handle_conn_message(&mut self, msg: ConnectionMessage) {
        match msg {
            ConnectionMessage::Execute { pgm, builder } => {
                block_in_place(|| self.conn.execute_program(&pgm, builder).unwrap())
            }
            ConnectionMessage::Describe => {
                todo!()
            }
        }
    }

    async fn handle_inbound(&mut self, _msg: Inbound) {
        tracing::debug!("primary connection received message, ignoring.")
    }
}
