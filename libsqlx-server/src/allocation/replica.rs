use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures::Future;
use libsqlx::libsql::{LibsqlConnection, LibsqlDatabase, ReplicaType};
use libsqlx::program::Program;
use libsqlx::proxy::{WriteProxyConnection, WriteProxyDatabase};
use libsqlx::result_builder::{Column, QueryBuilderConfig, ResultBuilder};
use libsqlx::{Connection, DescribeResponse, Frame, FrameNo, Injector};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::block_in_place;
use tokio::time::{timeout, Sleep};

use crate::linc::bus::Dispatch;
use crate::linc::proto::{BuilderStep, Enveloppe, Frames, Message, ProxyResponse};
use crate::linc::{Inbound, NodeId, Outbound};
use crate::meta::DatabaseId;

use super::{ConnectionHandler, ConnectionMessage};

type ProxyConnection = WriteProxyConnection<LibsqlConnection<ReplicaType>, RemoteConn>;
pub type ProxyDatabase = WriteProxyDatabase<LibsqlDatabase<ReplicaType>, RemoteDb>;

pub struct RemoteDb {
    pub proxy_request_timeout_duration: Duration,
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

pub struct Replicator {
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
    pub fn new(
        dispatcher: Arc<dyn Dispatch>,
        next_frame_no: FrameNo,
        database_id: DatabaseId,
        primary_node_id: NodeId,
        injector: Box<dyn Injector + Send + 'static>,
        receiver: mpsc::Receiver<Frames>,
    ) -> Self {
        Self {
            dispatcher,
            req_id: 0,
            next_frame_no,
            next_seq: 0,
            database_id,
            primary_node_id,
            injector,
            receiver,
        }
    }

    pub async fn run(mut self) {
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
                // no news from primary for the past 5 secs, send a request again
                Err(_) => self.query_replicate().await,
                Ok(None) => break,
            }
        }
    }

    async fn query_replicate(&mut self) {
        tracing::debug!("seinding replication request");
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

pub struct ReplicaConnection {
    pub conn: ProxyConnection,
    pub connection_id: u32,
    pub next_req_id: u32,
    pub primary_node_id: NodeId,
    pub database_id: DatabaseId,
    pub dispatcher: Arc<dyn Dispatch>,
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
                        BuilderStep::StepError(_e) => req
                            .builder
                            .step_error(todo!("handle proxy step error"))
                            .unwrap(),
                        BuilderStep::ColsDesc(cols) => req
                            .builder
                            .cols_description(&mut &mut cols.iter().map(|c| Column {
                                name: &c.name,
                                decl_ty: c.decl_ty.as_deref(),
                            }))
                            .unwrap(),
                        BuilderStep::BeginRows => req.builder.begin_rows().unwrap(),
                        BuilderStep::BeginRow => req.builder.begin_row().unwrap(),
                        BuilderStep::AddRowValue(v) => {
                            req.builder.add_row_value((&v).into()).unwrap()
                        }
                        BuilderStep::FinishRow => req.builder.finish_row().unwrap(),
                        BuilderStep::FinishRows => req.builder.finish_rows().unwrap(),
                        BuilderStep::Finnalize { is_txn, frame_no } => {
                            let _ = req.builder.finnalize(is_txn, frame_no).unwrap();
                            finnalized = true;
                        }
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
        let mut req = self.conn.writer().current_req.lock();
        let should_abort_query = match &mut *req {
            Some(ref mut req) => {
                ready!(req.timeout.as_mut().poll(cx));
                // the request has timedout, we finalize the builder with a error, and clean the
                // current request.
                req.builder.finnalize_error("request timed out".to_string());
                true
            }
            None => return Poll::Ready(()),
        };

        if should_abort_query {
            *req = None
        }

        Poll::Ready(())
    }

    async fn handle_conn_message(&mut self, msg: ConnectionMessage) {
        match msg {
            ConnectionMessage::Execute { pgm, builder } => {
                self.conn.execute_program(&pgm, builder).unwrap();
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
            ConnectionMessage::Describe => (),
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
