use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures_core::Stream;
use rusqlite::types::ValueRef;
use tokio::sync::mpsc;

use crate::auth::Authenticated;
use crate::connection::Connection;
use crate::database::PrimaryConnection;
use crate::query_analysis::TxnStatus;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;
use crate::rpc::proxy::rpc::{exec_req::Request, Message};

use super::proxy::rpc::{self, message::Payload, ExecReq, ExecResp};

pin_project_lite::pin_project! {
    pub struct StreamRequestHandler<S> {
        #[pin]
        request_stream: S,
        connection: Arc<PrimaryConnection>,
        state: HandlerState,
        authenticated: Authenticated,
    }
}

impl<S> StreamRequestHandler<S> {
    pub fn new(
        request_stream: S,
        connection: PrimaryConnection,
        authenticated: Authenticated,
    ) -> Self {
        Self {
            request_stream,
            connection: connection.into(),
            state: HandlerState::Idle,
            authenticated,
        }
    }
}

struct StreamResponseBuilder {
    request_id: u32,
    sender: mpsc::Sender<ExecResp>,
    current: Option<ExecResp>,
}

impl StreamResponseBuilder {
    fn current(&mut self) -> &mut ExecResp {
        self.current.get_or_insert_with(|| ExecResp {
            messages: Vec::new(),
            request_id: self.request_id,
        })
    }

    fn push(&mut self, payload: Payload) {
        const MAX_RESPONSE_MESSAGES: usize = 10;

        let current = self.current();
        current.messages.push(Message {
            payload: Some(payload),
        });

        if current.messages.len() > MAX_RESPONSE_MESSAGES {
            self.flush()
        }
    }

    fn flush(&mut self) {
        if let Some(current) = self.current.take() {
            self.sender.blocking_send(current).unwrap();
        }
    }
}

impl QueryResultBuilder for StreamResponseBuilder {
    type Ret = ();

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::Init(rpc::Init {}));
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::BeginStep(rpc::BeginStep {}));
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::FinishStep(rpc::FinishStep {
            affected_row_count,
            last_insert_rowid,
        }));
        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::StepError(rpc::StepError {
            error: Some(error.into()),
        }));
        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::ColsDescription(rpc::ColsDescription {
            columns: cols
                .into_iter()
                .map(Into::into)
                .map(|c| rpc::Column {
                    name: c.name.into(),
                    decltype: c.decl_ty.map(Into::into),
                })
                .collect::<Vec<_>>(),
        }));
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::BeginRows(rpc::BeginRows {}));
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::BeginRow(rpc::BeginRow {}));
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        let data = bincode::serialize(
            &crate::query::Value::try_from(v).map_err(QueryResultBuilderError::from_any)?,
        )
        .map_err(QueryResultBuilderError::from_any)?;

        let val = Some(rpc::Value { data });

        self.push(Payload::AddRowValue(rpc::AddRowValue { val }));
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::FinishRow(rpc::FinishRow {}));
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::FinishRows(rpc::FinishRows {}));
        Ok(())
    }

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        state: TxnStatus,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Payload::Finish(rpc::Finish {
            last_frame_no,
            state: rpc::State::from(state).into(),
        }));
        self.flush();
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        ()
    }
}

enum HandlerState {
    Execute(Pin<Box<dyn Stream<Item = ExecResp> + Send>>),
    Idle,
    Fused,
}

impl<S> Stream for StreamRequestHandler<S>
where
    S: Stream<Item = Result<ExecReq, tonic::Status>>,
{
    type Item = Result<ExecResp, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        dbg!();
        match this.state {
            HandlerState::Idle => {
                dbg!();
                match ready!(this.request_stream.poll_next(cx)) {
                    Some(Err(e)) => {
                        dbg!();
                        *this.state = HandlerState::Fused;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Some(Ok(req)) => {
                        dbg!(&req);
                        let request_id = req.request_id;
                        match req.request.unwrap() {
                            Request::Execute(pgm) => {
                                dbg!();
                                let pgm =
                                    crate::connection::program::Program::try_from(pgm).unwrap();
                                let conn = this.connection.clone();
                                let authenticated = this.authenticated.clone();
                                dbg!();

                                let s = async_stream::stream! {
                                    let (sender, mut receiver) = mpsc::channel(1);
                                    let builder = StreamResponseBuilder {
                                        request_id,
                                        sender,
                                        current: None,
                                    };
                                    let mut fut = conn.execute_program(pgm, authenticated, builder, None);
                                    loop {
                                        tokio::select! {
                                            _res = &mut fut => {
                                                dbg!();
                                                if let Err(e) = _res {
                                                    dbg!(e);
                                                }

                                                // drain the receiver
                                                while let Ok(msg) = receiver.try_recv() {
                                                    yield msg;
                                                }
                                                // todo check result?
                                                break
                                            }
                                            msg = receiver.recv() => {
                                                dbg!(&msg);
                                                if let Some(msg) = msg {
                                                    dbg!();
                                                    yield msg;
                                                }
                                            }
                                        }
                                    }
                                };
                                dbg!();
                                *this.state = HandlerState::Execute(Box::pin(s));
                            }
                            Request::Describe(_) => todo!(),
                        }
                        // we have placed the request, poll immediately
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    None => {
                        // this would easier if tokio_stream re-exported combinators
                        *this.state = HandlerState::Fused;
                        Poll::Ready(None)
                    }
                }
            }
            HandlerState::Fused => Poll::Ready(None),
            HandlerState::Execute(stream) => {
                dbg!();
                let resp = ready!(stream.as_mut().poll_next(cx));
                match resp {
                    Some(resp) => return Poll::Ready(Some(Ok(dbg!(resp)))),
                    None => {
                        dbg!();
                        // finished processing this query. Wake up immediately to prepare for the
                        // next
                        *this.state = HandlerState::Idle;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
