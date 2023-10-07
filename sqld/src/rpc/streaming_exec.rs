use std::sync::Arc;

use futures_core::Stream;
use futures_option::OptionExt;
use prost::Message;
use rusqlite::types::ValueRef;
use tokio::pin;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{Code, Status};

use crate::auth::Authenticated;
use crate::connection::Connection;
use crate::database::PrimaryConnection;
use crate::query_analysis::TxnStatus;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;
use crate::rpc::proxy::rpc::exec_req::Request;
use crate::rpc::proxy::rpc::exec_resp::{self, Response};

use super::proxy::rpc::resp_step::Step;
use super::proxy::rpc::{self, ExecReq, ExecResp, ProgramResp, RespStep, RowValue};

pub fn make_proxy_stream<S>(conn: PrimaryConnection, auth: Authenticated, request_stream: S) -> impl Stream<Item = Result<ExecResp, Status>> 
where
    S: Stream<Item = Result<ExecReq, Status>>,
{
    async_stream::stream! {
        let mut current_request_fut = None;
        let (snd, mut recv) = mpsc::channel(1);
        let conn = Arc::new(conn);
        pin!(request_stream);

        loop {
            tokio::select! {
                biased;
                Some(maybe_req) = request_stream.next() => {
                    match maybe_req {
                        Err(e) => {
                            tracing::error!("stream error: {e}");
                            break
                        }
                        Ok(req) => {
                            let request_id = req.request_id;
                            match req.request {
                                Some(Request::Execute(pgm)) => {
                                    let Ok(pgm) =
                                        crate::connection::program::Program::try_from(pgm.pgm.unwrap()) else {
                                            yield Err(Status::new(Code::InvalidArgument, "invalid program"));
                                            break
                                        };
                                    let conn = conn.clone();
                                    let auth = auth.clone();
                                    let sender = snd.clone();

                                    let fut = async move {
                                        let builder = StreamResponseBuilder {
                                            request_id,
                                            sender,
                                            current: None,
                                            current_size: 0,
                                        };

                                        let ret = conn.execute_program(pgm, auth, builder, None).await;
                                        (ret, request_id)
                                    };

                                    current_request_fut.replace(Box::pin(fut));
                                }
                                Some(Request::Describe(_)) => todo!(),
                                None => {
                                    yield Err(Status::new(Code::InvalidArgument, "invalid request"));
                                    break
                                }
                            }
                        }
                    }
                },
                Some(res) = recv.recv() => {
                    yield Ok(res);
                },
                (ret, request_id) = current_request_fut.current(), if current_request_fut.is_some() => {
                    if let Err(e) = ret {
                        yield Ok(ExecResp { request_id, response: Some(Response::Error(e.into())) })
                    }
                },
                else => break,
            }
        }
    }
}

struct StreamResponseBuilder {
    request_id: u32,
    sender: mpsc::Sender<ExecResp>,
    current: Option<ProgramResp>,
    current_size: usize,
}

impl StreamResponseBuilder {
    fn current(&mut self) -> &mut ProgramResp {
        self.current
            .get_or_insert_with(|| ProgramResp { steps: Vec::new() })
    }

    fn push(&mut self, step: Step) -> Result<(), QueryResultBuilderError> {
        const MAX_RESPONSE_SIZE: usize = bytesize::ByteSize::mb(1).as_u64() as usize;

        let current = self.current();
        let step = RespStep { step: Some(step) };
        let size = step.encoded_len();
        current.steps.push(step);
        self.current_size += size;

        if self.current_size >= MAX_RESPONSE_SIZE {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryResultBuilderError> {
        if let Some(current) = self.current.take() {
            let resp = ExecResp {
                request_id: self.request_id,
                response: Some(exec_resp::Response::ProgramResp(current)),
            };
            self.current_size = 0;
            self.sender
                .blocking_send(resp)
                .map_err(|_| QueryResultBuilderError::Internal(anyhow::anyhow!("stream closed")))?;
        }

        Ok(())
    }
}

impl QueryResultBuilder for StreamResponseBuilder {
    type Ret = ();

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.push(Step::Init(rpc::Init {}))?;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginStep(rpc::BeginStep {}))?;
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishStep(rpc::FinishStep {
            affected_row_count,
            last_insert_rowid,
        }))?;
        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.push(Step::StepError(rpc::StepError {
            error: Some(error.into()),
        }))?;
        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::ColsDescription(rpc::ColsDescription {
            columns: cols
                .into_iter()
                .map(Into::into)
                .map(|c| rpc::Column {
                    name: c.name.into(),
                    decltype: c.decl_ty.map(Into::into),
                })
                .collect::<Vec<_>>(),
        }))?;
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginRows(rpc::BeginRows {}))?;
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginRow(rpc::BeginRow {}))?;
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.push(Step::AddRowValue(rpc::AddRowValue {
            val: Some(v.into()),
        }))?;
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishRow(rpc::FinishRow {}))?;
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishRows(rpc::FinishRows {}))?;
        Ok(())
    }

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        state: TxnStatus,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::Finish(rpc::Finish {
            last_frame_no,
            state: rpc::State::from(state).into(),
        }))?;
        self.flush()?;
        Ok(())
    }

    fn into_ret(self) -> Self::Ret { }
}

impl From<ValueRef<'_>> for RowValue {
    fn from(value: ValueRef<'_>) -> Self {
        use rpc::row_value::Value;

        let value = Some(match value {
            ValueRef::Null => Value::Null(true),
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(x) => Value::Real(x),
            ValueRef::Text(s) => Value::Text(String::from_utf8(s.to_vec()).unwrap()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        });

        RowValue { value }
    }
}
