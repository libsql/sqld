use crate::hrana::error::{HranaError, ProtocolError, StreamResponseError};

use super::super::{batch, stmt, Version};
use super::{proto, stream};

pub async fn handle(
    stream_guard: &mut stream::Guard,
    request: proto::StreamRequest,
) -> crate::Result<proto::StreamResult, HranaError> {
    let result = match try_handle(stream_guard, request).await {
        Ok(response) => proto::StreamResult::Ok { response },
        Err(err) => {
            if let HranaError::StreamResponse(resp_err) = err {
                let error = proto::Error {
                    message: resp_err.to_string(),
                    code: resp_err.code().into(),
                };
                proto::StreamResult::Error { error }
            } else {
                return Err(err);
            }
        }
    };

    Ok(result)
}

async fn try_handle(
    stream_guard: &mut stream::Guard,
    request: proto::StreamRequest,
) -> crate::Result<proto::StreamResponse, HranaError> {
    Ok(match request {
        proto::StreamRequest::Close(_req) => {
            stream_guard.close_db();
            proto::StreamResponse::Close(proto::CloseStreamResp {})
        }
        proto::StreamRequest::Execute(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let query = stmt::proto_stmt_to_query(&req.stmt, sqls, Version::Hrana2)?;
            let result = stmt::execute_stmt(db, query).await?;
            proto::StreamResponse::Execute(proto::ExecuteStreamResp { result })
        }
        proto::StreamRequest::Batch(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let pgm = batch::proto_batch_to_program(&req.batch, sqls, Version::Hrana2)?;
            let result = batch::execute_batch(db, pgm).await?;
            proto::StreamResponse::Batch(proto::BatchStreamResp { result })
        }
        proto::StreamRequest::Sequence(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql =
                stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, Version::Hrana2)?;
            let pgm = batch::proto_sequence_to_program(sql)?;
            batch::execute_sequence(db, pgm)
                .await?;
            proto::StreamResponse::Sequence(proto::SequenceStreamResp {})
        }
        proto::StreamRequest::Describe(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql =
                stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, Version::Hrana2)?;
            let result = stmt::describe_stmt(db, sql.into())
                .await?;
            proto::StreamResponse::Describe(proto::DescribeStreamResp { result })
        }
        proto::StreamRequest::StoreSql(req) => {
            let sqls = stream_guard.sqls_mut();
            let sql_id = req.sql_id;
            if sqls.contains_key(&sql_id) {
                Err(ProtocolError::SqlExists { sql_id })?
            } else if sqls.len() >= MAX_SQL_COUNT {
                Err(StreamResponseError::SqlTooMany { count: sqls.len() })?
            }
            sqls.insert(sql_id, req.sql);
            proto::StreamResponse::StoreSql(proto::StoreSqlStreamResp {})
        }
        proto::StreamRequest::CloseSql(req) => {
            let sqls = stream_guard.sqls_mut();
            sqls.remove(&req.sql_id);
            proto::StreamResponse::CloseSql(proto::CloseSqlStreamResp {})
        }
    })
}

const MAX_SQL_COUNT: usize = 50;

impl StreamResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SqlTooMany { .. } => "SQL_STORE_TOO_MANY",
            Self::Stmt(err) => err.code(),
        }
    }
}
