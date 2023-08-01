use anyhow::{anyhow, bail, Result};
use sqld_libsql_bindings::ffi;
use std::collections::HashMap;

use super::result_builder::SingleStatementBuilder;
use super::{proto, ProtocolError, Version};
use crate::auth::Authenticated;
use crate::database::{Database, DescribeResponse};
use crate::error::Error as SqldError;
use crate::hrana;
use crate::query::{Params, Query, Value};
use crate::query_analysis::Statement;
use crate::query_result_builder::{QueryResultBuilder, QueryResultBuilderError};

/// An error during execution of an SQL statement.
#[derive(thiserror::Error, Debug)]
pub enum StmtError {
    #[error("SQL string could not be parsed: {source}")]
    SqlParse { source: anyhow::Error },
    #[error("SQL string does not contain any statement")]
    SqlNoStmt,
    #[error("SQL string contains more than one statement")]
    SqlManyStmts,
    #[error("Arguments do not match SQL parameters: {source}")]
    ArgsInvalid { source: anyhow::Error },
    #[error("Specifying both positional and named arguments is not supported")]
    ArgsBothPositionalAndNamed,

    #[error("Transaction timed out")]
    TransactionTimeout,
    #[error("Server cannot handle additional transactions")]
    TransactionBusy,
    #[error("SQLite error: {message}")]
    SqliteError {
        source: libsql::Error,
        message: String,
    },
    #[error("Operation was blocked{}", .reason.as_ref().map(|msg| format!(": {}", msg)).unwrap_or_default())]
    Blocked { reason: Option<String> },
    #[error("Response is too large")]
    ResponseTooLarge,
}

pub async fn execute_stmt(
    db: &impl Database,
    auth: Authenticated,
    query: Query,
) -> Result<proto::StmtResult> {
    let builder = SingleStatementBuilder::default();
    let (stmt_res, _) = db
        .execute_batch(vec![query], auth, builder)
        .await
        .map_err(catch_stmt_error)?;
    stmt_res.into_ret().map_err(catch_stmt_error)
}

pub async fn describe_stmt(
    db: &impl Database,
    auth: Authenticated,
    sql: String,
) -> Result<proto::DescribeResult> {
    match db.describe(sql, auth).await? {
        Ok(describe_response) => Ok(proto_describe_result_from_describe_response(
            describe_response,
        )),
        Err(sqld_error) => match stmt_error_from_sqld_error(sqld_error) {
            Ok(stmt_error) => bail!(stmt_error),
            Err(sqld_error) => bail!(sqld_error),
        },
    }
}

pub fn proto_stmt_to_query(
    proto_stmt: &proto::Stmt,
    sqls: &HashMap<i32, String>,
    verion: Version,
) -> Result<Query> {
    let sql = proto_sql_to_sql(proto_stmt.sql.as_deref(), proto_stmt.sql_id, sqls, verion)?;

    let mut stmt_iter = Statement::parse(sql);
    let stmt = match stmt_iter.next() {
        Some(Ok(stmt)) => stmt,
        Some(Err(err)) => bail!(StmtError::SqlParse { source: err }),
        None => bail!(StmtError::SqlNoStmt),
    };

    if stmt_iter.next().is_some() {
        bail!(StmtError::SqlManyStmts)
    }

    let params = if proto_stmt.named_args.is_empty() {
        let values = proto_stmt.args.iter().map(proto_value_to_value).collect();
        Params::Positional(values)
    } else if proto_stmt.args.is_empty() {
        let values = proto_stmt
            .named_args
            .iter()
            .map(|arg| (arg.name.clone(), proto_value_to_value(&arg.value)))
            .collect();
        Params::Named(values)
    } else {
        bail!(StmtError::ArgsBothPositionalAndNamed)
    };

    let want_rows = proto_stmt.want_rows.unwrap_or(true);
    Ok(Query {
        stmt,
        params,
        want_rows,
    })
}

pub fn proto_sql_to_sql<'s>(
    proto_sql: Option<&'s str>,
    proto_sql_id: Option<i32>,
    sqls: &'s HashMap<i32, String>,
    verion: Version,
) -> Result<&'s str, ProtocolError> {
    if proto_sql_id.is_some() && verion < Version::Hrana2 {
        return Err(ProtocolError::NotSupported {
            what: "`sql_id`",
            min_version: Version::Hrana2,
        });
    }

    match (proto_sql, proto_sql_id) {
        (Some(sql), None) => Ok(sql),
        (None, Some(sql_id)) => match sqls.get(&sql_id) {
            Some(sql) => Ok(sql),
            None => Err(ProtocolError::SqlNotFound { sql_id }),
        },
        (Some(_), Some(_)) => Err(ProtocolError::SqlIdAndSqlGiven),
        (None, None) => Err(ProtocolError::SqlIdOrSqlNotGiven),
    }
}

fn proto_value_to_value(proto_value: &proto::Value) -> Value {
    match proto_value {
        proto::Value::Null => Value::Null,
        proto::Value::Integer { value } => Value::Integer(*value),
        proto::Value::Float { value } => Value::Real(*value),
        proto::Value::Text { value } => Value::Text(value.as_ref().into()),
        proto::Value::Blob { value } => Value::Blob(value.as_ref().into()),
    }
}

fn proto_value_from_value(value: Value) -> proto::Value {
    match value {
        Value::Null => proto::Value::Null,
        Value::Integer(value) => proto::Value::Integer { value },
        Value::Real(value) => proto::Value::Float { value },
        Value::Text(value) => proto::Value::Text {
            value: value.into(),
        },
        Value::Blob(value) => proto::Value::Blob {
            value: value.into(),
        },
    }
}

fn proto_describe_result_from_describe_response(
    response: DescribeResponse,
) -> proto::DescribeResult {
    proto::DescribeResult {
        params: response
            .params
            .into_iter()
            .map(|p| proto::DescribeParam { name: p.name })
            .collect(),
        cols: response
            .cols
            .into_iter()
            .map(|c| proto::DescribeCol {
                name: c.name,
                decltype: c.decltype,
            })
            .collect(),
        is_explain: response.is_explain,
        is_readonly: response.is_readonly,
    }
}

fn catch_stmt_error(sqld_error: SqldError) -> anyhow::Error {
    match stmt_error_from_sqld_error(sqld_error) {
        Ok(stmt_error) => anyhow!(stmt_error),
        Err(sqld_error) => anyhow!(sqld_error),
    }
}

pub fn stmt_error_from_sqld_error(sqld_error: SqldError) -> Result<StmtError, SqldError> {
    Ok(match sqld_error {
        SqldError::LibSqlInvalidQueryParams(source) => StmtError::ArgsInvalid { source },
        SqldError::LibSqlTxTimeout => StmtError::TransactionTimeout,
        SqldError::LibSqlTxBusy => StmtError::TransactionBusy,
        SqldError::BuilderError(QueryResultBuilderError::ResponseTooLarge(_)) => {
            StmtError::ResponseTooLarge
        }
        SqldError::Blocked(reason) => StmtError::Blocked { reason },
        SqldError::LibSqlError(libsql::Error::LibError(rc)) => StmtError::SqliteError {
            message: libsql::errors::error_from_code(rc),
            source: libsql::Error::LibError(rc),
        },
        sqld_error => return Err(sqld_error),
    })
}

pub fn proto_error_from_stmt_error(error: &StmtError) -> hrana::proto::Error {
    hrana::proto::Error {
        message: error.to_string(),
        code: error.code().into(),
    }
}

impl StmtError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SqlParse { .. } => "SQL_PARSE_ERROR",
            Self::SqlNoStmt => "SQL_NO_STATEMENT",
            Self::SqlManyStmts => "SQL_MANY_STATEMENTS",
            Self::ArgsInvalid { .. } => "ARGS_INVALID",
            Self::ArgsBothPositionalAndNamed => "ARGS_BOTH_POSITIONAL_AND_NAMED",
            Self::TransactionTimeout => "TRANSACTION_TIMEOUT",
            Self::TransactionBusy => "TRANSACTION_BUSY",
            Self::SqliteError { source, .. } => sqlite_error_code(source),
            Self::Blocked { .. } => "BLOCKED",
            Self::ResponseTooLarge => "RESPONSE_TOO_LARGE",
        }
    }
}

fn sqlite_error_code(err: &libsql::Error) -> &'static str {
    match err {
        libsql::Error::LibError(code) => match (*code) as u32 {
            ffi::SQLITE_INTERNAL => "SQLITE_INTERNAL",
            ffi::SQLITE_PERM => "SQLITE_PERM",
            ffi::SQLITE_ABORT => "SQLITE_ABORT",
            ffi::SQLITE_BUSY => "SQLITE_BUSY",
            ffi::SQLITE_LOCKED => "SQLITE_LOCKED",
            ffi::SQLITE_NOMEM => "SQLITE_NOMEM",
            ffi::SQLITE_READONLY => "SQLITE_READONLY",
            ffi::SQLITE_INTERRUPT => "SQLITE_INTERRUPT",
            ffi::SQLITE_IOERR => "SQLITE_IOERR",
            ffi::SQLITE_CORRUPT => "SQLITE_CORRUPT",
            ffi::SQLITE_NOTFOUND => "SQLITE_NOTFOUND",
            ffi::SQLITE_FULL => "SQLITE_FULL",
            ffi::SQLITE_CANTOPEN => "SQLITE_CANTOPEN",
            ffi::SQLITE_PROTOCOL => "SQLITE_PROTOCOL",
            ffi::SQLITE_SCHEMA => "SQLITE_SCHEMA",
            ffi::SQLITE_TOOBIG => "SQLITE_TOOBIG",
            ffi::SQLITE_CONSTRAINT => "SQLITE_CONSTRAINT",
            ffi::SQLITE_MISMATCH => "SQLITE_MISMATCH",
            ffi::SQLITE_MISUSE => "SQLITE_MISUSE",
            ffi::SQLITE_NOLFS => "SQLITE_NOLFS",
            ffi::SQLITE_AUTH => "SQLITE_AUTH",
            ffi::SQLITE_RANGE => "SQLITE_RANGE",
            ffi::SQLITE_NOTADB => "SQLITE_NOTADB",
            _ => "SQLITE_UNKNOWN",
        },
        _ => "SQLITE_UNKNOWN",
    }
}

impl From<&proto::Value> for Value {
    fn from(proto_value: &proto::Value) -> Value {
        proto_value_to_value(proto_value)
    }
}

impl From<Value> for proto::Value {
    fn from(value: Value) -> proto::Value {
        proto_value_from_value(value)
    }
}
