use std::collections::HashMap;

use libsqlx::analysis::Statement;
use libsqlx::query::{Params, Query, Value};

use super::error::{HranaError, StmtError, ProtocolError};
use super::result_builder::SingleStatementBuilder;
use super::{proto, Version};
use crate::allocation::ConnectionHandle;
use crate::hrana;

pub async fn execute_stmt(
    conn: &ConnectionHandle,
    query: Query,
) -> crate::Result<proto::StmtResult, HranaError> {
    let (builder, ret) = SingleStatementBuilder::new();
    let pgm = libsqlx::program::Program::from_queries(std::iter::once(query));
    conn.execute(pgm, Box::new(builder)).await;
    ret.await
        .unwrap()
        .map_err(|sqld_error| { 
            match stmt_error_from_sqld_error(sqld_error) {
                Ok(e) => e.into(),
                Err(e) => e.into(),
            }
    })
}

pub async fn describe_stmt(
    _db: &ConnectionHandle,
    _sql: String,
) -> crate::Result<proto::DescribeResult, HranaError> {
    todo!();
    // match db.describe(sql).await? {
    //     Ok(describe_response) => todo!(),
    //     //     Ok(proto_describe_result_from_describe_response(
    //     //     describe_response,
    //     // )),
    //     Err(sqld_error) => match stmt_error_from_sqld_error(sqld_error) {
    //         Ok(stmt_error) => bail!(stmt_error),
    //         Err(sqld_error) => bail!(sqld_error),
    //     },
    // }
}

pub fn proto_stmt_to_query(
    proto_stmt: &proto::Stmt,
    sqls: &HashMap<i32, String>,
    version: Version,
) -> crate::Result<Query, HranaError> {
    let sql = proto_sql_to_sql(proto_stmt.sql.as_deref(), proto_stmt.sql_id, sqls, version)?;

    let mut stmt_iter = Statement::parse(sql);
    let stmt = match stmt_iter.next() {
        Some(Ok(stmt)) => stmt,
        Some(Err(err)) => Err(StmtError::SqlParse { source: err.into() })?,
        None => Err(StmtError::SqlNoStmt)?,
    };

    if stmt_iter.next().is_some() {
        Err(StmtError::SqlManyStmts)?
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
        Err(StmtError::ArgsBothPositionalAndNamed)?
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
) -> crate::Result<&'s str, ProtocolError> {
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

// fn proto_describe_result_from_describe_response(
//     response: DescribeResponse,
// ) -> proto::DescribeResult {
//     proto::DescribeResult {
//         params: response
//             .params
//             .into_iter()
//             .map(|p| proto::DescribeParam { name: p.name })
//             .collect(),
//         cols: response
//             .cols
//             .into_iter()
//             .map(|c| proto::DescribeCol {
//                 name: c.name,
//                 decltype: c.decltype,
//             })
//             .collect(),
//         is_explain: response.is_explain,
//         is_readonly: response.is_readonly,
//     }
// }

pub fn stmt_error_from_sqld_error(
    sqld_error: libsqlx::error::Error,
) -> Result<StmtError, libsqlx::error::Error> {
    Ok(match sqld_error {
        libsqlx::error::Error::LibSqlInvalidQueryParams(msg) => StmtError::ArgsInvalid { msg },
        libsqlx::error::Error::LibSqlTxTimeout => StmtError::TransactionTimeout,
        libsqlx::error::Error::LibSqlTxBusy => StmtError::TransactionBusy,
        libsqlx::error::Error::Blocked(reason) => StmtError::Blocked { reason },
        libsqlx::error::Error::RusqliteError(rusqlite_error) => match rusqlite_error {
            libsqlx::error::RusqliteError::SqliteFailure(sqlite_error, Some(message)) => {
                StmtError::SqliteError {
                    source: sqlite_error,
                    message,
                }
            }
            libsqlx::error::RusqliteError::SqliteFailure(sqlite_error, None) => {
                StmtError::SqliteError {
                    message: sqlite_error.to_string(),
                    source: sqlite_error,
                }
            }
            libsqlx::error::RusqliteError::SqlInputError {
                error: sqlite_error,
                msg: message,
                offset,
                ..
            } => StmtError::SqlInputError {
                source: sqlite_error.into(),
                message,
                offset,
            },
            rusqlite_error => return Err(libsqlx::error::Error::RusqliteError(rusqlite_error)),
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
