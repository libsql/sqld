use super::Version;

#[derive(thiserror::Error, Debug)]
pub enum HranaError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Stmt(#[from] StmtError),
    #[error(transparent)]
    Internal(color_eyre::eyre::Error),
    #[error("Statement in sequence was not executed")]
    StatementSkipped,
    #[error(transparent)]
    Libsqlx(#[from] libsqlx::error::Error),
    #[error(transparent)]
    StreamResponse(#[from] StreamResponseError),
    #[error(transparent)]
    Stream(#[from] StreamError)
}

/// An error from executing a [`proto::StreamRequest`]
#[derive(thiserror::Error, Debug)]
pub enum StreamResponseError {
    #[error("The server already stores {count} SQL texts, it cannot store more")]
    SqlTooMany { count: usize },
    #[error(transparent)]
    Stmt(StmtError),
}


/// An unrecoverable protocol error that should close the WebSocket or HTTP stream. A correct
/// client should never trigger any of these errors.
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("Cannot deserialize client message: {source}")]
    Deserialize { source: serde_json::Error },
    #[error("Received a binary WebSocket message, which is not supported")]
    BinaryWebSocketMessage,
    #[error("Received a request before hello message")]
    RequestBeforeHello,

    #[error("Stream {stream_id} not found")]
    StreamNotFound { stream_id: i32 },
    #[error("Stream {stream_id} already exists")]
    StreamExists { stream_id: i32 },

    #[error("Either `sql` or `sql_id` are required, but not both")]
    SqlIdAndSqlGiven,
    #[error("Either `sql` or `sql_id` are required")]
    SqlIdOrSqlNotGiven,
    #[error("SQL text {sql_id} not found")]
    SqlNotFound { sql_id: i32 },
    #[error("SQL text {sql_id} already exists")]
    SqlExists { sql_id: i32 },

    #[error("Invalid reference to step in a batch condition")]
    BatchCondBadStep,

    #[error("Received an invalid baton: {0}")]
    BatonInvalid(String),
    #[error("Received a baton that has already been used")]
    BatonReused,
    #[error("Stream for this baton was closed")]
    BatonStreamClosed,

    #[error("{what} is only supported in protocol version {min_version} and higher")]
    NotSupported {
        what: &'static str,
        min_version: Version,
    },

    #[error("{0}")]
    ResponseTooLarge(String),
}

/// An error during execution of an SQL statement.
#[derive(thiserror::Error, Debug)]
pub enum StmtError {
    #[error("SQL string could not be parsed: {source}")]
    SqlParse { source: color_eyre::eyre::Error },
    #[error("SQL string does not contain any statement")]
    SqlNoStmt,
    #[error("SQL string contains more than one statement")]
    SqlManyStmts,
    #[error("Arguments do not match SQL parameters: {msg}")]
    ArgsInvalid { msg: String },
    #[error("Specifying both positional and named arguments is not supported")]
    ArgsBothPositionalAndNamed,

    #[error("Transaction timed out")]
    TransactionTimeout,
    #[error("Server cannot handle additional transactions")]
    TransactionBusy,
    #[error("SQLite error: {message}")]
    SqliteError {
        source: libsqlx::rusqlite::ffi::Error,
        message: String,
    },
    #[error("SQL input error: {message} (at offset {offset})")]
    SqlInputError {
        source: color_eyre::eyre::Error,
        message: String,
        offset: i32,
    },

    #[error("Operation was blocked{}", .reason.as_ref().map(|msg| format!(": {}", msg)).unwrap_or_default())]
    Blocked { reason: Option<String> },
    #[error("query error: {0}")]
    QueryError(String)
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
            Self::SqliteError { source, .. } => sqlite_error_code(source.code),
            Self::SqlInputError { .. } => "SQL_INPUT_ERROR",
            Self::Blocked { .. } => "BLOCKED",
            Self::QueryError(_) => todo!(),
        }
    }
}

fn sqlite_error_code(code: libsqlx::error::ErrorCode) -> &'static str {
    match code {
        libsqlx::error::ErrorCode::InternalMalfunction => "SQLITE_INTERNAL",
        libsqlx::error::ErrorCode::PermissionDenied => "SQLITE_PERM",
        libsqlx::error::ErrorCode::OperationAborted => "SQLITE_ABORT",
        libsqlx::error::ErrorCode::DatabaseBusy => "SQLITE_BUSY",
        libsqlx::error::ErrorCode::DatabaseLocked => "SQLITE_LOCKED",
        libsqlx::error::ErrorCode::OutOfMemory => "SQLITE_NOMEM",
        libsqlx::error::ErrorCode::ReadOnly => "SQLITE_READONLY",
        libsqlx::error::ErrorCode::OperationInterrupted => "SQLITE_INTERRUPT",
        libsqlx::error::ErrorCode::SystemIoFailure => "SQLITE_IOERR",
        libsqlx::error::ErrorCode::DatabaseCorrupt => "SQLITE_CORRUPT",
        libsqlx::error::ErrorCode::NotFound => "SQLITE_NOTFOUND",
        libsqlx::error::ErrorCode::DiskFull => "SQLITE_FULL",
        libsqlx::error::ErrorCode::CannotOpen => "SQLITE_CANTOPEN",
        libsqlx::error::ErrorCode::FileLockingProtocolFailed => "SQLITE_PROTOCOL",
        libsqlx::error::ErrorCode::SchemaChanged => "SQLITE_SCHEMA",
        libsqlx::error::ErrorCode::TooBig => "SQLITE_TOOBIG",
        libsqlx::error::ErrorCode::ConstraintViolation => "SQLITE_CONSTRAINT",
        libsqlx::error::ErrorCode::TypeMismatch => "SQLITE_MISMATCH",
        libsqlx::error::ErrorCode::ApiMisuse => "SQLITE_MISUSE",
        libsqlx::error::ErrorCode::NoLargeFileSupport => "SQLITE_NOLFS",
        libsqlx::error::ErrorCode::AuthorizationForStatementDenied => "SQLITE_AUTH",
        libsqlx::error::ErrorCode::ParameterOutOfRange => "SQLITE_RANGE",
        libsqlx::error::ErrorCode::NotADatabase => "SQLITE_NOTADB",
        libsqlx::error::ErrorCode::Unknown => "SQLITE_UNKNOWN",
        _ => "SQLITE_UNKNOWN",
    }
}

/// An unrecoverable error that should close the stream. The difference from [`ProtocolError`] is
/// that a correct client may trigger this error, it does not mean that the protocol has been
/// violated.
#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("The stream has expired due to inactivity")]
    StreamExpired,
}

