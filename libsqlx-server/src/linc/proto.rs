use bytes::Bytes;
use libsqlx::{program::Program, FrameNo};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::meta::DatabaseId;

use super::NodeId;

#[derive(Debug, Serialize, Deserialize)]
pub struct Enveloppe {
    pub database_id: Option<DatabaseId>,
    pub message: Message,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// a batch of frames to inject
pub struct Frames {
    /// must match the Replicate request id
    pub req_no: u32,
    /// sequence id, monotonically incremented, reset when req_id changes.
    /// Used to detect gaps in received frames.
    pub seq_no: u32,
    pub frames: Vec<Bytes>,
}

#[derive(Debug, Serialize, Deserialize)]
/// Response to a proxied query
pub struct ProxyResponse {
    pub connection_id: u32,
    /// id of the request this message is a response to.
    pub req_id: u32,
    pub seq_no: u32,
    /// Collection of steps to drive the query builder transducer.
    pub row_steps: Vec<BuilderStep>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// Initial message exchanged between nodes when connecting
    Handshake {
        protocol_version: u32,
        node_id: NodeId,
    },
    ReplicationHandshake {
        database_name: String,
    },
    ReplicationHandshakeResponse {
        /// id of the replication log
        log_id: Uuid,
        /// current frame_no of the primary
        current_frame_no: u64,
    },
    Replicate {
        /// incremental request id, used when responding with a Frames message
        req_no: u32,
        /// next frame no to send
        next_frame_no: u64,
    },
    Frames(Frames),
    /// Proxy a query to a primary
    ProxyRequest {
        /// id of the connection to perform the query against
        /// If the connection doesn't already exist it is created
        /// Id of the request.
        /// Responses to this request must have the same id.
        connection_id: u32,
        req_id: u32,
        program: Program,
    },
    ProxyResponse(ProxyResponse),
    /// Stop processing request `id`.
    CancelRequest {
        req_id: u32,
    },
    /// Close Connection with passed id.
    CloseConnection {
        connection_id: u32,
    },
    Error(ProtoError),
}

#[derive(Debug, Serialize, Deserialize, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    /// Incompatible protocol versions
    #[error("invalid protocol version, expected: {expected}")]
    HandshakeVersionMismatch { expected: u32 },
    #[error("unknown database {0}")]
    UnknownDatabase(String),
}

/// Steps applied to the query builder transducer to build a response to a proxied query.
/// Those types closely mirror those of the `QueryBuilderTrait`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum BuilderStep {
    Init,
    BeginStep,
    FinishStep(u64, Option<i64>),
    StepError(StepError),
    ColsDesc(Vec<Column>),
    BeginRows,
    BeginRow,
    AddRowValue(Value),
    FinishRow,
    FinishRows,
    Finnalize {
        is_txn: bool,
        frame_no: Option<FrameNo>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    // TODO: how to stream blobs/string???
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl<'a> Into<libsqlx::result_builder::ValueRef<'a>> for &'a Value {
    fn into(self) -> libsqlx::result_builder::ValueRef<'a> {
        use libsqlx::result_builder::ValueRef;
        match self {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(*i),
            Value::Real(x) => ValueRef::Real(*x),
            Value::Text(ref t) => ValueRef::Text(t),
            Value::Blob(ref b) => ValueRef::Blob(b),
        }
    }
}

impl From<libsqlx::result_builder::ValueRef<'_>> for Value {
    fn from(value: libsqlx::result_builder::ValueRef) -> Self {
        use libsqlx::result_builder::ValueRef;
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(i) => Self::Integer(i),
            ValueRef::Real(x) => Self::Real(x),
            ValueRef::Text(s) => Self::Text(s.into()),
            ValueRef::Blob(b) => Self::Blob(b.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Column {
    /// name of the column
    pub name: String,
    /// Declared type of the column, if any.
    pub decl_ty: Option<String>,
}

impl From<libsqlx::result_builder::Column<'_>> for Column {
    fn from(value: libsqlx::result_builder::Column) -> Self {
        Self {
            name: value.name.to_string(),
            decl_ty: value.decl_ty.map(ToOwned::to_owned),
        }
    }
}

/// for now, the stringified version of a sqld::error::Error.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StepError(pub String);
