use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::meta::DatabaseId;

use super::NodeId;

pub type Program = String;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Enveloppe {
    pub database_id: Option<DatabaseId>,
    pub message: Message,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// a batch of frames to inject
pub struct Frames{
    /// must match the Replicate request id
    pub req_id: u32,
    /// sequence id, monotonically incremented, reset when req_id changes.
    /// Used to detect gaps in received frames.
    pub seq: u32,
    pub frames: Vec<Bytes>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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
        req_id: u32,
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
    /// Response to a proxied query
    ProxyResponse {
        /// id of the request this message is a response to.
        req_id: u32,
        /// Collection of steps to drive the query builder transducer.
        row_step: Vec<BuilderStep>,
    },
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ReplicationMessage {
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
        /// next frame no to send
        next_frame_no: u64,
    },
    /// a batch of frames that are part of the same transaction
    Transaction {
        /// if not None, then the last frame is a commit frame, and this is the new size of the database.
        size_after: Option<u32>,
        /// frame_no of the last frame in frames
        end_frame_no: u64,
        /// a batch of frames part of the transaction.
        frames: Vec<Frame>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Frame {
    /// Page id of that frame
    page_id: u32,
    /// Data
    data: Bytes,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ProxyMessage {
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
    /// Response to a proxied query
    ProxyResponse {
        /// id of the request this message is a response to.
        req_id: u32,
        /// Collection of steps to drive the query builder transducer.
        row_step: Vec<BuilderStep>,
    },
    /// Stop processing request `id`.
    CancelRequest { req_id: u32 },
    /// Close Connection with passed id.
    CloseConnection { connection_id: u32 },
}

/// Steps applied to the query builder transducer to build a response to a proxied query.
/// Those types closely mirror those of the `QueryBuilderTrait`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum BuilderStep {
    BeginStep,
    FinishStep(u64, Option<u64>),
    StepError(StepError),
    ColsDesc(Vec<Column>),
    BeginRows,
    BeginRow,
    AddRowValue(Value),
    FinishRow,
    FinishRos,
    Finish(ConnectionState),
}

// State of the connection after a query was executed
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ConnectionState {
    /// The connection is still in a open transaction state
    OpenTxn,
    /// The connection is idle.
    Idle,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum Value {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Column {
    /// name of the column
    name: String,
    /// Declared type of the column, if any.
    decl_ty: Option<String>,
}

/// for now, the stringified version of a sqld::error::Error.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StepError(String);
