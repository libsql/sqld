use std::fmt;

use bytes::Bytes;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::DatabaseId;

pub type Program = String;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StreamId(#[serde(deserialize_with = "non_zero")] i32);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn non_zero<'de, D>(d: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = i32::deserialize(d)?;

    if value == 0 {
        return Err(D::Error::custom("invalid stream_id"));
    }

    Ok(value)
}

impl StreamId {
    /// creates a new stream_id.
    /// panics if val is zero.
    pub fn new(val: i32) -> Self {
        assert!(val != 0);
        Self(val)
    }

    pub fn is_positive(&self) -> bool {
        self.0.is_positive()
    }

    #[cfg(test)]
    pub fn new_unchecked(i: i32) -> Self {
        Self(i)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Message {
    /// Messages destined to a node
    Node(NodeMessage),
    /// message destined to a database
    Stream {
        stream_id: StreamId,
        payload: StreamMessage,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeMessage {
    /// Initial message exchanged between nodes when connecting
    Handshake {
        protocol_version: u32,
        node_id: Uuid,
    },
    /// Request to open a bi-directional stream between the client and the server
    OpenStream {
        /// Id to give to the newly opened stream
        /// Initiator of the connection create streams with positive ids,
        /// and acceptor of the connection create streams with negative ids.
        stream_id: StreamId,
        /// Id of the database to open the stream to.
        database_id: Uuid,
    },
    /// Close a previously opened stream
    CloseStream { stream_id: StreamId },
    /// Error type returned while handling a node message
    Error(NodeError),
}

#[derive(Debug, Serialize, Deserialize, thiserror::Error, PartialEq, Eq)]
pub enum NodeError {
    /// The requested stream does not exist
    #[error("unknown stream: {0}")]
    UnknownStream(StreamId),
    /// Incompatible protocol versions
    #[error("invalid protocol version, expected: {expected}")]
    HandshakeVersionMismatch { expected: u32 },
    #[error("stream {0} already exists")]
    StreamAlreadyExist(StreamId),
    #[error("cannot open stream {1}: unknown database {0}")]
    UnknownDatabase(DatabaseId, StreamId),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum StreamMessage {
    /// Replication message between a replica and a primary
    Replication(ReplicationMessage),
    /// Proxy message between a replica and a primary
    Proxy(ProxyMessage),
    #[cfg(test)]
    Dummy,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ReplicationMessage {
    HandshakeResponse {
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
    /// Error occurred handling a replication message
    Error(ReplicationError),
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
    /// Error returned when handling a proxied query message.
    Error(ProxyError),
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

/// TBD
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ProxyError {}

/// TBD
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ReplicationError {}
