use uuid::Uuid;

use self::proto::Enveloppe;

pub mod bus;
pub mod connection;
pub mod connection_pool;
pub mod net;
pub mod proto;
pub mod server;
pub mod handler;

type NodeId = Uuid;
type DatabaseId = Uuid;

const CURRENT_PROTO_VERSION: u32 = 1;
const MAX_STREAM_MSG: usize = 64;

#[derive(Debug)]
pub struct Inbound {
    /// Id of the node sending the message
    pub from: NodeId,
    /// payload
    pub enveloppe: Enveloppe,
}

#[derive(Debug)]
pub struct Outbound {
    pub to: NodeId,
    pub enveloppe: Enveloppe,
}

