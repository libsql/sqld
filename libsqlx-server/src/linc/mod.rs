use self::proto::Enveloppe;

pub mod bus;
pub mod connection;
pub mod connection_pool;
pub mod handler;
pub mod net;
pub mod proto;
pub mod server;

pub type NodeId = u64;

const CURRENT_PROTO_VERSION: u32 = 1;

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
