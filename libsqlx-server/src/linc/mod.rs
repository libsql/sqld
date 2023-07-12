use self::proto::{Enveloppe, Message};

pub mod bus;
pub mod connection;
pub mod connection_pool;
pub mod handler;
pub mod net;
pub mod proto;
pub mod server;

pub type NodeId = u64;

const CURRENT_PROTO_VERSION: u32 = 1;
const MAX_STREAM_MSG: usize = 64;

#[derive(Debug)]
pub struct Inbound {
    /// Id of the node sending the message
    pub from: NodeId,
    /// payload
    pub enveloppe: Enveloppe,
}

impl Inbound {
    pub fn respond(&self, message: Message) -> Outbound {
        Outbound {
            to: self.from,
            enveloppe: Enveloppe {
                from: self.enveloppe.to,
                to: self.enveloppe.from,
                message,
            },
        }
    }
}

#[derive(Debug)]
pub struct Outbound {
    pub to: NodeId,
    pub enveloppe: Enveloppe,
}
