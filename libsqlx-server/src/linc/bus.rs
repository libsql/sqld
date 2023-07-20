use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use super::connection::SendQueue;
use super::handler::Handler;
use super::proto::Enveloppe;
use super::{Inbound, NodeId, Outbound};

pub struct Bus<H> {
    node_id: NodeId,
    handler: H,
    peers: RwLock<HashSet<NodeId>>,
    send_queue: SendQueue,
}

impl<H: Handler> Bus<H> {
    pub fn new(node_id: NodeId, handler: H) -> Self {
        let send_queue = SendQueue::new();
        Self {
            node_id,
            handler,
            send_queue,
            peers: Default::default(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub async fn incomming(self: &Arc<Self>, incomming: Inbound) {
        self.handler.handle(self.clone(), incomming).await;
    }

    pub fn send_queue(&self) -> &SendQueue {
        &self.send_queue
    }

    pub fn connect(&self, node_id: NodeId) -> mpsc::UnboundedReceiver<Enveloppe> {
        // TODO: handle peer already exists
        self.peers.write().insert(node_id);
        self.send_queue.register(node_id)
    }

    pub fn disconnect(&self, node_id: NodeId) {
        self.peers.write().remove(&node_id);
    }
}

#[async_trait::async_trait]
pub trait Dispatch: Send + Sync + 'static {
    async fn dispatch(&self, msg: Outbound);
    fn node_id(&self) -> NodeId;
}

#[async_trait::async_trait]
impl<H: Handler> Dispatch for Bus<H> {
    async fn dispatch(&self, msg: Outbound) {
        assert!(
            msg.to != self.node_id(),
            "trying to send a message to ourself!"
        );
        // This message is outbound.
        self.send_queue.enqueue(msg).await;
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }
}
