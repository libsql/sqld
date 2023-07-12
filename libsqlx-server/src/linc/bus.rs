use std::sync::Arc;

use super::{connection::SendQueue, handler::Handler, Inbound, NodeId, Outbound};

pub struct Bus<H> {
    node_id: NodeId,
    handler: H,
    send_queue: SendQueue,
}

impl<H: Handler> Bus<H> {
    pub fn new(node_id: NodeId, handler: H) -> Self {
        let send_queue = SendQueue::new();
        Self {
            node_id,
            handler,
            send_queue,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn incomming(self: &Arc<Self>, incomming: Inbound) {
        self.handler.handle(self.clone(), incomming);
    }

    pub fn send_queue(&self) -> &SendQueue {
        &self.send_queue
    }
}

#[async_trait::async_trait]
pub trait Dispatch: Send + Sync + 'static {
    async fn dispatch(&self, msg: Outbound);
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
}
