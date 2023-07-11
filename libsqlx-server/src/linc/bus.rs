use std::sync::Arc;

use uuid::Uuid;

use super::{connection::SendQueue, handler::Handler, Outbound, Inbound};

type NodeId = Uuid;
type DatabaseId = Uuid;

pub struct Bus<H> {
    inner: Arc<BusInner<H>>,
}

impl<H> Clone for Bus<H> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

struct BusInner<H> {
    node_id: NodeId,
    handler: H,
    send_queue: SendQueue,
}

impl<H: Handler> Bus<H> {
    pub fn new(node_id: NodeId, handler: H) -> Self {
        let send_queue = SendQueue::new();
        Self {
            inner: Arc::new(BusInner {
                node_id,
                handler,
                send_queue,
            }),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub async fn incomming(&self, incomming: Inbound) {
        self.inner.handler.handle(self, incomming);
    }

    pub async fn dispatch(&self, msg: Outbound) {
        assert!(
            msg.to != self.node_id(),
            "trying to send a message to ourself!"
        );
        // This message is outbound.
        self.inner.send_queue.enqueue(msg).await;
    }

    pub fn send_queue(&self) -> &SendQueue {
        &self.inner.send_queue

    }
}
