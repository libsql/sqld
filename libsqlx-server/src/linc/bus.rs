use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use color_eyre::eyre::{anyhow, bail};
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

use super::connection::{ConnectionHandle, Stream};

type NodeId = Uuid;
type DatabaseId = Uuid;

#[must_use]
pub struct Subscription {
    receiver: mpsc::Receiver<Stream>,
    bus: Bus,
    database_id: DatabaseId,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus
            .inner
            .lock()
            .subscriptions
            .remove(&self.database_id);
    }
}

impl futures::Stream for Subscription {
    type Item = Stream;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

#[derive(Clone)]
pub struct Bus {
    inner: Arc<Mutex<BusInner>>,
    pub node_id: NodeId,
}

enum ConnectionSlot {
    Handle(ConnectionHandle),
    // Interest in the connection when it becomes available
    Interest(Arc<Notify>),
}

struct BusInner {
    connections: HashMap<NodeId, ConnectionSlot>,
    subscriptions: HashMap<DatabaseId, mpsc::Sender<Stream>>,
}

impl Bus {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            inner: Arc::new(Mutex::new(BusInner {
                connections: HashMap::new(),
                subscriptions: HashMap::new(),
            })),
        }
    }

    /// open a new stream to the database at `database_id` on the node `node_id`
    pub async fn new_stream(
        &self,
        node_id: NodeId,
        database_id: DatabaseId,
    ) -> color_eyre::Result<Stream> {
        let get_conn = || {
            let mut lock = self.inner.lock();
            match lock.connections.entry(node_id) {
                Entry::Occupied(mut e) => match e.get_mut() {
                    ConnectionSlot::Handle(h) => Ok(h.clone()),
                    ConnectionSlot::Interest(notify) => Err(notify.clone()),
                },
                Entry::Vacant(e) => {
                    let notify = Arc::new(Notify::new());
                    e.insert(ConnectionSlot::Interest(notify.clone()));
                    Err(notify)
                }
            }
        };

        let conn = match get_conn() {
            Ok(conn) => conn,
            Err(notify) => {
                notify.notified().await;
                get_conn().map_err(|_| anyhow!("failed to create stream"))?
            }
        };

        conn.new_stream(database_id).await
    }

    /// Notify a subscription that new stream was openned
    pub async fn notify_subscription(
        &mut self,
        database_id: DatabaseId,
        stream: Stream,
    ) -> color_eyre::Result<()> {
        let maybe_sender = self.inner.lock().subscriptions.get(&database_id).cloned();

        match maybe_sender {
            Some(sender) => {
                if sender.send(stream).await.is_err() {
                    bail!("subscription for {database_id} closed");
                }

                Ok(())
            }
            None => {
                bail!("no subscription for {database_id}")
            }
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().connections.is_empty()
    }

    #[must_use]
    pub fn register_connection(&self, node_id: NodeId, conn: ConnectionHandle) -> Registration {
        let mut lock = self.inner.lock();
        match lock.connections.entry(node_id) {
            Entry::Occupied(mut e) => {
                if let ConnectionSlot::Interest(ref notify) = e.get() {
                    notify.notify_waiters();
                }

                *e.get_mut() = ConnectionSlot::Handle(conn);
            }
            Entry::Vacant(e) => {
                e.insert(ConnectionSlot::Handle(conn));
            }
        }

        Registration {
            bus: self.clone(),
            node_id,
        }
    }

    pub fn subscribe(&self, database_id: DatabaseId) -> color_eyre::Result<Subscription> {
        let (sender, receiver) = mpsc::channel(1);
        {
            let mut inner = self.inner.lock();

            if inner.subscriptions.contains_key(&database_id) {
                bail!("a subscription already exist for that database");
            }

            inner.subscriptions.insert(database_id, sender);
        }

        Ok(Subscription {
            receiver,
            bus: self.clone(),
            database_id,
        })
    }
}

pub struct Registration {
    bus: Bus,
    node_id: NodeId,
}

impl Drop for Registration {
    fn drop(&mut self) {
        assert!(self
            .bus
            .inner
            .lock()
            .connections
            .remove(&self.node_id)
            .is_some());
    }
}
