use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use tokio::task::JoinSet;
use tokio::time::Duration;

use super::connection::Connection;
use super::handler::Handler;
use super::net::Connector;
use super::{bus::Bus, NodeId};

/// Manages a pool of connections to other peers, handling re-connection.
pub struct ConnectionPool<H> {
    managed_peers: HashMap<NodeId, String>,
    connections: JoinSet<NodeId>,
    bus: Arc<Bus<H>>,
}

impl<H: Handler> ConnectionPool<H> {
    pub fn new(
        bus: Arc<Bus<H>>,
        managed_peers: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Self {
        Self {
            managed_peers: managed_peers.into_iter().filter(|(id, _)| *id < bus.node_id()).collect(),
            connections: JoinSet::new(),
            bus,
        }
    }

    pub fn managed_count(&self) -> usize {
        self.managed_peers.len()
    }

    pub async fn run<C: Connector>(mut self) -> color_eyre::Result<()> {
        self.init::<C>().await;

        while self.tick::<C>().await {}

        Ok(())
    }

    pub async fn tick<C: Connector>(&mut self) -> bool {
        if let Some(maybe_to_restart) = self.connections.join_next().await {
            if let Ok(to_restart) = maybe_to_restart {
                self.connect::<C>(to_restart);
            }
            true
        } else {
            false
        }
    }

    async fn init<C: Connector>(&mut self) {
        let peers = self.managed_peers.keys().copied().collect_vec();
        peers.into_iter().for_each(|p| self.connect::<C>(p));
    }

    fn connect<C: Connector>(&mut self, peer_id: NodeId) {
        let bus = self.bus.clone();
        let peer_addr = self.managed_peers[&peer_id].clone();
        let fut = async move {
            let stream = match C::connect(peer_addr.clone()).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("error connection to peer {peer_id}@{peer_addr}: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    return peer_id;
                }
            };
            let connection = Connection::new_initiator(stream, bus.clone());
            connection.run().await;

            dbg!();
            peer_id
        };

        self.connections.spawn(fut);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::SinkExt;
    use tokio::sync::Notify;
    use tokio_stream::StreamExt;

    use crate::linc::{server::Server, AllocId};

    use super::*;

    #[test]
    fn manage_connections() {
        let mut sim = turmoil::Builder::new().build();
        let database_id = AllocId::new_v4();
        let notify = Arc::new(Notify::new());

        let expected_msg = crate::linc::proto::StreamMessage::Proxy(
            crate::linc::proto::ProxyMessage::ProxyRequest {
                connection_id: 42,
                req_id: 42,
                program: "foobar".into(),
            },
        );

        let spawn_host = |node_id| {
            let notify = notify.clone();
            let expected_msg = expected_msg.clone();
            move || {
                let notify = notify.clone();
                let expected_msg = expected_msg.clone();
                async move {
                    let bus = Bus::new(node_id);
                    let mut sub = bus.subscribe(database_id).unwrap();
                    let mut server = Server::new(bus.clone());
                    let mut listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                        .await
                        .unwrap();

                    let mut has_closed = false;
                    let mut streams = Vec::new();
                    loop {
                        tokio::select! {
                            _ = notify.notified() => {
                                if !has_closed {
                                    streams.clear();
                                    server.close_connections().await;
                                    has_closed = true;
                                } else {
                                    break;
                                }
                            },
                            _ = server.tick(&mut listener) => (),
                            Some(mut stream) = sub.next() => {
                                stream
                                    .send(expected_msg.clone())
                                    .await
                                    .unwrap();
                                streams.push(stream);
                            }
                        }
                    }

                    Ok(())
                }
            }
        };

        let host1_id = NodeId::new_v4();
        sim.host("host1", spawn_host(host1_id));

        let host2_id = NodeId::new_v4();
        sim.host("host2", spawn_host(host2_id));

        let host3_id = NodeId::new_v4();
        sim.host("host3", spawn_host(host3_id));

        sim.client("client", async move {
            let bus = Bus::new(NodeId::new_v4());
            let pool = ConnectionPool::new(
                bus.clone(),
                vec![
                    (host1_id, "host1:1234".into()),
                    (host2_id, "host2:1234".into()),
                    (host3_id, "host3:1234".into()),
                ],
            );

            tokio::task::spawn_local(pool.run::<turmoil::net::TcpStream>());

            // all three hosts are reachable:
            let mut stream1 = bus.new_stream(host1_id, database_id).await.unwrap();
            let m = stream1.next().await.unwrap();
            assert_eq!(m, expected_msg);

            let mut stream2 = bus.new_stream(host2_id, database_id).await.unwrap();
            let m = stream2.next().await.unwrap();
            assert_eq!(m, expected_msg);

            let mut stream3 = bus.new_stream(host3_id, database_id).await.unwrap();
            let m = stream3.next().await.unwrap();
            assert_eq!(m, expected_msg);

            // sever connections
            notify.notify_waiters();

            assert!(stream1.next().await.is_none());
            assert!(stream2.next().await.is_none());
            assert!(stream3.next().await.is_none());

            let mut stream1 = bus.new_stream(host1_id, database_id).await.unwrap();
            let m = stream1.next().await.unwrap();
            assert_eq!(m, expected_msg);

            let mut stream2 = bus.new_stream(host2_id, database_id).await.unwrap();
            let m = stream2.next().await.unwrap();
            assert_eq!(m, expected_msg);

            let mut stream3 = bus.new_stream(host3_id, database_id).await.unwrap();
            let m = stream3.next().await.unwrap();
            assert_eq!(m, expected_msg);

            // terminate test
            notify.notify_waiters();

            Ok(())
        });

        sim.run().unwrap();
    }
}
