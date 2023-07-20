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
            managed_peers: managed_peers
                .into_iter()
                .filter(|(id, _)| *id < bus.node_id())
                .collect(),
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

            peer_id
        };

        self.connections.spawn(fut);
    }
}
