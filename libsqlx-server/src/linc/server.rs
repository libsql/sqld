use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;

use crate::linc::connection::Connection;

use super::bus::Bus;
use super::handler::Handler;

pub struct Server<H> {
    /// reference to the bus
    bus: Arc<Bus<H>>,
    /// Connection tasks owned by the server
    connections: JoinSet<color_eyre::Result<()>>,
}

impl<H: Handler> Server<H> {
    pub fn new(bus: Arc<Bus<H>>) -> Self {
        Self {
            bus,
            connections: JoinSet::new(),
        }
    }

    /// Close all connections
    #[cfg(test)]
    pub async fn close_connections(&mut self) {
        self.connections.abort_all();
        while self.connections.join_next().await.is_some() {}
    }

    pub async fn run<L>(mut self, mut listener: L) -> color_eyre::Result<()>
    where
        L: super::net::Listener,
    {
        tracing::info!("Cluster server listening on {}", listener.local_addr()?);
        while self.tick(&mut listener).await {}

        Ok(())
    }

    pub async fn tick<L>(&mut self, listener: &mut L) -> bool
    where
        L: super::net::Listener,
    {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                self.make_connection(stream).await;
                true
            }
            Err(e) => {
                tracing::error!("error creating connection: {e}");
                false
            }
        }
    }

    async fn make_connection<S>(&mut self, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let bus = self.bus.clone();
        let fut = async move {
            let connection = Connection::new_acceptor(stream, bus);
            connection.run().await;
            Ok(())
        };

        self.connections.spawn(fut);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    use turmoil::net::TcpStream;

    #[test]
    fn server_respond_to_handshake() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = 0;
        let notify = Arc::new(tokio::sync::Notify::new());
        sim.host("host", move || {
            let notify = notify.clone();
            async move {
                let bus = Arc::new(Bus::new(host_node_id, |_, _| async {}));
                let mut server = Server::new(bus);
                let mut listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                    .await
                    .unwrap();
                server.tick(&mut listener).await;
                notify.notified().await;

                Ok(())
            }
        });

        sim.client("client", async move {
            let node_id = 1;
            let mut c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                Arc::new(Bus::new(node_id, |_, _| async {})),
            );

            c.tick().await;
            c.tick().await;

            assert_eq!(c.peer, Some(host_node_id));

            Ok(())
        });

        sim.run().unwrap();
    }
}
