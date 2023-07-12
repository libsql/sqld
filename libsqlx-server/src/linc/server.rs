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

    pub async fn run<L>(mut self, mut listener: L)
    where
        L: super::net::Listener,
    {
        while self.tick(&mut listener).await {}
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

    use crate::linc::{proto::ProxyMessage, AllocId, NodeId};

    use super::*;

    use futures::{SinkExt, StreamExt};
    use tokio::sync::Notify;
    use turmoil::net::TcpStream;

    #[test]
    fn server_respond_to_handshake() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        let notify = Arc::new(tokio::sync::Notify::new());
        sim.host("host", move || {
            let notify = notify.clone();
            async move {
                let bus = Bus::new(host_node_id);
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
            let node_id = NodeId::new_v4();
            let mut c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                Bus::new(node_id),
            );

            c.tick().await;
            c.tick().await;

            assert_eq!(c.peer, Some(host_node_id));

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn client_create_stream_client_close() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        let stream_db_id = AllocId::new_v4();
        let notify = Arc::new(Notify::new());
        let expected_msg = StreamMessage::Proxy(ProxyMessage::ProxyRequest {
            connection_id: 12,
            req_id: 1,
            program: "hello".to_string(),
        });

        sim.host("host", {
            let notify = notify.clone();
            let expected_msg = expected_msg.clone();
            move || {
                let notify = notify.clone();
                let expected_msg = expected_msg.clone();
                async move {
                    let bus = Bus::new(host_node_id);
                    let server = Server::new(bus.clone());
                    let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                        .await
                        .unwrap();
                    let mut subs = bus.subscribe(stream_db_id).unwrap();
                    tokio::task::spawn_local(server.run(listener));

                    let mut stream = subs.next().await.unwrap();

                    let msg = stream.next().await.unwrap();

                    assert_eq!(msg, expected_msg);

                    notify.notify_waiters();

                    assert!(stream.next().await.is_none());

                    notify.notify_waiters();

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let node_id = NodeId::new_v4();
            let bus = Bus::new(node_id);
            let mut c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                bus.clone(),
            );
            c.tick().await;
            c.tick().await;
            let _h = tokio::spawn(c.run());
            let mut stream = bus.new_stream(host_node_id, stream_db_id).await.unwrap();
            stream.send(expected_msg).await.unwrap();

            notify.notified().await;

            drop(stream);

            notify.notified().await;

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn client_create_stream_server_close() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        let database_id = AllocId::new_v4();
        let notify = Arc::new(Notify::new());

        sim.host("host", {
            let notify = notify.clone();
            move || {
                let notify = notify.clone();
                async move {
                    let bus = Bus::new(host_node_id);
                    let server = Server::new(bus.clone());
                    let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                        .await
                        .unwrap();
                    let mut subs = bus.subscribe(database_id).unwrap();
                    tokio::task::spawn_local(server.run(listener));

                    let stream = subs.next().await.unwrap();
                    drop(stream);

                    notify.notify_waiters();
                    notify.notified().await;

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let node_id = NodeId::new_v4();
            let bus = Bus::new(node_id);
            let mut c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                bus.clone(),
            );
            c.tick().await;
            c.tick().await;
            let _h = tokio::spawn(c.run());
            let mut stream = bus.new_stream(host_node_id, database_id).await.unwrap();

            notify.notified().await;
            assert!(stream.next().await.is_none());
            notify.notify_waiters();

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn server_create_stream_server_close() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        let notify = Arc::new(Notify::new());
        let client_id = NodeId::new_v4();
        let database_id = AllocId::new_v4();
        let expected_msg = StreamMessage::Proxy(ProxyMessage::ProxyRequest {
            connection_id: 12,
            req_id: 1,
            program: "hello".to_string(),
        });

        sim.host("host", {
            let notify = notify.clone();
            let expected_msg = expected_msg.clone();
            move || {
                let notify = notify.clone();
                let expected_msg = expected_msg.clone();
                async move {
                    let bus = Bus::new(host_node_id);
                    let server = Server::new(bus.clone());
                    let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                        .await
                        .unwrap();
                    tokio::task::spawn_local(server.run(listener));

                    let mut stream = bus.new_stream(client_id, database_id).await.unwrap();
                    stream.send(expected_msg).await.unwrap();
                    notify.notified().await;
                    drop(stream);

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let bus = Bus::new(client_id);
            let mut subs = bus.subscribe(database_id).unwrap();
            let c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                bus.clone(),
            );
            let _h = tokio::spawn(c.run());

            let mut stream = subs.next().await.unwrap();
            let msg = stream.next().await.unwrap();
            assert_eq!(msg, expected_msg);
            notify.notify_waiters();
            assert!(stream.next().await.is_none());

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn server_create_stream_client_close() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        let client_id = NodeId::new_v4();
        let database_id = AllocId::new_v4();

        sim.host("host", {
            move || async move {
                let bus = Bus::new(host_node_id);
                let server = Server::new(bus.clone());
                let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                    .await
                    .unwrap();
                tokio::task::spawn_local(server.run(listener));

                let mut stream = bus.new_stream(client_id, database_id).await.unwrap();
                assert!(stream.next().await.is_none());

                Ok(())
            }
        });

        sim.client("client", async move {
            let bus = Bus::new(client_id);
            let mut subs = bus.subscribe(database_id).unwrap();
            let c = Connection::new_initiator(
                TcpStream::connect("host:1234").await.unwrap(),
                bus.clone(),
            );
            let _h = tokio::spawn(c.run());

            let stream = subs.next().await.unwrap();
            drop(stream);

            Ok(())
        });

        sim.run().unwrap();
    }
}
