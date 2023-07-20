use std::collections::HashMap;
use std::sync::Arc;

use async_bincode::tokio::AsyncBincodeStream;
use async_bincode::AsyncDestination;
use color_eyre::eyre::bail;
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

use crate::linc::proto::ProtoError;
use crate::linc::CURRENT_PROTO_VERSION;

use super::bus::Bus;
use super::handler::Handler;
use super::proto::{Enveloppe, Message};
use super::{Inbound, NodeId, Outbound};

/// A connection to another node. Manage the connection state, and (de)register streams with the
/// `Bus`
pub struct Connection<S, H> {
    /// Id of the current node
    pub peer: Option<NodeId>,
    /// State of the connection
    pub state: ConnectionState,
    /// Sink/Stream for network messages
    conn: AsyncBincodeStream<S, Enveloppe, Enveloppe, AsyncDestination>,
    /// Are we the initiator of this connection?
    is_initiator: bool,
    /// send queue for this connection
    send_queue: Option<mpsc::UnboundedReceiver<Enveloppe>>,
    bus: Arc<Bus<H>>,
}

#[derive(Debug)]
pub enum ConnectionState {
    Init,
    Connecting,
    Connected,
    // Closing the connection with an error
    CloseError(color_eyre::eyre::Error),
    // Graceful connection shutdown
    Close,
}

pub fn handshake_deadline() -> Instant {
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
    Instant::now() + HANDSHAKE_TIMEOUT
}

// TODO: limit send queue depth
pub struct SendQueue {
    senders: RwLock<HashMap<NodeId, mpsc::UnboundedSender<Enveloppe>>>,
}

impl SendQueue {
    pub fn new() -> Self {
        Self {
            senders: Default::default(),
        }
    }

    pub async fn enqueue(&self, msg: Outbound) {
        let sender = match self.senders.read().get(&msg.to) {
            Some(sender) => sender.clone(),
            None => todo!("no queue"),
        };

        sender.send(msg.enveloppe).unwrap();
    }

    pub fn register(&self, node_id: NodeId) -> mpsc::UnboundedReceiver<Enveloppe> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.senders.write().insert(node_id, sender);

        receiver
    }
}

impl<S, H> Connection<S, H>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: Handler,
{
    const MAX_CONNECTION_MESSAGES: usize = 128;

    pub fn new_initiator(stream: S, bus: Arc<Bus<H>>) -> Self {
        Self {
            peer: None,
            state: ConnectionState::Init,
            conn: AsyncBincodeStream::from(stream).for_async(),
            is_initiator: true,
            send_queue: None,
            bus,
        }
    }

    pub fn new_acceptor(stream: S, bus: Arc<Bus<H>>) -> Self {
        Connection {
            peer: None,
            state: ConnectionState::Connecting,
            is_initiator: false,
            bus,
            send_queue: None,
            conn: AsyncBincodeStream::from(stream).for_async(),
        }
    }

    pub async fn run(mut self) {
        while self.tick().await {}
    }

    pub async fn tick(&mut self) -> bool {
        match self.state {
            ConnectionState::Connected => self.tick_connected().await,
            ConnectionState::Init => match self.initiate_connection().await {
                Ok(_) => {
                    self.state = ConnectionState::Connecting;
                }
                Err(e) => {
                    self.state = ConnectionState::CloseError(e);
                }
            },
            ConnectionState::Connecting => {
                if let Err(e) = self
                    .wait_handshake_response_with_deadline(handshake_deadline())
                    .await
                {
                    self.state = ConnectionState::CloseError(e);
                }
            }
            ConnectionState::CloseError(ref e) => {
                tracing::error!("closing connection with {:?}: {e}", self.peer);
                return false;
            }
            ConnectionState::Close => return false,
        }
        true
    }

    async fn tick_connected(&mut self) {
        tokio::select! {
            m = self.conn.next() => {
                match m {
                    Some(Ok(m)) => {
                        self.handle_message(m).await;
                    }
                    Some(Err(e)) => {
                        self.state = ConnectionState::CloseError(e.into());
                    }
                    None => {
                        self.state = ConnectionState::Close;
                    }
                }
            },
            // TODO: pop send queue
            Some(m) = self.send_queue.as_mut().unwrap().recv() => {
                self.conn.feed(m).await.unwrap();
                // send as many as possible
                while let Ok(m) = self.send_queue.as_mut().unwrap().try_recv() {
                    self.conn.feed(m).await.unwrap();
                }
                self.conn.flush().await.unwrap();
            },
            else => {
                self.state = ConnectionState::Close;
            }
        }
    }

    async fn handle_message(&mut self, enveloppe: Enveloppe) {
        let incomming = Inbound {
            from: self.peer.expect("peer id should be known at this point"),
            enveloppe,
        };
        self.bus.incomming(incomming).await;
    }

    fn close_error(&mut self, error: color_eyre::eyre::Error) {
        self.state = ConnectionState::CloseError(error);
    }

    /// wait for a handshake response from peer
    pub async fn wait_handshake_response_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> color_eyre::Result<()> {
        assert!(matches!(self.state, ConnectionState::Connecting));

        match tokio::time::timeout_at(deadline, self.conn.next()).await {
            Ok(Some(Ok(Enveloppe {
                message:
                    Message::Handshake {
                        protocol_version,
                        node_id,
                    },
                ..
            }))) => {
                if protocol_version != CURRENT_PROTO_VERSION {
                    let msg = Enveloppe {
                        database_id: None,
                        message: Message::Error(ProtoError::HandshakeVersionMismatch {
                            expected: CURRENT_PROTO_VERSION,
                        }),
                    };

                    let _ = self.conn.send(msg).await;

                    bail!("handshake error: invalid peer protocol version");
                } else {
                    // when not initiating a connection, respond to handshake message with a
                    // handshake message
                    if !self.is_initiator {
                        let msg = Enveloppe {
                            database_id: None,
                            message: Message::Handshake {
                                protocol_version: CURRENT_PROTO_VERSION,
                                node_id: self.bus.node_id(),
                            },
                        };
                        self.conn.send(msg).await?;
                    }

                    tracing::info!("Connected to peer {node_id}");

                    self.peer = Some(node_id);
                    self.state = ConnectionState::Connected;
                    self.send_queue = Some(self.bus.connect(node_id));

                    Ok(())
                }
            }
            Ok(Some(Ok(Enveloppe {
                message: Message::Error(e),
                ..
            }))) => {
                bail!("handshake error: {e}");
            }
            Ok(Some(Ok(_))) => {
                bail!("unexpected message from peer during handshake.");
            }
            Ok(Some(Err(e))) => {
                bail!("failed to perform handshake with peer: {e}");
            }
            Ok(None) => {
                bail!("failed to perform handshake with peer: connection closed");
            }
            Err(_e) => {
                bail!("failed to perform handshake with peer: timed out");
            }
        }
    }

    async fn initiate_connection(&mut self) -> color_eyre::Result<()> {
        let msg = Enveloppe {
            database_id: None,
            message: Message::Handshake {
                protocol_version: CURRENT_PROTO_VERSION,
                node_id: self.bus.node_id(),
            },
        };

        self.conn.send(msg).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::{future, pin_mut};
    use tokio::sync::Notify;
    use turmoil::net::{TcpListener, TcpStream};

    use super::*;

    #[test]
    fn invalid_handshake() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = 0;
        let done = Arc::new(Notify::new());
        let done_clone = done.clone();
        sim.host("host", move || {
            let done_clone = done_clone.clone();
            async move {
                let bus = Arc::new(Bus::new(host_node_id, |_, _| async {}));
                let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                    .await
                    .unwrap();
                let (s, _) = listener.accept().await.unwrap();
                let connection = Connection::new_acceptor(s, bus);
                let done = done_clone.notified();
                let run = connection.run();
                pin_mut!(done);
                pin_mut!(run);
                future::select(run, done).await;

                Ok(())
            }
        });

        sim.client("client", async move {
            let s = TcpStream::connect("host:1234").await.unwrap();
            let mut s = AsyncBincodeStream::<_, Enveloppe, Enveloppe, _>::from(s).for_async();

            let msg = Enveloppe {
                database_id: None,
                message: Message::Handshake {
                    protocol_version: 1234,
                    node_id: 1,
                },
            };
            s.send(msg).await.unwrap();
            let m = s.next().await.unwrap().unwrap();

            assert!(matches!(
                m.message,
                Message::Error(ProtoError::HandshakeVersionMismatch { .. })
            ));

            done.notify_waiters();

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn connection_closed_by_peer_close_connection() {
        let mut sim = turmoil::Builder::new().build();

        let notify = Arc::new(Notify::new());
        sim.host("host", {
            let notify = notify.clone();
            move || {
                let notify = notify.clone();
                async move {
                    let listener = TcpListener::bind("0.0.0.0:1234").await.unwrap();
                    let (stream, _) = listener.accept().await.unwrap();
                    notify.notified().await;

                    // drop connection
                    drop(stream);

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let stream = TcpStream::connect("host:1234").await.unwrap();
            let bus = Arc::new(Bus::new(1, |_, _| async {}));
            let mut conn = Connection::new_acceptor(stream, bus);

            notify.notify_waiters();

            conn.tick().await;

            assert!(matches!(conn.state, ConnectionState::CloseError(_)));

            Ok(())
        });

        sim.run().unwrap();
    }
}
