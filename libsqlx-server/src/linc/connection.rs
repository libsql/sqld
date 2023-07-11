use std::collections::HashMap;

use async_bincode::tokio::AsyncBincodeStream;
use async_bincode::AsyncDestination;
use color_eyre::eyre::{anyhow, bail};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_util::sync::PollSender;

use crate::linc::proto::{NodeError, NodeMessage};
use crate::linc::CURRENT_PROTO_VERSION;

use super::bus::{Bus, Registration};
use super::proto::{Message, StreamId, StreamMessage};
use super::{DatabaseId, NodeId};
use super::{StreamIdAllocator, MAX_STREAM_MSG};

#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    connection_sender: mpsc::Sender<ConnectionMessage>,
}

impl ConnectionHandle {
    pub async fn new_stream(&self, database_id: DatabaseId) -> color_eyre::eyre::Result<Stream> {
        let (send, ret) = oneshot::channel();
        self.connection_sender
            .send(ConnectionMessage::StreamCreate {
                database_id,
                ret: send,
            })
            .await
            .unwrap();

        Ok(ret.await?)
    }
}

/// A Bidirectional stream between databases on two nodes.
#[derive(Debug)]
pub struct Stream {
    stream_id: StreamId,
    /// sender to the connection
    sender: tokio_util::sync::PollSender<ConnectionMessage>,
    /// incoming message for this stream
    recv: tokio_stream::wrappers::ReceiverStream<StreamMessage>,
}

impl futures::Sink<StreamMessage> for Stream {
    type Error = tokio_util::sync::PollSendError<ConnectionMessage>;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sender.poll_ready_unpin(cx)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        payload: StreamMessage,
    ) -> Result<(), Self::Error> {
        let stream_id = self.stream_id;
        self.sender
            .start_send_unpin(ConnectionMessage::Message(Message::Stream {
                stream_id,
                payload,
            }))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sender.poll_flush_unpin(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sender.poll_close_unpin(cx)
    }
}

impl futures::Stream for Stream {
    type Item = StreamMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.recv.poll_next_unpin(cx)
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.recv.close();
        assert!(self.recv.as_mut().try_recv().is_err());
        let mut sender = self.sender.clone();
        let id = self.stream_id;
        if let Some(sender_ref) = sender.get_ref() {
            // Try send here is mostly for turmoil, since it stops polling the future as soon as
            // the test future returns which causes spawn to panic. In the tests, the channel will
            // always have capacity.
            if let Err(TrySendError::Full(m)) =
                sender_ref.try_send(ConnectionMessage::CloseStream(id))
            {
                tokio::task::spawn(async move {
                    let _ = sender.send(m).await;
                });
            }
        }
    }
}

struct StreamState {
    sender: mpsc::Sender<StreamMessage>,
}

/// A connection to another node. Manage the connection state, and (de)register streams with the
/// `Bus`
pub struct Connection<S> {
    /// Id of the current node
    pub peer: Option<NodeId>,
    /// State of the connection
    pub state: ConnectionState,
    /// Sink/Stream for network messages
    conn: AsyncBincodeStream<S, Message, Message, AsyncDestination>,
    /// Collection of streams for that connection
    streams: HashMap<StreamId, StreamState>,
    /// internal connection messages
    connection_messages: mpsc::Receiver<ConnectionMessage>,
    connection_messages_sender: mpsc::Sender<ConnectionMessage>,
    /// Are we the initiator of this connection?
    is_initiator: bool,
    bus: Bus,
    stream_id_allocator: StreamIdAllocator,
    /// handle to the registration of this connection to the bus.
    /// Dropping this deregister this connection from the bus
    registration: Option<Registration>,
}

#[derive(Debug)]
pub enum ConnectionMessage {
    StreamCreate {
        database_id: DatabaseId,
        ret: oneshot::Sender<Stream>,
    },
    CloseStream(StreamId),
    Message(Message),
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

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const MAX_CONNECTION_MESSAGES: usize = 128;

    pub fn new_initiator(stream: S, bus: Bus) -> Self {
        let (connection_messages_sender, connection_messages) =
            mpsc::channel(Self::MAX_CONNECTION_MESSAGES);
        Self {
            peer: None,
            state: ConnectionState::Init,
            conn: AsyncBincodeStream::from(stream).for_async(),
            streams: HashMap::new(),
            is_initiator: true,
            bus,
            stream_id_allocator: StreamIdAllocator::new(true),
            connection_messages,
            connection_messages_sender,
            registration: None,
        }
    }

    pub fn new_acceptor(stream: S, bus: Bus) -> Self {
        let (connection_messages_sender, connection_messages) =
            mpsc::channel(Self::MAX_CONNECTION_MESSAGES);
        Connection {
            peer: None,
            state: ConnectionState::Connecting,
            streams: HashMap::new(),
            connection_messages,
            connection_messages_sender,
            is_initiator: false,
            bus,
            conn: AsyncBincodeStream::from(stream).for_async(),
            stream_id_allocator: StreamIdAllocator::new(false),
            registration: None,
        }
    }

    pub fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            connection_sender: self.connection_messages_sender.clone(),
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
            }
            Some(command) = self.connection_messages.recv() => {
                self.handle_command(command).await;
            },
            else => {
                self.state = ConnectionState::Close;
            }
        }
    }

    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::Node(NodeMessage::OpenStream {
                stream_id,
                database_id,
            }) => {
                if self.streams.contains_key(&stream_id) {
                    self.send_message(Message::Node(NodeMessage::Error(
                        NodeError::StreamAlreadyExist(stream_id),
                    )))
                    .await;
                    return;
                }
                let stream = self.create_stream(stream_id);
                if let Err(e) = self.bus.notify_subscription(database_id, stream).await {
                    tracing::error!("{e}");
                    self.send_message(Message::Node(NodeMessage::Error(
                        NodeError::UnknownDatabase(database_id, stream_id),
                    )))
                    .await;
                }
            }
            Message::Node(NodeMessage::Handshake { .. }) => {
                self.close_error(anyhow!("unexpected handshake: closing connection"));
            }
            Message::Node(NodeMessage::CloseStream { stream_id: id }) => {
                self.close_stream(id);
            }
            Message::Node(NodeMessage::Error(e @ NodeError::HandshakeVersionMismatch { .. })) => {
                self.close_error(anyhow!("unexpected peer error: {e}"));
            }
            Message::Node(NodeMessage::Error(NodeError::UnknownStream(id))) => {
                tracing::error!("unkown stream: {id}");
                self.close_stream(id);
            }
            Message::Node(NodeMessage::Error(e @ NodeError::StreamAlreadyExist(_))) => {
                self.state = ConnectionState::CloseError(e.into());
            }
            Message::Node(NodeMessage::Error(ref e @ NodeError::UnknownDatabase(_, stream_id))) => {
                tracing::error!("{e}");
                self.close_stream(stream_id);
            }
            Message::Stream { stream_id, payload } => {
                match self.streams.get_mut(&stream_id) {
                    Some(s) => {
                        // TODO: there is not stream-independant control-flow for now.
                        // When/if control-flow is implemented, it will be handled here.
                        if s.sender.send(payload).await.is_err() {
                            self.close_stream(stream_id);
                        }
                    }
                    None => {
                        self.send_message(Message::Node(NodeMessage::Error(
                            NodeError::UnknownStream(stream_id),
                        )))
                        .await;
                    }
                }
            }
        }
    }

    fn close_error(&mut self, error: color_eyre::eyre::Error) {
        self.state = ConnectionState::CloseError(error);
    }

    fn close_stream(&mut self, id: StreamId) {
        self.streams.remove(&id);
    }

    async fn handle_command(&mut self, command: ConnectionMessage) {
        match command {
            ConnectionMessage::Message(m) => {
                self.send_message(m).await;
            }
            ConnectionMessage::CloseStream(stream_id) => {
                self.close_stream(stream_id);
                self.send_message(Message::Node(NodeMessage::CloseStream { stream_id }))
                    .await;
            }
            ConnectionMessage::StreamCreate { database_id, ret } => {
                let Some(stream_id) = self.stream_id_allocator.allocate() else {
                    // TODO: We close the connection here, which will cause a reconnections, and
                    // reset the stream_id allocator. If that happens in practice, it should be very quick to
                    // re-establish a connection. If this is an issue, we can either start using
                    // i64 stream_ids, or use a smarter id allocator.
                    self.state = ConnectionState::CloseError(anyhow!("Ran out of stream ids"));
                    return
                };
                assert_eq!(stream_id.is_positive(), self.is_initiator);
                assert!(!self.streams.contains_key(&stream_id));
                let stream = self.create_stream(stream_id);
                self.send_message(Message::Node(NodeMessage::OpenStream {
                    stream_id,
                    database_id,
                }))
                .await;
                let _ = ret.send(stream);
            }
        }
    }

    async fn send_message(&mut self, message: Message) {
        if let Err(e) = self.conn.send(message).await {
            self.close_error(e.into());
        }
    }

    fn create_stream(&mut self, stream_id: StreamId) -> Stream {
        let (sender, recv) = mpsc::channel(MAX_STREAM_MSG);
        let stream = Stream {
            stream_id,
            sender: PollSender::new(self.connection_messages_sender.clone()),
            recv: recv.into(),
        };
        self.streams.insert(stream_id, StreamState { sender });
        stream
    }

    /// wait for a handshake response from peer
    pub async fn wait_handshake_response_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> color_eyre::Result<()> {
        assert!(matches!(self.state, ConnectionState::Connecting));

        match tokio::time::timeout_at(deadline, self.conn.next()).await {
            Ok(Some(Ok(Message::Node(NodeMessage::Handshake {
                protocol_version,
                node_id,
            })))) => {
                if protocol_version != CURRENT_PROTO_VERSION {
                    let _ = self
                        .conn
                        .send(Message::Node(NodeMessage::Error(
                            NodeError::HandshakeVersionMismatch {
                                expected: CURRENT_PROTO_VERSION,
                            },
                        )))
                        .await;

                    bail!("handshake error: invalid peer protocol version");
                } else {
                    // when not initiating a connection, respond to handshake message with a
                    // handshake message
                    if !self.is_initiator {
                        self.conn
                            .send(Message::Node(NodeMessage::Handshake {
                                protocol_version: CURRENT_PROTO_VERSION,
                                node_id: self.bus.node_id,
                            }))
                            .await?;
                    }

                    self.peer = Some(node_id);
                    self.state = ConnectionState::Connected;
                    self.registration = Some(self.bus.register_connection(node_id, self.handle()));

                    Ok(())
                }
            }
            Ok(Some(Ok(Message::Node(NodeMessage::Error(e))))) => {
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
        self.conn
            .send(Message::Node(NodeMessage::Handshake {
                protocol_version: CURRENT_PROTO_VERSION,
                node_id: self.bus.node_id,
            }))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::Notify;
    use turmoil::net::{TcpListener, TcpStream};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn invalid_handshake() {
        let mut sim = turmoil::Builder::new().build();

        let host_node_id = NodeId::new_v4();
        sim.host("host", move || async move {
            let bus = Bus::new(host_node_id);
            let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                .await
                .unwrap();
            let (s, _) = listener.accept().await.unwrap();
            let mut connection = Connection::new_acceptor(s, bus);
            connection.tick().await;

            Ok(())
        });

        sim.client("client", async move {
            let s = TcpStream::connect("host:1234").await.unwrap();
            let mut s = AsyncBincodeStream::<_, Message, Message, _>::from(s).for_async();

            s.send(Message::Node(NodeMessage::Handshake {
                protocol_version: 1234,
                node_id: Uuid::new_v4(),
            }))
            .await
            .unwrap();
            let m = s.next().await.unwrap().unwrap();

            assert!(matches!(
                m,
                Message::Node(NodeMessage::Error(
                    NodeError::HandshakeVersionMismatch { .. }
                ))
            ));

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn stream_closed() {
        let mut sim = turmoil::Builder::new().build();

        let database_id = DatabaseId::new_v4();
        let host_node_id = NodeId::new_v4();
        let notify = Arc::new(Notify::new());
        sim.host("host", {
            let notify = notify.clone();
            move || {
                let notify = notify.clone();
                async move {
                    let bus = Bus::new(host_node_id);
                    let mut sub = bus.subscribe(database_id).unwrap();
                    let listener = turmoil::net::TcpListener::bind("0.0.0.0:1234")
                        .await
                        .unwrap();
                    let (s, _) = listener.accept().await.unwrap();
                    let connection = Connection::new_acceptor(s, bus);
                    tokio::task::spawn_local(connection.run());
                    let mut streams = Vec::new();
                    loop {
                        tokio::select! {
                            Some(mut stream) = sub.next() => {
                                let m = stream.next().await.unwrap();
                                stream.send(m).await.unwrap();
                                streams.push(stream);
                            }
                            _ = notify.notified() => {
                                break;
                            }
                        }
                    }

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let stream_id = StreamId::new(1);
            let node_id = NodeId::new_v4();
            let s = TcpStream::connect("host:1234").await.unwrap();
            let mut s = AsyncBincodeStream::<_, Message, Message, _>::from(s).for_async();

            s.send(Message::Node(NodeMessage::Handshake {
                protocol_version: CURRENT_PROTO_VERSION,
                node_id,
            }))
            .await
            .unwrap();
            let m = s.next().await.unwrap().unwrap();
            assert!(matches!(m, Message::Node(NodeMessage::Handshake { .. })));

            // send message to unexisting stream:
            s.send(Message::Stream {
                stream_id,
                payload: StreamMessage::Dummy,
            })
            .await
            .unwrap();
            let m = s.next().await.unwrap().unwrap();
            assert_eq!(
                m,
                Message::Node(NodeMessage::Error(NodeError::UnknownStream(stream_id)))
            );

            // open stream then send message
            s.send(Message::Node(NodeMessage::OpenStream {
                stream_id,
                database_id,
            }))
            .await
            .unwrap();
            s.send(Message::Stream {
                stream_id,
                payload: StreamMessage::Dummy,
            })
            .await
            .unwrap();
            let m = s.next().await.unwrap().unwrap();
            assert_eq!(
                m,
                Message::Stream {
                    stream_id,
                    payload: StreamMessage::Dummy
                }
            );

            s.send(Message::Node(NodeMessage::CloseStream {
                stream_id: StreamId::new(1),
            }))
            .await
            .unwrap();
            s.send(Message::Stream {
                stream_id,
                payload: StreamMessage::Dummy,
            })
            .await
            .unwrap();
            let m = s.next().await.unwrap().unwrap();
            assert_eq!(
                m,
                Message::Node(NodeMessage::Error(NodeError::UnknownStream(stream_id)))
            );

            notify.notify_waiters();

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
            let bus = Bus::new(NodeId::new_v4());
            let mut conn = Connection::new_acceptor(stream, bus);

            notify.notify_waiters();

            conn.tick().await;

            assert!(matches!(conn.state, ConnectionState::CloseError(_)));

            Ok(())
        });

        sim.run().unwrap();
    }

    #[test]
    fn zero_stream_id() {
        let mut sim = turmoil::Builder::new().build();

        let notify = Arc::new(Notify::new());
        sim.host("host", {
            let notify = notify.clone();
            move || {
                let notify = notify.clone();
                async move {
                    let listener = TcpListener::bind("0.0.0.0:1234").await.unwrap();
                    let (stream, _) = listener.accept().await.unwrap();
                    let (connection_messages_sender, connection_messages) = mpsc::channel(1);
                    let conn = Connection {
                        peer: Some(NodeId::new_v4()),
                        state: ConnectionState::Connected,
                        conn: AsyncBincodeStream::from(stream).for_async(),
                        streams: HashMap::new(),
                        connection_messages,
                        connection_messages_sender,
                        is_initiator: false,
                        bus: Bus::new(NodeId::new_v4()),
                        stream_id_allocator: StreamIdAllocator::new(false),
                        registration: None,
                    };

                    conn.run().await;

                    Ok(())
                }
            }
        });

        sim.client("client", async move {
            let stream = TcpStream::connect("host:1234").await.unwrap();
            let mut stream = AsyncBincodeStream::<_, Message, Message, _>::from(stream).for_async();

            stream
                .send(Message::Stream {
                    stream_id: StreamId::new_unchecked(0),
                    payload: StreamMessage::Dummy,
                })
                .await
                .unwrap();

            assert!(stream.next().await.is_none());

            Ok(())
        });

        sim.run().unwrap();
    }
}
