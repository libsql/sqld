use bytes::Bytes;
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio::sync::watch;

use crate::configure_rpc;
use crate::database::backbone::MetaMessage;
use crate::database::write_proxy::WriteProxyDbFactory;
use crate::replication::replica::{FrameInjectorHandle, Frames, ReplicationMeta};
use crate::replication::{frame::Frame, FrameNo};

use super::{BackboneDatabase, Connections, NodeInfo, Role};

pub struct ReplicaState<'a> {
    pub backbone: &'a mut BackboneDatabase,
    buffer: Vec<Frame>,
    next_frame_no: FrameNo,
    connections: Connections,
    frame_no_sender: watch::Sender<FrameNo>,
    current_primary: NodeInfo,
}

impl<'a> ReplicaState<'a> {
    pub fn new(backbone: &'a mut BackboneDatabase, current_primary: NodeInfo) -> Self {
        let config = &backbone.config;
        let (channel, uri) = configure_rpc(
            dbg!(current_primary.addr.clone()),
            config.rpc_tls_config.clone(),
        )
        .expect("invalid rpc configuration");
        let (frame_no_sender, frame_no_receiver) = watch::channel(0);
        let factory = WriteProxyDbFactory::new(
            config.db_path.clone(),
            config.extensions.clone(),
            channel,
            uri,
            config.stats.clone(),
            frame_no_receiver,
        );
        let connections = Connections::new(Box::new(factory));

        Self {
            backbone,
            buffer: Vec::new(),
            connections,
            frame_no_sender,
            current_primary,
            next_frame_no: 0,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<Role<'a>> {
        tracing::info!(
            primary = self.current_primary.id,
            addr = self.current_primary.addr,
            "Started running in replica mode",
        );

        let (mut injector, _) =
            FrameInjectorHandle::new(self.backbone.config.db_path.clone(), |meta| {
                Ok(meta.unwrap_or_else(|| ReplicationMeta {
                    pre_commit_frame_no: 0,
                    post_commit_frame_no: 0,
                    generation_id: 0,
                    database_id: 0,
                }))
            })
            .await?;

        let mut config = ClientConfig::new();
        config
            .set("group.id", &self.backbone.config.node_id)
            .set(
                "bootstrap.servers",
                self.backbone
                    .config
                    .kafka_bootstrap_servers
                    .first()
                    .unwrap()
                    .to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false");
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(&[&self.backbone.config.cluster_id])?;
        let mut stream = consumer.stream();

        loop {
            tokio::select! {
                biased;
                Some(Ok(msg)) = stream.next() => {
                    if msg.key() == Some(b"meta") {
                        let meta: MetaMessage = serde_json::from_slice(msg.payload().unwrap())?;
                        if self.backbone.term >= meta.term {
                            continue;
                        }
                        let offset = msg.offset();
                        drop(msg);
                        injector.shutdown().await?;
                        return Role::transition(self, &meta, offset);
                    } else if let Some(key) = msg.key() {
                        if key.starts_with(b"frame") {
                            let key =std::str::from_utf8(key).unwrap();
                            let (_key, term) = key.split_once(':').unwrap();
                            let term = term.parse::<u64>().unwrap();
                            assert_eq!(term, self.backbone.term);
                            if let Some(payload) = msg.payload() {
                                let bytes = Bytes::copy_from_slice(payload);
                                let frame = Frame::try_from_bytes(bytes).unwrap();
                                // TODO: this is blocking for too long, delegate to a separate thread
                                // instead
                                self.handle_frame(frame, &mut injector).await;
                            }

                        }
                    }
                }
                Some((id, op)) = self.backbone.db_ops_receiver.recv() => {
                    self.connections.handle_op(id, op).await?;
                }
            }
        }
    }

    async fn handle_frame(&mut self, frame: Frame, injector: &mut FrameInjectorHandle) {
        let should_inject = frame.header().size_after != 0;
        dbg!(frame.header());
        assert_eq!(self.next_frame_no, frame.header().frame_no);
        self.next_frame_no += 1;
        self.buffer.push(frame);
        dbg!(should_inject);
        if should_inject {
            // transaction boundary, inject buffer
            injector
                .inject_frames(Frames::Vec(std::mem::take(&mut self.buffer)))
                .await
                .unwrap();
        }
    }
}
