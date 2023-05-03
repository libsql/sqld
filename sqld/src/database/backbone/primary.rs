use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use tokio::sync::{mpsc, oneshot, watch};

use crate::database::backbone::MetaMessage;
use crate::database::libsql::LibSqlDb;
use crate::replication::{frame::Frame, ReplicationLoggerHook};

use super::{BackboneDatabase, BackboneReplicationLogger, Connections, Role};

pub struct PrimaryState<'a> {
    pub backbone: &'a mut BackboneDatabase,
    /// Database connections
    connections: Connections,
    frame_receiver: mpsc::Receiver<(Vec<Frame>, oneshot::Sender<anyhow::Result<()>>)>,
    current_kafka_offset: u64,
}

impl<'a> PrimaryState<'a> {
    pub fn new(
        backbone: &'a mut BackboneDatabase,
        current_kafka_offset: u64,
    ) -> anyhow::Result<Self> {
        let (sender, frame_receiver) = mpsc::channel(50);
        let logger = BackboneReplicationLogger::new(0, 0, sender);
        let hook = ReplicationLoggerHook::new(Arc::new(logger));
        let factory = enclose::enclose!(
            (backbone.config.extensions.clone() => ext,
             backbone.config.db_path.clone() => db_path,
             backbone.config.stats => stats)
            move || ready(LibSqlDb::new(
                    db_path.clone(),
                    ext.clone(),
                    hook.clone(),
                    false,
                    stats.clone(),
            ))
        );

        Ok(Self {
            backbone,
            connections: Connections::new(Box::new(factory)),
            frame_receiver,
            current_kafka_offset,
        })
    }

    async fn handle_frames(
        &mut self,
        producer: &mut FutureProducer,
        frames: Vec<Frame>,
        mut recv: watch::Receiver<(Option<MetaMessage>, i64)>,
    ) -> anyhow::Result<()> {
        for frame in frames {
            let key = format!("frame:{}", self.backbone.term);
            let record = FutureRecord::to(&self.backbone.config.cluster_id)
                .key(&key)
                .payload(frame.as_bytes());
            match producer.send(record, Timeout::Never).await {
                Ok((_partition, offset)) => {
                    // Since there is only a single primary, no one else should be writing to the queue
                    // while we are. If we notice that the offset of the message we just wrote increased
                    // by more than one, it means that someone else wrote to the queue, and we should
                    // rollback the transaction.
                    //
                    // there may be in flight messages, that doesn't mean we are not the leader
                    // anymore. In order to solve that:
                    // - if the condition described above is satisfied, proceed with commit
                    // - else, spawn the consumer on a different task, and wait for this task to catch up
                    // with the replication offset. Once this is done compare the terms. If the term has
                    // changed, then, stepdown and rollback. Otherwise, we are still leader, proceed to
                    // commit and update offset.
                    if dbg!(offset) as u64 != dbg!(self.current_kafka_offset + 1) {
                        let r = recv.wait_for(|(_, offset)| offset >= offset).await?;
                        if let (Some(ref meta), _) = *r {
                            if meta.term > self.backbone.term {
                                // new term, we'll have to step_down
                                tracing::error!("cannot perform write: not a leader");
                                bail!("not a leader");
                            } else {
                                self.current_kafka_offset = offset as _;
                                continue;
                            }
                        }
                        bail!("offset of the message doesn't match expected offset");
                    }
                    self.current_kafka_offset += 1;
                }
                Err((_e, _msg)) => {
                    bail!("failed to replicate")
                }
            }
        }

        Ok(())
    }

    pub async fn run(mut self) -> anyhow::Result<Option<Role<'a>>> {
        tracing::info!("entering primary state");
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
        let mut producer: FutureProducer = config.set("transactional.id", "12").create()?;
        producer.init_transactions(Timeout::Never)?;

        let (sender, mut recv) = watch::channel((None, 0));
        let consumer_loop_handle = tokio::spawn(async move {
            let mut stream = consumer.stream();
            loop {
                tokio::select! {
                    biased;
                    _ = sender.closed() => {
                        return Ok(())
                    }
                    Some(msg) = stream.next() => {
                        match msg {
                            Ok(msg) => {
                                if msg.key() == Some(b"meta") {
                                    let meta: MetaMessage = serde_json::from_slice(msg.payload().unwrap())?;
                                    if let Err(_) = sender.send((Some(meta), msg.offset())) {
                                        return Ok(());
                                    }
                                    // if self.backbone.term >= meta.term {
                                    //     continue;
                                    // }
                                    // let offset = msg.offset();
                                    // drop(msg); // holding a ref to the message while dropping the consumer causes a
                                    // return Role::transition(self, meta, offset).map(Some);
                                } else {
                                    continue;
                                }
                            }
                            Err(e) => bail!(e),
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                biased;
                maybe_new_term = recv.changed() => {
                    match maybe_new_term {
                        Ok(()) => {
                            let term = {
                                let meta_ref = recv.borrow_and_update() ;
                                let (Some(ref meta), _) = *meta_ref else { panic!("watcher updated without new meta") };
                                meta.term
                            };
                            dbg!(term);
                            if dbg!(term) > dbg!(self.backbone.term) {
                                // this must be the last channel, dropping it will cause the
                                // consumer loop to exit.
                                let new_role = {
                                    let meta_ref = recv.borrow_and_update() ;
                                    let (Some(ref meta), offset) = *meta_ref else { panic!("watcher updated without new meta") };
                                    Role::transition(self, meta, offset).map(Some)
                                };
                                dbg!();
                                drop(recv);
                                let _ = consumer_loop_handle.await;
                                dbg!();
                                return new_role;
                            }
                        }
                        Err(_) => {
                            todo!()
                        }
                    }
                }
                Some((frames, ret)) = self.frame_receiver.recv() => {
                    if let Err(e) = producer.begin_transaction() {
                        let _ = ret.send(Err(anyhow::anyhow!("failed to start transaction: {e}")));
                        continue;
                    }
                    // increment offset for begin
                    self.current_kafka_offset += 1;
                    match self.handle_frames(&mut producer, frames, recv.clone()).await {
                        Ok(_) => {
                            // todo: this blocks the executor
                            if let Err(e) = producer.commit_transaction(Duration::from_secs(1)) {
                                let _ =  ret.send(Err(anyhow::anyhow!("failed to commit transaction: {e}")));
                            } else {
                                // increment offset for commit
                                self.current_kafka_offset += 1;
                                let _ = ret.send(Ok(()));
                            }
                        },
                        Err(e) => {
                            let _ = ret.send(Err(anyhow::anyhow!("failed to commit transaction: {e}")));
                            // todo: this blocks the executor
                            if let Err(e) = producer.abort_transaction(Duration::from_secs(2)) {
                                tracing::error!("failed to rollback: {e}");
                            }
                            // increment offset for rollback
                            self.current_kafka_offset += 1;
                        },
                    }
                }
                Some((id, msg)) = self.backbone.db_ops_receiver.recv() => {
                    self.connections.handle_op(id, msg).await?;
                },
                else => return Ok(None),
            }
        }
    }
}
