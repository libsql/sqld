use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};

use crate::database::backbone::MetaMessage;

use super::{BackboneDatabase, Role};

pub struct InitState<'a> {
    pub backbone: &'a mut BackboneDatabase,
    consumer: StreamConsumer,
}

impl<'a> InitState<'a> {
    pub fn new(backbone: &'a mut BackboneDatabase) -> anyhow::Result<Self> {
        let mut config = ClientConfig::new();
        config
            .set("group.id", &backbone.config.node_id)
            .set(
                "bootstrap.servers",
                backbone
                    .config
                    .kafka_bootstrap_servers
                    .first()
                    .unwrap()
                    .to_string(),
            )
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false");

        let consumer: StreamConsumer =
            config.clone().set("auto.offset.reset", "latest").create()?;
        consumer.subscribe(&[&backbone.config.cluster_id])?;

        Ok(Self { consumer, backbone })
    }

    pub async fn run(self) -> anyhow::Result<Role<'a>> {
        tracing::info!("entering idle state");
        dbg!(&self.backbone.config.node_id);
        dbg!();
        loop {
            let msg = self.consumer.recv().await?;
            if msg.key() == Some(b"meta") {
                match msg.payload() {
                    Some(payload) => {
                        let meta: MetaMessage = serde_json::from_slice(payload)?;
                        if self.backbone.term >= meta.term {
                            continue;
                        }
                        let offset = msg.offset();
                        drop(msg); // holding a ref to the message while dropping the consumer causes a
                                   // deadlock in the Drop implementation of StreamConsumer
                        return Role::transition(self, &meta, offset);
                    }
                    None => anyhow::bail!("message with empty payload"),
                }
            }
        }
    }
}
