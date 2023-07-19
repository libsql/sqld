use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use moka::future::Cache;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::allocation::{Allocation, AllocationMessage, Database};
use crate::hrana;
use crate::linc::bus::Bus;
use crate::linc::handler::Handler;
use crate::linc::Inbound;
use crate::meta::{DatabaseId, Store};

pub struct Manager {
    cache: Cache<DatabaseId, mpsc::Sender<AllocationMessage>>,
    meta_store: Arc<Store>,
    db_path: PathBuf,
}

const MAX_ALLOC_MESSAGE_QUEUE_LEN: usize = 32;

impl Manager {
    pub fn new(db_path: PathBuf, meta_store: Arc<Store>, max_conccurent_allocs: u64) -> Self {
        Self {
            cache: Cache::new(max_conccurent_allocs),
            meta_store,
            db_path,
        }
    }

    /// Returns a handle to an allocation, lazily initializing if it isn't already loaded.
    pub async fn alloc(
        self: &Arc<Self>,
        database_id: DatabaseId,
        bus: Arc<Bus<Arc<Self>>>,
    ) -> Option<mpsc::Sender<AllocationMessage>> {
        if let Some(sender) = self.cache.get(&database_id) {
            return Some(sender.clone());
        }

        if let Some(config) = self.meta_store.meta(&database_id).await {
            let path = self.db_path.join("dbs").join(database_id.to_string());
            tokio::fs::create_dir_all(&path).await.unwrap();
            let (alloc_sender, inbox) = mpsc::channel(MAX_ALLOC_MESSAGE_QUEUE_LEN);
            let alloc = Allocation {
                inbox,
                database: Database::from_config(&config, path, bus.clone()),
                connections_futs: JoinSet::new(),
                next_conn_id: 0,
                max_concurrent_connections: config.max_conccurent_connection,
                hrana_server: Arc::new(hrana::http::Server::new(None)),
                bus, // TODO: handle self URL?
                db_name: config.db_name,
                connections: HashMap::new(),
            };

            tokio::spawn(alloc.run());

            self.cache.insert(database_id, alloc_sender.clone()).await;

            return Some(alloc_sender);
        }

        None
    }
}

#[async_trait::async_trait]
impl Handler for Arc<Manager> {
    async fn handle(&self, bus: Arc<Bus<Self>>, msg: Inbound) {
        if let Some(sender) = self
            .clone()
            .alloc(msg.enveloppe.database_id.unwrap(), bus.clone())
            .await
        {
            let _ = sender.send(AllocationMessage::Inbound(msg)).await;
        }
    }
}
