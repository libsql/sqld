use std::path::PathBuf;
use std::sync::Arc;

use moka::future::Cache;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::allocation::{Allocation, AllocationMessage, Database};
use crate::hrana;
use crate::meta::Store;

pub struct Manager {
    cache: Cache<String, mpsc::Sender<AllocationMessage>>,
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
    pub async fn alloc(&self, alloc_id: &str) -> Option<mpsc::Sender<AllocationMessage>> {
        if let Some(sender) = self.cache.get(alloc_id) {
            return Some(sender.clone());
        }

        if let Some(config) = self.meta_store.meta(alloc_id).await {
            let path = self.db_path.join("dbs").join(alloc_id);
            tokio::fs::create_dir_all(&path).await.unwrap();
            let (alloc_sender, inbox) = mpsc::channel(MAX_ALLOC_MESSAGE_QUEUE_LEN);
            let alloc = Allocation {
                inbox,
                database: Database::from_config(&config, path),
                connections_futs: JoinSet::new(),
                next_conn_id: 0,
                max_concurrent_connections: config.max_conccurent_connection,
                hrana_server: Arc::new(hrana::http::Server::new(None)), // TODO: handle self URL?
            };

            tokio::spawn(alloc.run());

            self.cache
                .insert(alloc_id.to_string(), alloc_sender.clone())
                .await;

            return Some(alloc_sender);
        }

        None
    }
}
