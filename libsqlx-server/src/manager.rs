use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use moka::future::Cache;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::allocation::{Allocation, AllocationMessage, Database};
use crate::meta::Store;

pub struct Manager {
    cache: Cache<String, mpsc::Sender<AllocationMessage>>,
    meta_store: Arc<Store>,
    db_path: PathBuf,
}

const MAX_ALLOC_MESSAGE_QUEUE_LEN: usize = 32;

impl Manager {
    pub async fn alloc(&self, alloc_id: &str) -> mpsc::Sender<AllocationMessage> {
        if let Some(sender) = self.cache.get(alloc_id) {
            return sender.clone();
        }

        if let Some(config) = self.meta_store.meta(alloc_id).await {
            let path = self.db_path.join("dbs").join(alloc_id);
            tokio::fs::create_dir_all(&path).await.unwrap();
            let (alloc_sender, inbox) = mpsc::channel(MAX_ALLOC_MESSAGE_QUEUE_LEN);
            let alloc = Allocation {
                inbox,
                database: Database::from_config(&config, path),
                connections: HashMap::new(),
                connections_futs: JoinSet::new(),
                next_conn_id: 0,
                max_concurrent_connections: config.max_conccurent_connection,
            };

            tokio::spawn(alloc.run());

            self.cache
                .insert(alloc_id.to_string(), alloc_sender.clone())
                .await;

            return alloc_sender;
        }

        todo!("alloc doesn't exist")
    }
}
