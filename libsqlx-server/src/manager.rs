use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use moka::future::Cache;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::allocation::config::AllocConfig;
use crate::allocation::{Allocation, AllocationMessage, Database};
use crate::compactor::CompactionQueue;
use crate::hrana;
use crate::linc::bus::Dispatch;
use crate::linc::handler::Handler;
use crate::linc::Inbound;
use crate::meta::{DatabaseId, Store};
use crate::replica_commit_store::ReplicaCommitStore;

pub struct Manager {
    cache: Cache<DatabaseId, mpsc::Sender<AllocationMessage>>,
    meta_store: Arc<Store>,
    db_path: PathBuf,
    compaction_queue: Arc<CompactionQueue>,
    replica_commit_store: Arc<ReplicaCommitStore>,
}

const MAX_ALLOC_MESSAGE_QUEUE_LEN: usize = 32;

impl Manager {
    pub fn new(
        db_path: PathBuf,
        meta_store: Arc<Store>,
        max_conccurent_allocs: u64,
        compaction_queue: Arc<CompactionQueue>,
        replica_commit_store: Arc<ReplicaCommitStore>,
    ) -> Self {
        Self {
            cache: Cache::new(max_conccurent_allocs),
            meta_store,
            db_path,
            compaction_queue,
            replica_commit_store,
        }
    }

    /// Returns a handle to an allocation, lazily initializing if it isn't already loaded.
    pub async fn schedule(
        self: &Arc<Self>,
        database_id: DatabaseId,
        dispatcher: Arc<dyn Dispatch>,
    ) -> Option<mpsc::Sender<AllocationMessage>> {
        if let Some(sender) = self.cache.get(&database_id) {
            return Some(sender.clone());
        }

        if let Some(config) = self.meta_store.meta(&database_id) {
            let path = self.db_path.join("dbs").join(database_id.to_string());
            tokio::fs::create_dir_all(&path).await.unwrap();
            let (alloc_sender, inbox) = mpsc::channel(MAX_ALLOC_MESSAGE_QUEUE_LEN);
            let alloc = Allocation {
                inbox,
                database: Database::from_config(
                    &config,
                    path,
                    dispatcher.clone(),
                    self.compaction_queue.clone(),
                    self.replica_commit_store.clone(),
                )
                .unwrap(),
                connections_futs: JoinSet::new(),
                next_conn_id: 0,
                max_concurrent_connections: config.max_conccurent_connection,
                hrana_server: Arc::new(hrana::http::Server::new(None)),
                dispatcher, // TODO: handle self URL?
                db_name: config.db_name,
                connections: HashMap::new(),
            };

            tokio::spawn(alloc.run());

            self.cache.insert(database_id, alloc_sender.clone()).await;

            return Some(alloc_sender);
        }

        None
    }

    pub async fn allocate(
        self: &Arc<Self>,
        database_id: DatabaseId,
        meta: &AllocConfig,
        dispatcher: Arc<dyn Dispatch>,
    ) {
        self.store().allocate(&database_id, meta);
        self.schedule(database_id, dispatcher).await;
    }

    pub async fn deallocate(&self, database_id: DatabaseId) {
        self.meta_store.deallocate(&database_id);
        self.cache.remove(&database_id).await;
        let db_path = self.db_path.join("dbs").join(database_id.to_string());
        tokio::fs::remove_dir_all(db_path).await.unwrap();
    }

    pub fn store(&self) -> &Store {
        &self.meta_store
    }
}

#[async_trait::async_trait]
impl Handler for Arc<Manager> {
    async fn handle(&self, bus: Arc<dyn Dispatch>, msg: Inbound) {
        if let Some(sender) = self
            .clone()
            .schedule(msg.enveloppe.database_id.unwrap(), bus.clone())
            .await
        {
            let _ = sender.send(AllocationMessage::Inbound(msg)).await;
        }
    }
}
