use std::path::Path;

use sled::Tree;
use uuid::Uuid;

use crate::allocation::config::AllocConfig;

type ExecFn = Box<dyn FnOnce(&mut libsqlx::libsql::LibsqlConnection<()>)>;

pub struct Store {
    meta_store: Tree,
}

impl Store {
    pub fn new(path: &Path) -> Self {
        std::fs::create_dir_all(&path).unwrap();
        let path = path.join("store");
        let db = sled::open(path).unwrap();
        let meta_store = db.open_tree("meta_store").unwrap();

        Self { meta_store }
    }

    pub async fn allocate(&self, alloc_id: &str, meta: &AllocConfig) {
        //TODO: Handle conflict
        tokio::task::block_in_place(|| {
            let meta_bytes = bincode::serialize(meta).unwrap();
            self.meta_store
                .compare_and_swap(alloc_id, None as Option<&[u8]>, Some(meta_bytes))
                .unwrap()
                .unwrap();
        });
    }

    pub async fn deallocate(&self, alloc_id: Uuid) {}

    pub async fn meta(&self, alloc_id: &str) -> Option<AllocConfig> {
        tokio::task::block_in_place(|| {
            let config = self.meta_store.get(alloc_id).unwrap()?;
            let config = bincode::deserialize(config.as_ref()).unwrap();
            Some(config)
        })
    }

    pub async fn list_allocs(&self) -> Vec<AllocConfig> {
        tokio::task::block_in_place(|| {
            let mut out = Vec::new();
            for kv in self.meta_store.iter() {
                let (k, v) = kv.unwrap();
                let alloc = bincode::deserialize(&v).unwrap();
                out.push(alloc);
            }

            out
        })
    }
}
