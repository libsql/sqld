use core::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha3::digest::{ExtendableOutput, Update, XofReader};
use sha3::Shake128;
use sled::Tree;
use tokio::task::block_in_place;

use crate::allocation::config::AllocConfig;

type ExecFn = Box<dyn FnOnce(&mut libsqlx::libsql::LibsqlConnection<()>)>;

pub struct Store {
    meta_store: Tree,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Hash, Clone, Copy)]
pub struct DatabaseId([u8; 16]);

impl DatabaseId {
    pub fn from_name(name: &str) -> Self {
        let mut hasher = Shake128::default();
        hasher.update(name.as_bytes());
        let mut reader = hasher.finalize_xof();
        let mut out = [0; 16];
        reader.read(&mut out);
        Self(out)
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", u128::from_be_bytes(self.0))
    }
}

impl AsRef<[u8]> for DatabaseId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Store {
    pub fn new(path: &Path) -> Self {
        std::fs::create_dir_all(&path).unwrap();
        let path = path.join("store");
        let db = sled::open(path).unwrap();
        let meta_store = db.open_tree("meta_store").unwrap();

        Self { meta_store }
    }

    pub async fn allocate(&self, database_name: &str, meta: &AllocConfig) {
        //TODO: Handle conflict
        block_in_place(|| {
            let meta_bytes = bincode::serialize(meta).unwrap();
            let id = DatabaseId::from_name(database_name);
            self.meta_store
                .compare_and_swap(id, None as Option<&[u8]>, Some(meta_bytes))
                .unwrap()
                .unwrap();
        });
    }

    pub async fn deallocate(&self, _database_name: &str) {
        todo!()
    }

    pub async fn meta(&self, database_id: &DatabaseId) -> Option<AllocConfig> {
        block_in_place(|| {
            let config = self.meta_store.get(database_id).unwrap()?;
            let config = bincode::deserialize(config.as_ref()).unwrap();
            Some(config)
        })
    }

    pub async fn list_allocs(&self) -> Vec<AllocConfig> {
        block_in_place(|| {
            let mut out = Vec::new();
            for kv in self.meta_store.iter() {
                let (_k, v) = kv.unwrap();
                let alloc = bincode::deserialize(&v).unwrap();
                out.push(alloc);
            }

            out
        })
    }
}
