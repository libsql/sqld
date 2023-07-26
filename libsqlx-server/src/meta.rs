use std::fmt;
use std::mem::size_of;

use heed::bytemuck::{Pod, Zeroable};
use heed_types::{OwnedType, SerdeBincode};
use serde::{Deserialize, Serialize};
use sha3::digest::{ExtendableOutput, Update, XofReader};
use sha3::Shake128;
use tokio::task::block_in_place;

use crate::allocation::config::AllocConfig;

type ExecFn = Box<dyn FnOnce(&mut libsqlx::libsql::LibsqlConnection<()>)>;

pub struct Store {
    env: heed::Env,
    alloc_config_db: heed::Database<OwnedType<DatabaseId>, SerdeBincode<AllocConfig>>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Hash, Clone, Copy, Pod, Zeroable)]
#[repr(transparent)]
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

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), size_of::<Self>());
        Self(bytes.try_into().unwrap())
    }

    #[cfg(test)]
    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
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
    const ALLOC_CONFIG_DB_NAME: &'static str = "alloc_conf_db";

    pub fn new(env: heed::Env) -> Self {
        let mut txn = env.write_txn().unwrap();
        let alloc_config_db = env
            .create_database(&mut txn, Some(Self::ALLOC_CONFIG_DB_NAME))
            .unwrap();
        txn.commit().unwrap();

        Self {
            env,
            alloc_config_db,
        }
    }

    pub fn allocate(&self, id: &DatabaseId, meta: &AllocConfig) {
        //TODO: Handle conflict
        block_in_place(|| {
            let mut txn = self.env.write_txn().unwrap();
            if self
                .alloc_config_db
                .lazily_decode_data()
                .get(&txn, id)
                .unwrap()
                .is_some()
            {
                panic!("alloc already exists");
            };
            self.alloc_config_db.put(&mut txn, id, meta).unwrap();
            txn.commit().unwrap();
        });
    }

    pub fn deallocate(&self, id: &DatabaseId) {
        block_in_place(|| {
            let mut txn = self.env.write_txn().unwrap();
            self.alloc_config_db.delete(&mut txn, id).unwrap();
            txn.commit().unwrap();
        });
    }

    pub fn meta(&self, id: &DatabaseId) -> Option<AllocConfig> {
        block_in_place(|| {
            let txn = self.env.read_txn().unwrap();
            self.alloc_config_db.get(&txn, id).unwrap()
        })
    }

    pub fn list_allocs(&self) -> Vec<AllocConfig> {
        block_in_place(|| {
            let txn = self.env.read_txn().unwrap();
            self.alloc_config_db
                .iter(&txn)
                .unwrap()
                .map(Result::unwrap)
                .map(|x| x.1)
                .collect()
        })
    }
}
