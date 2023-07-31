use std::fmt;
use std::mem::size_of;

use chrono::{DateTime, Utc};
use heed::bytemuck::{Pod, Zeroable};
use heed_types::{OwnedType, SerdeBincode};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sha3::digest::{ExtendableOutput, Update, XofReader};
use sha3::Shake128;
use tokio::task::block_in_place;

use crate::allocation::config::AllocConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct AllocMeta {
    pub config: AllocConfig,
    pub created_at: DateTime<Utc>,
}

pub struct Store {
    env: heed::Env,
    alloc_config_db: heed::Database<OwnedType<DatabaseId>, SerdeBincode<AllocMeta>>,
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

#[derive(Debug, thiserror::Error)]
pub enum AllocationError {
    #[error("an allocation already exists for {0}")]
    AlreadyExist(String),
}

impl Store {
    const ALLOC_CONFIG_DB_NAME: &'static str = "alloc_conf_db";

    pub fn new(env: heed::Env) -> crate::Result<Self> {
        let mut txn = env.write_txn()?;
        let alloc_config_db = env.create_database(&mut txn, Some(Self::ALLOC_CONFIG_DB_NAME))?;
        txn.commit()?;

        Ok(Self {
            env,
            alloc_config_db,
        })
    }

    pub fn allocate(&self, id: &DatabaseId, config: AllocConfig) -> crate::Result<AllocMeta> {
        block_in_place(|| {
            let mut txn = self.env.write_txn()?;
            if self
                .alloc_config_db
                .lazily_decode_data()
                .get(&txn, id)?
                .is_some()
            {
                Err(AllocationError::AlreadyExist(config.db_name.clone()))?;
            };

            let meta = AllocMeta {
                config,
                created_at: Utc::now(),
            };

            self.alloc_config_db.put(&mut txn, id, &meta)?;

            txn.commit()?;

            Ok(meta)
        })
    }

    pub fn deallocate(&self, id: &DatabaseId) -> crate::Result<()> {
        block_in_place(|| {
            let mut txn = self.env.write_txn()?;
            self.alloc_config_db.delete(&mut txn, id)?;
            txn.commit()?;

            Ok(())
        })
    }

    pub fn meta(&self, id: &DatabaseId) -> crate::Result<Option<AllocMeta>> {
        block_in_place(|| {
            let txn = self.env.read_txn()?;
            Ok(self.alloc_config_db.get(&txn, id)?)
        })
    }

    pub fn list_allocs(&self) -> crate::Result<Vec<AllocMeta>> {
        block_in_place(|| {
            let txn = self.env.read_txn()?;
            let res = self
                .alloc_config_db
                .iter(&txn)?
                .map(|x| x.map(|x| x.1))
                .try_collect()?;
            Ok(res)
        })
    }
}
