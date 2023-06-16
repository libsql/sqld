use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use crate::error::Error;
use crate::Result;

pub struct DatabaseConfigStore {
    config_path: PathBuf,
    tmp_config_path: PathBuf,
    config: Mutex<Arc<DatabaseConfig>>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub block_level: BlockLevel,
    #[serde(default)]
    pub block_reason: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlockLevel {
    #[default]
    BlockNothing,
    BlockWrites,
    BlockReads,
}

impl DatabaseConfigStore {
    pub fn load(db_path: &Path) -> Result<Self> {
        let config_path = db_path.join("config.json");
        let tmp_config_path = db_path.join("config.json~");

        let config = match fs::read(&config_path) {
            Ok(data) => serde_json::from_slice(&data)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => DatabaseConfig::default(),
            Err(err) => return Err(Error::IOError(err)),
        };

        Ok(Self {
            config_path,
            tmp_config_path,
            config: Mutex::new(Arc::new(config)),
        })
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self {
            config_path: "".into(),
            tmp_config_path: "".into(),
            config: Mutex::new(Arc::new(DatabaseConfig::default())),
        }
    }

    pub fn set(&self, config: DatabaseConfig) -> Result<()> {
        let data = serde_json::to_vec_pretty(&config)?;
        fs::write(&self.tmp_config_path, data)?;
        fs::rename(&self.tmp_config_path, &self.config_path)?;
        *self.config.lock() = Arc::new(config);
        Ok(())
    }

    pub fn check_block_level(&self, max_block_level: BlockLevel) -> Result<()> {
        let config = self.config.lock();
        if config.block_level >= max_block_level {
            return Err(Error::Blocked(config.block_reason.clone()));
        }
        Ok(())
    }
}
