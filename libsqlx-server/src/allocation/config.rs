use serde::{Deserialize, Serialize};

/// Structural supertype of AllocConfig, used for checking the meta version. Subsequent version of
/// AllocConfig need to conform to this prototype.
#[derive(Debug, Serialize, Deserialize)]
struct ConfigVersion {
    config_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AllocConfig {
    pub max_conccurent_connection: u32,
    pub id: String,
    pub db_config: DbConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DbConfig {
    Primary {},
    Replica { primary_node_id: String },
}
