use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub db_path: PathBuf,
    pub cluster_config: ClusterConfig,
    pub user_api_config: UserApiConfig,
    pub admin_api_config: AdminApiConfig,
}

impl Config {
    pub fn validate(&self) -> color_eyre::Result<()> {
        // TODO: implement validation
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub addr: SocketAddr,
    pub peers: Vec<(u64, String)>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct UserApiConfig {
    pub addr: SocketAddr,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AdminApiConfig {
    pub addr: SocketAddr,
}
