use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_db_path")]
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
    #[serde(default = "default_user_addr")]
    pub addr: SocketAddr,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AdminApiConfig {
    #[serde(default = "default_admin_addr")]
    pub addr: SocketAddr,
}

fn default_db_path() -> PathBuf {
    PathBuf::from("data.sqld")
}

fn default_admin_addr() -> SocketAddr {
    "0.0.0.0:8081".parse().unwrap()
}

fn default_user_addr() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}
