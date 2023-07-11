use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    cluster_config: ClusterConfig,
    user_api_config: UserApiConfig,
    admin_api_config: AdminApiConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    addr: SocketAddr,
    peers: Vec<(u64, String)>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct UserApiConfig {
    addr: SocketAddr,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AdminApiConfig {
    addr: SocketAddr,
}
