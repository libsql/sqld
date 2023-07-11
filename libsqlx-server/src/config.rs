use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::Visitor;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// Database path
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
    /// Cluster configuration
    pub cluster: ClusterConfig,
    /// User API configuration
    pub user_api: UserApiConfig,
    /// Admin API configuration
    pub admin_api: AdminApiConfig,
}

impl Config {
    pub fn validate(&self) -> color_eyre::Result<()> {
        // TODO: implement validation
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    /// Address to bind this node to
    #[serde(default = "default_linc_addr")]
    pub addr: SocketAddr,
    /// List of peers in the format `<node_id>:<node_addr>`
    pub peers: Vec<Peer>,
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

fn default_linc_addr() -> SocketAddr {
    "0.0.0.0:5001".parse().unwrap()
}

#[derive(Debug, Clone)]
struct Peer {
    id: u64,
    addr: String,
}

impl<'de> Deserialize<'de> for Peer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
            struct V;

            impl Visitor<'_> for V {
                type Value = Peer;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a string in the format <node_id>:<node_addr>")
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error, {

                        let mut iter = v.split(":");
                        let Some(id) = iter.next() else { return Err(E::custom("node id is missing")) };
                        let Ok(id) = id.parse::<u64>() else { return Err(E::custom("failed to parse node id")) };
                        let Some(addr) = iter.next() else { return Err(E::custom("node address is missing")) };
                        Ok(Peer { id, addr: addr.to_string() })
                }
            }

            deserializer.deserialize_str(V)
        }
}

