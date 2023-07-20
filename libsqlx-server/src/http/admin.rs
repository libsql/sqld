use std::sync::Arc;
use std::str::FromStr;
use std::time::Duration;

use axum::{Json, Router};
use axum::routing::post;
use axum::extract::State;
use color_eyre::eyre::Result;
use hyper::server::accept::Accept;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::meta::Store;
use crate::allocation::config::{AllocConfig, DbConfig};
use crate::linc::NodeId;

pub struct Config {
    pub meta_store: Arc<Store>,
}

struct AdminServerState {
    meta_store: Arc<Store>,
}

pub async fn run_admin_api<I>(config: Config, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = AdminServerState {
        meta_store: config.meta_store,
    };

    let app = Router::new()
        .route("/manage/allocation", post(allocate).get(list_allocs))
        .with_state(Arc::new(state));
    axum::Server::builder(listener)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[derive(Serialize, Debug)]
struct ErrorResponse {}

#[derive(Serialize, Debug)]
struct AllocateResp {}

#[derive(Deserialize, Debug)]
struct AllocateReq {
    alloc_id: String,
    max_conccurent_connection: Option<u32>,
    config: DbConfigReq,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbConfigReq {
    Primary {},
    Replica {
        primary_node_id: NodeId,
        #[serde(deserialize_with = "deserialize_duration", default = "default_proxy_timeout")]
        proxy_request_timeout_duration: Duration,
    },
}

const fn default_proxy_timeout() -> Duration {
    Duration::from_secs(5)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value = Duration;

        fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error, 
        {
            match humantime::Duration::from_str(v) {
                Ok(d) => Ok(*d),
                Err(e) => Err(E::custom(e.to_string())),
            }
        }

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a duration, in a string format")
        }

    }

    deserializer.deserialize_str(Visitor)
}

async fn allocate(
    State(state): State<Arc<AdminServerState>>,
    Json(req): Json<AllocateReq>,
) -> Result<Json<AllocateResp>, Json<ErrorResponse>> {
    let config = AllocConfig {
        max_conccurent_connection: req.max_conccurent_connection.unwrap_or(16),
        db_name: req.alloc_id.clone(),
        db_config: match req.config {
            DbConfigReq::Primary {} => DbConfig::Primary {},
            DbConfigReq::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
            } => DbConfig::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
            },
        },
    };
    state.meta_store.allocate(&req.alloc_id, &config).await;

    Ok(Json(AllocateResp {}))
}

#[derive(Serialize, Debug)]
struct ListAllocResp {
    allocs: Vec<AllocView>,
}

#[derive(Serialize, Debug)]
struct AllocView {
    id: String,
}

async fn list_allocs(
    State(state): State<Arc<AdminServerState>>,
) -> Result<Json<ListAllocResp>, Json<ErrorResponse>> {
    let allocs = state
        .meta_store
        .list_allocs()
        .await
        .into_iter()
        .map(|cfg| AllocView { id: cfg.db_name })
        .collect();

    Ok(Json(ListAllocResp { allocs }))
}
