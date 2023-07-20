use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::routing::{delete, post};
use axum::{Json, Router};
use color_eyre::eyre::Result;
use hyper::server::accept::Accept;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::allocation::config::{AllocConfig, DbConfig};
use crate::linc::bus::Bus;
use crate::linc::NodeId;
use crate::manager::Manager;
use crate::meta::DatabaseId;

pub struct Config {
    pub bus: Arc<Bus<Arc<Manager>>>,
}

struct AdminServerState {
    bus: Arc<Bus<Arc<Manager>>>,
}

pub async fn run_admin_api<I>(config: Config, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = AdminServerState { bus: config.bus };

    let app = Router::new()
        .route("/manage/allocation", post(allocate).get(list_allocs))
        .route("/manage/allocation/:db_name", delete(deallocate))
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
    database_name: String,
    max_conccurent_connection: Option<u32>,
    config: DbConfigReq,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbConfigReq {
    Primary {},
    Replica {
        primary_node_id: NodeId,
        #[serde(
            deserialize_with = "deserialize_duration",
            default = "default_proxy_timeout"
        )]
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
        db_name: req.database_name.clone(),
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

    let dispatcher = state.bus.clone();
    let id = DatabaseId::from_name(&req.database_name);
    state.bus.handler().allocate(id, &config, dispatcher).await;

    Ok(Json(AllocateResp {}))
}

async fn deallocate(
    State(state): State<Arc<AdminServerState>>,
    Path(database_name): Path<String>,
) -> Result<Json<AllocateResp>, Json<ErrorResponse>> {
    let id = DatabaseId::from_name(&database_name);
    state.bus.handler().deallocate(id).await;

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
        .bus
        .handler()
        .store()
        .list_allocs()
        .await
        .into_iter()
        .map(|cfg| AllocView { id: cfg.db_name })
        .collect();

    Ok(Json(ListAllocResp { allocs }))
}
