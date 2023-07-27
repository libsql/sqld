use std::ops::Deref;
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
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub struct Primary {
    /// The maximum size the replication is allowed to grow. Expects a string like 200mb.
    #[serde(default = "default_max_log_size")]
    pub max_replication_log_size: bytesize::ByteSize,
    pub replication_log_compact_interval: Option<HumanDuration>,
    #[serde(default = "default_txn_timeout")]
    transaction_timeout_duration: HumanDuration,
}

#[derive(Debug)]
pub struct HumanDuration(Duration);

impl Deref for HumanDuration {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;
        impl serde::de::Visitor<'_> for DurationVisitor {
            type Value = HumanDuration;

            fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match humantime::Duration::from_str(v) {
                    Ok(d) => Ok(HumanDuration(*d)),
                    Err(e) => Err(E::custom(e.to_string())),
                }
            }

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a duration, in a string format")
            }
        }

        deserializer.deserialize_str(DurationVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbConfigReq {
    Primary(Primary),
    Replica {
        primary_node_id: NodeId,
        #[serde(default = "default_proxy_timeout")]
        proxy_request_timeout_duration: HumanDuration,
        #[serde(default = "default_txn_timeout")]
        transaction_timeout_duration: HumanDuration,
    },
}

const fn default_max_log_size() -> bytesize::ByteSize {
    bytesize::ByteSize::mb(100)
}

const fn default_proxy_timeout() -> HumanDuration {
    HumanDuration(Duration::from_secs(5))
}

const fn default_txn_timeout() -> HumanDuration {
    HumanDuration(Duration::from_secs(5))
}

async fn allocate(
    State(state): State<Arc<AdminServerState>>,
    Json(req): Json<AllocateReq>,
) -> Result<Json<AllocateResp>, Json<ErrorResponse>> {
    let config = AllocConfig {
        max_conccurent_connection: req.max_conccurent_connection.unwrap_or(16),
        db_name: req.database_name.clone(),
        db_config: match req.config {
            DbConfigReq::Primary(Primary {
                max_replication_log_size,
                replication_log_compact_interval,
                transaction_timeout_duration,
            }) => DbConfig::Primary {
                max_log_size: max_replication_log_size.as_u64() as usize,
                replication_log_compact_interval: replication_log_compact_interval
                    .as_deref()
                    .copied(),
                transaction_timeout_duration: *transaction_timeout_duration,
            },
            DbConfigReq::Replica {
                primary_node_id,
                proxy_request_timeout_duration,
                transaction_timeout_duration,
            } => DbConfig::Replica {
                primary_node_id,
                proxy_request_timeout_duration: *proxy_request_timeout_duration,
                transaction_timeout_duration: *transaction_timeout_duration,
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
        .into_iter()
        .map(|cfg| AllocView { id: cfg.db_name })
        .collect();

    Ok(Json(ListAllocResp { allocs }))
}
