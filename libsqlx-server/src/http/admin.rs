use std::{path::PathBuf, sync::Arc};

use axum::{extract::State, routing::post, Json, Router};
use color_eyre::eyre::Result;
use hyper::server::accept::Accept;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    allocation::config::{AllocConfig, DbConfig},
    meta::Store,
};

pub struct AdminServerConfig {
    pub db_path: PathBuf,
}

struct AdminServerState {
    meta_store: Arc<Store>,
}

pub async fn run_admin_server<I>(config: AdminServerConfig, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = AdminServerState {
        meta_store: Arc::new(Store::new(&config.db_path)),
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbConfigReq {
    Primary { },
    Replica { primary_node_id: String },
}

async fn allocate(
    State(state): State<Arc<AdminServerState>>,
    Json(req): Json<AllocateReq>,
) -> Result<Json<AllocateResp>, Json<ErrorResponse>> {
    let config = AllocConfig {
        max_conccurent_connection: req.max_conccurent_connection.unwrap_or(16),
        id: req.alloc_id.clone(),
        db_config: match req.config {
            DbConfigReq::Primary {  } => DbConfig::Primary {  },
            DbConfigReq::Replica { primary_node_id } => DbConfig::Replica { primary_node_id },
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
        .map(|cfg| AllocView { id: cfg.id })
        .collect();

    Ok(Json(ListAllocResp { allocs }))
}
