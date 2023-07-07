use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use color_eyre::eyre::Result;
use hyper::server::accept::Accept;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{meta::MetaStore, allocation::config::AllocConfig};

pub struct AdminServerConfig {}

struct AdminServerState {
    meta_store: Arc<MetaStore>,
}

pub async fn run_admin_server<I>(_config: AdminServerConfig, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = AdminServerState {
        meta_store: todo!(),
    };
    let app = Router::new()
        .route("/manage/allocation/create", post(allocate))
        .with_state(Arc::new(state));
    axum::Server::builder(listener)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[derive(Serialize, Debug)]
struct ErrorResponse {}

#[derive(Serialize, Debug)]
struct AllocateResp { }

#[derive(Deserialize, Debug)]
struct AllocateReq {
    alloc_id: String,
    config: AllocConfig,
}

async fn allocate(
    State(state): State<Arc<AdminServerState>>,
    Json(req): Json<AllocateReq>,
) -> Result<Json<AllocateResp>, Json<ErrorResponse>> {
    state.meta_store.allocate(&req.alloc_id, &req.config).await;
    todo!();
}
