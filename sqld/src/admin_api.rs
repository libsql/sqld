use anyhow::Context as _;
use axum::Json;
use enclose::enclose;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::database::config::{BlockLevel, DatabaseConfig, DatabaseConfigStore};

pub async fn run_admin_api(
    addr: SocketAddr,
    db_config_store: Arc<DatabaseConfigStore>,
) -> anyhow::Result<()> {
    use axum::routing::{get, post};
    let router = axum::Router::new()
        .route("/", get(handle_get_index))
        .route(
            "/v1/config",
            get(enclose! {(db_config_store) move || async move {
                handle_get_config(&db_config_store).await
            }}),
        )
        .route(
            "/v1/block",
            post(enclose! {(db_config_store) move |req| async move{
                handle_post_block(&db_config_store, req).await
            }}),
        );

    let server = hyper::Server::try_bind(&addr)
        .context("Could not bind admin HTTP API server")?
        .serve(router.into_make_service());

    tracing::info!(
        "Listening for admin HTTP API requests on {}",
        server.local_addr()
    );
    server.await?;
    Ok(())
}

async fn handle_get_index() -> String {
    "Welcome to the sqld admin API".into()
}

async fn handle_get_config(db_config_store: &DatabaseConfigStore) -> Json<Arc<DatabaseConfig>> {
    Json(db_config_store.get())
}

#[derive(Debug, Deserialize)]
struct BlockReq {
    block_level: BlockLevel,
    #[serde(default)]
    block_reason: Option<String>,
}

async fn handle_post_block(
    db_config_store: &DatabaseConfigStore,
    Json(req): Json<BlockReq>,
) -> (axum::http::StatusCode, String) {
    let mut config = (*db_config_store.get()).clone();
    config.block_level = req.block_level;
    config.block_reason = req.block_reason;

    match db_config_store.store(config) {
        Ok(()) => (axum::http::StatusCode::OK, "OK".into()),
        Err(err) => {
            tracing::warn!("Could not store database config: {err}");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed".into(),
            )
        }
    }
}
