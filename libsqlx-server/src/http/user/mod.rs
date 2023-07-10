use std::sync::Arc;

use axum::routing::post;
use axum::{Json, Router};
use color_eyre::Result;
use hyper::server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::database::Database;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::manager::Manager;

mod error;
mod extractors;

pub struct UserApiConfig {
    pub manager: Arc<Manager>,
}

struct UserApiState {
    manager: Arc<Manager>,
}

pub async fn run_user_api<I>(config: UserApiConfig, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = UserApiState {
        manager: config.manager,
    };

    let app = Router::new()
        .route("/v2/pipeline", post(handle_hrana_pipeline))
        .with_state(Arc::new(state));

    axum::Server::builder(listener)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn handle_hrana_pipeline(db: Database, Json(req): Json<PipelineRequestBody>) -> Json<PipelineResponseBody> {
    let resp = db.hrana_pipeline(req).await;
    dbg!();
    Json(resp.unwrap())
}
