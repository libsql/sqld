use std::sync::Arc;

use axum::routing::post;
use axum::{Json, Router};
use color_eyre::Result;
use hyper::server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::database::Database;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Bus;
use crate::manager::Manager;

mod error;
mod extractors;

pub struct Config {
    pub manager: Arc<Manager>,
    pub bus: Arc<Bus<Arc<Manager>>>,
}

struct UserApiState {
    manager: Arc<Manager>,
    bus: Arc<Bus<Arc<Manager>>>,
}

pub async fn run_user_api<I>(config: Config, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = UserApiState {
        manager: config.manager,
        bus: config.bus,
    };

    let app = Router::new()
        .route("/v2/pipeline", post(handle_hrana_pipeline))
        .with_state(Arc::new(state));

    axum::Server::builder(listener)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn handle_hrana_pipeline(
    db: Database,
    Json(req): Json<PipelineRequestBody>,
) -> Json<PipelineResponseBody> {
    let resp = db.hrana_pipeline(req).await;
    Json(resp.unwrap())
}
