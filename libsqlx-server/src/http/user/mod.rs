use std::sync::Arc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use color_eyre::Result;
use hyper::server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::database::Database;
use crate::hrana;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Bus;
use crate::manager::Manager;

mod error;
mod extractors;

pub struct Config {
    pub manager: Arc<Manager>,
    pub bus: Arc<Bus<Arc<Manager>>>,
    pub hrana_server: Arc<hrana::http::Server>,
}

struct UserApiState {
    manager: Arc<Manager>,
    bus: Arc<Bus<Arc<Manager>>>,
    hrana_server: Arc<hrana::http::Server>,
}

pub async fn run_user_api<I>(config: Config, listener: I) -> Result<()>
where
    I: Accept<Error = std::io::Error>,
    I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = UserApiState {
        manager: config.manager,
        bus: config.bus,
        hrana_server: config.hrana_server,
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
    State(state): State<Arc<UserApiState>>,
    db: Database,
    Json(req): Json<PipelineRequestBody>,
) -> Json<PipelineResponseBody> {
    let ret = hrana::http::handle_pipeline(&state.hrana_server, req, db).await.unwrap();
    Json(ret)
}
