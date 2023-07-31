use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use color_eyre::Result;
use hyper::server::accept::Accept;
use hyper::StatusCode;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::database::Database;
use crate::hrana;
use crate::hrana::error::HranaError;
use crate::hrana::http::proto::{PipelineRequestBody, PipelineResponseBody};
use crate::linc::bus::Bus;
use crate::manager::Manager;

mod error;
mod extractors;

#[derive(Debug, Serialize)]
struct ErrorResponseBody {
    pub message: String,
    pub code: String,
}

impl IntoResponse for HranaError {
    fn into_response(self) -> axum::response::Response {
        let (message, code) = match self.code() {
            Some(code) => (self.to_string(), code.to_owned()),
            None => (
                "internal error, please check the logs".to_owned(),
                "INTERNAL_ERROR".to_owned(),
            ),
        };
        let resp = ErrorResponseBody { message, code };
        let mut resp = Json(resp).into_response();
        *resp.status_mut() = StatusCode::BAD_REQUEST;
        resp
    }
}

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
) -> crate::Result<Json<PipelineResponseBody>, HranaError> {
    let ret = hrana::http::handle_pipeline(&state.hrana_server, req, db).await?;
    Ok(Json(ret))
}
