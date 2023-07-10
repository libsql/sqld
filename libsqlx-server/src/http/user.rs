use std::sync::Arc;

use axum::{async_trait, extract::FromRequestParts, response::IntoResponse, routing::get, Router, Json};
use color_eyre::Result;
use hyper::{http::request::Parts, server::accept::Accept, StatusCode};
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};

use crate::{allocation::AllocationMessage, manager::Manager};

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
    let state = UserApiState { manager: config.manager };

    let app = Router::new()
        .route("/", get(test_database))
        .with_state(Arc::new(state));

    axum::Server::builder(listener)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

struct Database {
    sender: mpsc::Sender<AllocationMessage>,
}

#[derive(Debug, thiserror::Error)]
enum UserApiError {
    #[error("missing host header")]
    MissingHost,
    #[error("invalid host header format")]
    InvalidHost,
    #[error("Database `{0}` doesn't exist")]
    UnknownDatabase(String),
}

impl UserApiError  {
    fn http_status(&self) -> StatusCode {
        match self {
            UserApiError::MissingHost 
            | UserApiError::InvalidHost 
            | UserApiError::UnknownDatabase(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}

impl IntoResponse for UserApiError {
    fn into_response(self) -> axum::response::Response {
        let mut resp = Json(ApiError {
            error: self.to_string()
        }).into_response();
        *resp.status_mut() = self.http_status();

        resp
    }
}

#[async_trait]
impl FromRequestParts<Arc<UserApiState>> for Database {
    type Rejection = UserApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<UserApiState>,
    ) -> Result<Self, Self::Rejection> {
        let Some(host) = parts.headers.get("host") else { return Err(UserApiError::MissingHost) };
        let Ok(host_str) = std::str::from_utf8(host.as_bytes()) else {return Err(UserApiError::MissingHost)};
        let db_id = parse_host(host_str)?;
        let Some(sender) = state.manager.alloc(db_id).await else { return Err(UserApiError::UnknownDatabase(db_id.to_owned())) };

        Ok(Database { sender })
    }
}

fn parse_host(host: &str) -> Result<&str, UserApiError> {
    let mut split = host.split(".");
    let Some(db_id) = split.next() else { return Err(UserApiError::InvalidHost) };
    Ok(db_id)
}
