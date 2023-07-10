use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum UserApiError {
    #[error("missing host header")]
    MissingHost,
    #[error("invalid host header format")]
    InvalidHost,
    #[error("Database `{0}` doesn't exist")]
    UnknownDatabase(String),
}

impl UserApiError {
    fn http_status(&self) -> StatusCode {
        match self {
            UserApiError::MissingHost
            | UserApiError::InvalidHost
            | UserApiError::UnknownDatabase(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    error: String,
}

impl IntoResponse for UserApiError {
    fn into_response(self) -> axum::response::Response {
        let mut resp = Json(ApiError {
            error: self.to_string(),
        })
        .into_response();
        *resp.status_mut() = self.http_status();

        resp
    }
}
