use std::sync::Arc;

use axum::async_trait;
use axum::extract::FromRequestParts;
use hyper::http::request::Parts;

use crate::{database::Database, meta::DatabaseId};

use super::{error::UserApiError, UserApiState};

#[async_trait]
impl FromRequestParts<Arc<UserApiState>> for Database {
    type Rejection = UserApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<UserApiState>,
    ) -> Result<Self, Self::Rejection> {
        let Some(host) = parts.headers.get("host") else { return Err(UserApiError::MissingHost) };
        let Ok(host_str) = std::str::from_utf8(host.as_bytes()) else {return Err(UserApiError::MissingHost)};
        let db_name = parse_host(host_str)?;
        let db_id = DatabaseId::from_name(db_name);
        let Some(sender) = state.manager.alloc(db_id, state.bus.clone()).await else { return Err(UserApiError::UnknownDatabase(db_name.to_owned())) };

        Ok(Database { sender })
    }
}

fn parse_host(host: &str) -> Result<&str, UserApiError> {
    let mut split = host.split(".");
    let Some(db_id) = split.next() else { return Err(UserApiError::InvalidHost) };
    Ok(db_id)
}
