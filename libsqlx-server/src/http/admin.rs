use std::sync::Arc;

use axum::Router;
use color_eyre::eyre::Result;
use hyper::server::accept::Accept;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct AdminServerConfig { }

struct AdminServerState { }

pub async fn run_admin_server<I>(_config: AdminServerConfig, listener: I) -> Result<()>
where I: Accept<Error = std::io::Error>,
      I::Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let state = AdminServerState { };
    let app = Router::new().with_state(Arc::new(state));
    axum::Server::builder(listener).serve(app.into_make_service()).await?;

    Ok(())
}
