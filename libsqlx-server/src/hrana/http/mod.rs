use parking_lot::Mutex;

use super::error::HranaError;
// use crate::auth::Authenticated;
use crate::database::Database;
pub use stream::StreamError;

pub mod proto;
pub mod request;
mod stream;

pub struct Server {
    self_url: Option<String>,
    baton_key: [u8; 32],
    stream_state: Mutex<stream::ServerStreamState>,
}

#[derive(Debug)]
pub enum Route {
    GetIndex,
    PostPipeline,
}

impl Server {
    pub fn new(self_url: Option<String>) -> Self {
        Self {
            self_url,
            baton_key: rand::random(),
            stream_state: Mutex::new(stream::ServerStreamState::new()),
        }
    }

    pub async fn run_expire(&self) {
        stream::run_expire(self).await
    }
}

pub async fn handle_pipeline(
    server: &Server,
    // auth: Authenticated,
    req: proto::PipelineRequestBody,
    db: Database,
) -> crate::Result<proto::PipelineResponseBody, HranaError>
{
    let mut stream_guard = stream::acquire(server, req.baton.as_deref(), db).await?;

    let mut results = Vec::with_capacity(req.requests.len());
    for request in req.requests.into_iter() {
        let result = request::handle(&mut stream_guard, /*auth,*/ request).await?;
        results.push(result);
    }

    Ok(proto::PipelineResponseBody {
        baton: stream_guard.release(),
        base_url: server.self_url.clone(),
        results,
    })
}
