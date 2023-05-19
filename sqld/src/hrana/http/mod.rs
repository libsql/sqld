use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::Authenticated;
use crate::database::factory::DbFactory;
use super::ProtocolError;

mod proto;
mod request;
mod stream;

pub struct Server {
    db_factory: Arc<dyn DbFactory>,
    streams: Mutex<HashMap<u64, stream::Handle>>,
    baton_key: [u8; 32],
}

// TODO: release streams after 10 seconds of inactivity

#[derive(Debug)]
pub enum Route {
    GetIndex,
    PostPipeline,
}

impl Server {
    pub fn new(db_factory: Arc<dyn DbFactory>) -> Self {
        Self {
            db_factory,
            streams: Mutex::new(HashMap::new()),
            baton_key: rand::random(),
        }
    }

    pub async fn handle(
        &self,
        auth: Authenticated,
        route: Route,
        req: hyper::Request<hyper::Body>,
    ) -> Result<hyper::Response<hyper::Body>> {
        let res = match route {
            Route::GetIndex => handle_index(),
            Route::PostPipeline => handle_pipeline(self, auth, req).await,
        };
        let resp = match res {
            Ok(resp) => resp,
            Err(err) => {
                let proto_err = err.downcast::<ProtocolError>()?;
                hyper::Response::builder()
                    .status(hyper::StatusCode::BAD_REQUEST)
                    .header(hyper::http::header::CONTENT_TYPE, "text/plain")
                    .body(hyper::Body::from(proto_err.to_string()))
                    .unwrap()
            },
        };
        Ok(resp)
    }
}

fn handle_index() -> Result<hyper::Response<hyper::Body>> {
    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::http::header::CONTENT_TYPE, "text/plain")
        .body(hyper::Body::from("Hello, this is HTTP API v2 (Hrana over HTTP)"))
        .unwrap())
}

async fn handle_pipeline(
    server: &Server,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    let req_body: proto::PipelineRequestBody = read_request_json(req).await?;
    let stream_guard = stream::acquire(server, req_body.baton.as_deref()).await?;

    let mut results = Vec::with_capacity(req_body.requests.len());
    for request in req_body.requests.into_iter() {
        let result = request::execute(&stream_guard, auth, request).await
            .context("Could not execute a request in pipeline")?;
        results.push(result);
    }

    let resp_body = proto::PipelineResponseBody {
        baton: stream_guard.release(),
        base_url: None, // TODO: take the base_url from a command-line argument
        results,
    };
    Ok(json_response(&resp_body))
}

async fn read_request_json<T: DeserializeOwned>(req: hyper::Request<hyper::Body>) -> Result<T> {
    let req_body = hyper::body::to_bytes(req.into_body()).await
        .context("Could not read request body")?;
    let req_body = serde_json::from_slice(&req_body)
        .map_err(|err| ProtocolError::Deserialize { source: err })
        .context("Could not deserialize JSON request body")?;
    Ok(req_body)
}

fn json_response<T: Serialize>(resp_body: &T) -> hyper::Response<hyper::Body> {
    let resp_body = serde_json::to_vec(resp_body).unwrap();
    hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::http::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(resp_body))
        .unwrap()
}
