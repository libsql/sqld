use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use super::ProtocolError;
use crate::auth::Authenticated;
use crate::database::factory::DbFactory;

mod proto;
mod request;
mod stream;

pub struct Server {
    db_factory: Arc<dyn DbFactory>,
    baton_key: [u8; 32],
    stream_state: Mutex<stream::ServerStreamState>,
}

#[derive(Debug)]
pub enum Route {
    GetIndex,
    PostPipeline,
}

impl Server {
    pub fn new(db_factory: Arc<dyn DbFactory>) -> Self {
        Self {
            db_factory,
            baton_key: rand::random(),
            stream_state: Mutex::new(stream::ServerStreamState::new()),
        }
    }

    pub async fn run_expire(&self) {
        stream::run_expire(self).await
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
        res.or_else(|err| {
            err.downcast::<stream::StreamError>()
                .map(stream_error_response)
        })
        .or_else(|err| err.downcast::<ProtocolError>().map(protocol_error_response))
    }
}

fn handle_index() -> Result<hyper::Response<hyper::Body>> {
    Ok(text_response(
        hyper::StatusCode::OK,
        "Hello, this is HTTP API v2 (Hrana over HTTP)".into(),
    ))
}

async fn handle_pipeline(
    server: &Server,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    let req_body: proto::PipelineRequestBody = read_request_json(req).await?;
    let mut stream_guard = stream::acquire(server, req_body.baton.as_deref()).await?;

    let mut results = Vec::with_capacity(req_body.requests.len());
    for request in req_body.requests.into_iter() {
        let result = request::handle(&mut stream_guard, auth, request)
            .await
            .context("Could not execute a request in pipeline")?;
        results.push(result);
    }

    let resp_body = proto::PipelineResponseBody {
        baton: stream_guard.release(),
        base_url: None, // TODO: take the base_url from a command-line argument
        results,
    };
    Ok(json_response(hyper::StatusCode::OK, &resp_body))
}

async fn read_request_json<T: DeserializeOwned>(req: hyper::Request<hyper::Body>) -> Result<T> {
    let req_body = hyper::body::to_bytes(req.into_body())
        .await
        .context("Could not read request body")?;
    let req_body = serde_json::from_slice(&req_body)
        .map_err(|err| ProtocolError::Deserialize { source: err })
        .context("Could not deserialize JSON request body")?;
    Ok(req_body)
}

fn protocol_error_response(err: ProtocolError) -> hyper::Response<hyper::Body> {
    text_response(hyper::StatusCode::BAD_REQUEST, err.to_string())
}

fn stream_error_response(err: stream::StreamError) -> hyper::Response<hyper::Body> {
    json_response(
        hyper::StatusCode::INTERNAL_SERVER_ERROR,
        &proto::Error {
            message: err.to_string(),
            code: err.code().into(),
        },
    )
}

fn json_response<T: Serialize>(
    status: hyper::StatusCode,
    resp_body: &T,
) -> hyper::Response<hyper::Body> {
    let resp_body = serde_json::to_vec(resp_body).unwrap();
    hyper::Response::builder()
        .status(status)
        .header(hyper::http::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(resp_body))
        .unwrap()
}

fn text_response(status: hyper::StatusCode, resp_body: String) -> hyper::Response<hyper::Body> {
    hyper::Response::builder()
        .status(status)
        .header(hyper::http::header::CONTENT_TYPE, "text/plain")
        .body(hyper::Body::from(resp_body))
        .unwrap()
}
