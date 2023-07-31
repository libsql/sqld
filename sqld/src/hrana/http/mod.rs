use std::sync::Arc;

use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};

use super::{Encoding, ProtocolError, Version};
use crate::auth::Authenticated;
use crate::connection::{Connection, MakeConnection};
mod proto;
mod protobuf;
mod request;
mod stream;

pub struct Server<C> {
    self_url: Option<String>,
    baton_key: [u8; 32],
    stream_state: Mutex<stream::ServerStreamState<C>>,
}

#[derive(Debug, Copy, Clone)]
pub enum Endpoint {
    Pipeline,
    Cursor,
}

impl<C: Connection> Server<C> {
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

    pub async fn handle_request(
        &self,
        auth: Authenticated,
        req: hyper::Request<hyper::Body>,
        endpoint: Endpoint,
        version: Version,
        encoding: Encoding,
    ) -> Result<hyper::Response<hyper::Body>> {
        handle_request(self, auth, req, endpoint, version, encoding)
            .await
            .or_else(|err| {
                err.downcast::<stream::StreamError>()
                    .map(|err| stream_error_response(err, encoding))
            })
            .or_else(|err| err.downcast::<ProtocolError>().map(protocol_error_response))
    }
}

pub(crate) async fn handle_index() -> hyper::Response<hyper::Body> {
    text_response(
        hyper::StatusCode::OK,
        "Hello, this is HTTP API v2 (Hrana over HTTP)".into(),
    )
}

async fn handle_request<D: Database>(
    server: &Server<D>,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
    endpoint: Endpoint,
    version: Version,
    encoding: Encoding,
) -> Result<hyper::Response<hyper::Body>> {
    match endpoint {
        Endpoint::Pipeline => handle_pipeline(server, auth, req, version, encoding).await,
        Endpoint::Cursor => handle_cursor(server, auth, req, version, encoding).await,
    }
}

async fn handle_pipeline<D: Database>(
    server: &Server<D>,
    connection_maker: Arc<dyn MakeConnection<Connection = D>>,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
    version: Version,
    encoding: Encoding,
) -> Result<hyper::Response<hyper::Body>> {
    let req_body: proto::PipelineRequestBody = read_decode_request(req, encoding).await?;
    let mut stream_guard = stream::acquire(server, req_body.baton.as_deref()).await?;

    let mut results = Vec::with_capacity(req_body.requests.len());
    for request in req_body.requests.into_iter() {
        let result = request::handle(&mut stream_guard, auth, request, version)
            .await
            .context("Could not execute a request in pipeline")?;
        results.push(result);
    }

    let resp_body = proto::PipelineResponseBody {
        baton: stream_guard.release(),
        base_url: server.self_url.clone(),
        results,
    };
    Ok(encode_response(hyper::StatusCode::OK, &resp_body, encoding))
}

async fn handle_cursor<D: Database>(
    _server: &Server<D>,
    _auth: Authenticated,
    _req: hyper::Request<hyper::Body>,
    _version: Version,
    _encoding: Encoding,
) -> Result<hyper::Response<hyper::Body>> {
    bail!("Cursor over HTTP not implemented")
}

async fn read_decode_request<T: DeserializeOwned + prost::Message + Default>(
    req: hyper::Request<hyper::Body>,
    encoding: Encoding,
) -> Result<T> {
    let req_body = hyper::body::to_bytes(req.into_body())
        .await
        .context("Could not read request body")?;
    match encoding {
        Encoding::Json => serde_json::from_slice(&req_body)
            .map_err(|err| ProtocolError::JsonDeserialize { source: err })
            .context("Could not deserialize JSON request body"),
        Encoding::Protobuf => <T as prost::Message>::decode(req_body)
            .map_err(|err| ProtocolError::ProtobufDecode { source: err })
            .context("Could not decode Protobuf request body"),
    }
}

fn protocol_error_response(err: ProtocolError) -> hyper::Response<hyper::Body> {
    text_response(hyper::StatusCode::BAD_REQUEST, err.to_string())
}

fn stream_error_response(
    err: stream::StreamError,
    encoding: Encoding,
) -> hyper::Response<hyper::Body> {
    encode_response(
        hyper::StatusCode::INTERNAL_SERVER_ERROR,
        &proto::Error {
            message: err.to_string(),
            code: err.code().into(),
        },
        encoding,
    )
}

fn encode_response<T: Serialize + prost::Message>(
    status: hyper::StatusCode,
    resp_body: &T,
    encoding: Encoding,
) -> hyper::Response<hyper::Body> {
    let (resp_body, content_type) = match encoding {
        Encoding::Json => (serde_json::to_vec(resp_body).unwrap(), "application/json"),
        Encoding::Protobuf => (
            <T as prost::Message>::encode_to_vec(resp_body),
            "application/x-protobuf",
        ),
    };

    hyper::Response::builder()
        .status(status)
        .header(hyper::http::header::CONTENT_TYPE, content_type)
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
