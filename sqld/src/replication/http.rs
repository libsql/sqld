use crate::Auth;
use anyhow::{Context, Result};
use hyper::server::conn::AddrIncoming;
use hyper::{Body, Method, Request, Response};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::trace::DefaultOnResponse;
use tower_http::{compression::CompressionLayer, cors};
use tracing::{Level, Span};

pub(crate) async fn run(auth: Arc<Auth>, addr: SocketAddr) -> Result<()> {
    tracing::info!("listening for HTTP requests on {addr}");

    fn trace_request<B>(req: &Request<B>, _span: &Span) {
        tracing::debug!("got request: {} {}", req.method(), req.uri());
    }
    let service = ServiceBuilder::new()
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        )
        .layer(CompressionLayer::new())
        .layer(
            cors::CorsLayer::new()
                .allow_methods(cors::AllowMethods::any())
                .allow_headers(cors::Any)
                .allow_origin(cors::Any),
        )
        .service_fn(move |req| {
            let auth = auth.clone();
            handle_request(auth, req)
        });

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server = hyper::server::Server::builder(AddrIncoming::from_listener(listener)?)
        .tcp_nodelay(true)
        .serve(tower::make::Shared::new(service));

    server.await.context("Http server exited with an error")?;

    Ok(())
}

async fn handle_request(auth: Arc<Auth>, req: Request<Body>) -> Result<Response<Body>> {
    let auth_header = req.headers().get(hyper::header::AUTHORIZATION);
    let auth = match auth.authenticate_http(auth_header) {
        Ok(auth) => auth,
        Err(err) => {
            return Ok(Response::builder()
                .status(hyper::StatusCode::UNAUTHORIZED)
                .body(err.to_string().into())
                .unwrap());
        }
    };

    match (req.method(), req.uri().path()) {
        (&Method::POST, "/frames") => handle_query(req, auth).await,
        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FramesRequest {
    pub next_offset: u64,
}

fn error(msg: &str, code: hyper::StatusCode) -> Response<Body> {
    let err = serde_json::json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

async fn handle_query(
    mut req: Request<Body>,
    _auth: crate::auth::Authenticated,
) -> Result<Response<Body>> {
    let bytes = hyper::body::to_bytes(req.body_mut()).await?;
    let FramesRequest { next_offset } = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(resp) => return Ok(error(&resp.to_string(), hyper::StatusCode::BAD_REQUEST)),
    };

    Ok(Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from(format!(
            "{{\"comment\":\"thx for sending the request\", \"next_offset\":{}}}",
            next_offset + 1
        )))
        .unwrap())
}
