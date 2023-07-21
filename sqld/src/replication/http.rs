use crate::replication::LogReadError;
use crate::replication::{frame::Frame, primary::frame_stream::FrameStream, ReplicationLogger};
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

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FramesRequest {
    pub next_offset: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Frames {
    pub frames: Vec<Frame>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Hello {
    pub generation_id: uuid::Uuid,
    pub generation_start_index: u64,
    pub database_id: uuid::Uuid,
}

pub(crate) async fn run(
    auth: Arc<Auth>,
    addr: SocketAddr,
    logger: Arc<ReplicationLogger>,
) -> Result<()> {
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
            let logger = logger.clone();
            handle_request(auth, req, logger)
        });

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server = hyper::server::Server::builder(AddrIncoming::from_listener(listener)?)
        .tcp_nodelay(true)
        .serve(tower::make::Shared::new(service));

    server.await.context("Http server exited with an error")?;

    Ok(())
}

async fn handle_request(
    auth: Arc<Auth>,
    req: Request<Body>,
    logger: Arc<ReplicationLogger>,
) -> Result<Response<Body>> {
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
        (&Method::GET, "/hello") => handle_hello(logger).await,
        (&Method::POST, "/frames") => handle_query(req, auth, logger).await,
        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
    }
}

impl Frames {
    pub fn new() -> Self {
        Self { frames: Vec::new() }
    }

    pub fn push(&mut self, frame: Frame) {
        self.frames.push(frame);
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

fn error(msg: &str, code: hyper::StatusCode) -> Response<Body> {
    let err = serde_json::json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

async fn handle_hello(logger: Arc<ReplicationLogger>) -> Result<Response<Body>> {
    let hello = Hello {
        generation_id: logger.generation.id,
        generation_start_index: logger.generation.start_index,
        database_id: logger.database_id()?,
    };

    let resp = Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from(serde_json::to_vec(&hello)?))
        .unwrap();
    Ok(resp)
}

async fn handle_query(
    mut req: Request<Body>,
    _auth: crate::auth::Authenticated,
    logger: Arc<ReplicationLogger>,
) -> Result<Response<Body>> {
    const MAX_FRAMES_IN_SINGLE_RESPONSE: usize = 256;

    let bytes = hyper::body::to_bytes(req.body_mut()).await?;
    let FramesRequest { next_offset } = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(resp) => return Ok(error(&resp.to_string(), hyper::StatusCode::BAD_REQUEST)),
    };
    tracing::trace!("Requested next offset: {next_offset}");

    let next_offset = std::cmp::max(next_offset, 1); // Frames start from 1
    let current_frameno = next_offset - 1;
    let mut frame_stream = FrameStream::new(logger.clone(), current_frameno);
    tracing::trace!(
        "Max available frame_no: {}",
        frame_stream.max_available_frame_no
    );
    if frame_stream.max_available_frame_no < next_offset {
        tracing::trace!("No frames available starting {next_offset}, returning 204 No Content");
        return Ok(Response::builder()
            .status(hyper::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap());
    }

    let mut frames = Frames::new();
    for _ in 0..MAX_FRAMES_IN_SINGLE_RESPONSE {
        use futures::StreamExt;

        match frame_stream.next().await {
            Some(Ok(frame)) => {
                tracing::trace!("Read frame {}", frame_stream.current_frame_no);
                frames.push(frame);
            }
            Some(Err(LogReadError::SnapshotRequired)) => {
                drop(frame_stream);
                if frames.is_empty() {
                    tracing::debug!("Snapshot required, switching to snapshot mode");
                    frames = load_snapshot(logger, next_offset)?;
                } else {
                    tracing::debug!("Snapshot required, but some frames were read - returning.");
                }
                break;
            }
            Some(Err(e)) => {
                tracing::error!("Error reading frame: {}", e);
                return Ok(Response::builder()
                    .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap());
            }
            None => break,
        }

        if frame_stream.max_available_frame_no <= frame_stream.current_frame_no {
            break;
        }
    }

    if frames.is_empty() {
        return Ok(Response::builder()
            .status(hyper::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap());
    }

    Ok(Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from(serde_json::to_string(&frames)?))
        .unwrap())
}

// FIXME: In the HTTP stateless spirit, we just unconditionally send the whole snapshot
// here, which is an obvious overcommit. We should instead stream in smaller parts
// if the snapshot is large.
fn load_snapshot(logger: Arc<ReplicationLogger>, from: u64) -> Result<Frames> {
    let snapshot = match logger.get_snapshot_file(from) {
        Ok(Some(snapshot)) => snapshot,
        _ => {
            tracing::trace!("No snapshot available, returning no frames");
            return Ok(Frames { frames: Vec::new() });
        }
    };
    let mut frames = Frames::new();
    for bytes in snapshot.frames_iter_from(from) {
        frames.push(Frame::try_from_bytes(bytes?)?);
    }
    tracing::trace!(
        "Loaded {} frames from the snapshot file",
        frames.frames.len()
    );
    Ok(frames)
}
