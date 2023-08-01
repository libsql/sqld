mod hrana_over_http_1;
mod result_builder;
pub mod stats;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::{FromRef, FromRequestParts, State as AxumState};
use axum::http::request::Parts;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use bytes::Bytes;
use hyper::server::conn::AddrIncoming;
use hyper::{Body, Request, Response, StatusCode};
use serde::Serialize;
use serde_json::Number;
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::http;
use tower_http::trace::DefaultOnResponse;
use tower_http::{compression::CompressionLayer, cors};
use tracing::{Level, Span};

use crate::auth::{Auth, Authenticated};
use crate::database::factory::DbFactory;
use crate::database::Database;
use crate::error::Error;
use crate::hrana;
use crate::http::types::HttpQuery;
use crate::query::{self, Query};
use crate::query_analysis::{predict_final_state, State, Statement};
use crate::query_result_builder::QueryResultBuilder;
use crate::stats::Stats;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;
use crate::version;

use self::result_builder::JsonHttpPayloadBuilder;
use self::types::QueryObject;

impl TryFrom<query::Value> for serde_json::Value {
    type Error = Error;

    fn try_from(value: query::Value) -> Result<Self, Self::Error> {
        let value = match value {
            query::Value::Null => serde_json::Value::Null,
            query::Value::Integer(i) => serde_json::Value::Number(Number::from(i)),
            query::Value::Real(x) => {
                serde_json::Value::Number(Number::from_f64(x).ok_or_else(|| {
                    Error::DbValueError(format!(
                        "Cannot to convert database value `{x}` to a JSON number"
                    ))
                })?)
            }
            query::Value::Text(s) => serde_json::Value::String(s),
            query::Value::Blob(v) => serde_json::json!({
                "base64": BASE64_STANDARD_NO_PAD.encode(v),
            }),
        };

        Ok(value)
    }
}

/// Encodes a query response rows into json
#[derive(Debug, Serialize)]
struct RowsResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

fn error(msg: &str, code: StatusCode) -> Response<Body> {
    let err = serde_json::json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

fn parse_queries(queries: Vec<QueryObject>) -> anyhow::Result<Vec<Query>> {
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut iter = Statement::parse(&query.q);
        let stmt = iter.next().transpose()?.unwrap_or_default();
        if iter.next().is_some() {
            anyhow::bail!(
                "found more than one command in a single statement string. It is allowed to issue only one command per string."
            );
        }
        let query = Query {
            stmt,
            params: query.params.0,
            want_rows: true,
        };

        out.push(query);
    }

    match predict_final_state(State::Init, out.iter().map(|q| &q.stmt)) {
        State::Txn => anyhow::bail!("interactive transaction not allowed in HTTP queries"),
        State::Init => (),
        // maybe we should err here, but let's sqlite deal with that.
        State::Invalid => (),
    }

    Ok(out)
}

fn parse_payload(data: &[u8]) -> Result<HttpQuery, Response<Body>> {
    match serde_json::from_slice(data) {
        Ok(data) => Ok(data),
        Err(e) => Err(error(&e.to_string(), http::status::StatusCode::BAD_REQUEST)),
    }
}

async fn handle_query<D: Database>(
    auth: Authenticated,
    AxumState(state): AxumState<AppState<D>>,
    body: Bytes,
) -> Response<Body> {
    let AppState { db_factory, .. } = state;

    let req = match parse_payload(&body) {
        Ok(req) => req,
        Err(resp) => return resp,
    };

    let batch = match parse_queries(req.statements) {
        Ok(queries) => queries,
        Err(e) => return error(&e.to_string(), StatusCode::BAD_REQUEST),
    };

    // TODO(lucio): convert this error into a status
    let db = db_factory.create().await.unwrap();

    let builder = JsonHttpPayloadBuilder::new();
    match db.execute_batch_or_rollback(batch, auth, builder).await {
        // TODO(lucio): convert these into axum responses
        Ok((builder, _)) => Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from(builder.into_ret()))
            .unwrap(),
        Err(e) => error(
            &format!("internal error: {e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn show_console<D>(
    AxumState(AppState { enable_console, .. }): AxumState<AppState<D>>,
) -> impl IntoResponse {
    if enable_console {
        Html(std::include_str!("console.html")).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn handle_health() -> Response<Body> {
    // return empty OK
    Response::new(Body::empty())
}

async fn handle_upgrade<D>(
    AxumState(AppState { upgrade_tx, .. }): AxumState<AppState<D>>,
    req: Request<Body>,
) -> impl IntoResponse {
    if !hyper_tungstenite::is_upgrade_request(&req) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let (response_tx, response_rx) = oneshot::channel();
    let _: Result<_, _> = upgrade_tx
        .send(hrana::ws::Upgrade {
            request: req,
            response_tx,
        })
        .await;

    match response_rx.await {
        Ok(response) => response.into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "sqld was not able to process the HTTP upgrade",
        )
            .into_response(),
    }
}

async fn handle_version() -> Response<Body> {
    let version = version::version();
    Response::new(Body::from(version))
}

async fn handle_hrana_v2<D: Database>(
    AxumState(state): AxumState<AppState<D>>,
    auth: Authenticated,
    req: Request<Body>,
) -> Response<Body> {
    let server = state.hrana_http_srv;

    // TODO(lucio): handle error
    server
        .handle(auth, crate::hrana::http::Route::PostPipeline, req)
        .await
        .unwrap()
}

async fn handle_fallback() -> impl IntoResponse {
    (StatusCode::NOT_FOUND).into_response()
}

pub(crate) struct AppState<D> {
    auth: Arc<Auth>,
    db_factory: Arc<dyn DbFactory<Db = D>>,
    upgrade_tx: mpsc::Sender<hrana::ws::Upgrade>,
    hrana_http_srv: Arc<hrana::http::Server<D>>,
    enable_console: bool,
    stats: Stats,
}

impl<D> Clone for AppState<D> {
    fn clone(&self) -> Self {
        Self {
            auth: self.auth.clone(),
            db_factory: self.db_factory.clone(),
            upgrade_tx: self.upgrade_tx.clone(),
            hrana_http_srv: self.hrana_http_srv.clone(),
            enable_console: self.enable_console,
            stats: self.stats.clone(),
        }
    }
}

// TODO: refactor
#[allow(clippy::too_many_arguments)]
pub async fn run_http<D: Database>(
    addr: SocketAddr,
    auth: Arc<Auth>,
    db_factory: Arc<dyn DbFactory<Db = D>>,
    upgrade_tx: mpsc::Sender<hrana::ws::Upgrade>,
    hrana_http_srv: Arc<hrana::http::Server<D>>,
    enable_console: bool,
    _idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
) -> anyhow::Result<()> {
    let state = AppState {
        auth,
        db_factory,
        upgrade_tx,
        hrana_http_srv,
        enable_console,
        stats,
    };

    tracing::info!("listening for HTTP requests on {addr}");

    fn trace_request<B>(req: &Request<B>, _span: &Span) {
        tracing::debug!("got request: {} {}", req.method(), req.uri());
    }

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/", get(handle_upgrade))
        .route("/version", get(handle_version))
        .route("/console", get(show_console))
        .route("/health", get(handle_health))
        .route("/v1/stats", get(stats::handle_stats))
        .route("/v1/", get(hrana_over_http_1::handle_index))
        .route("/v1/execute", post(hrana_over_http_1::handle_execute))
        .route("/v1/batch", post(hrana_over_http_1::handle_batch))
        .route("/v2", get(crate::hrana::http::handle_index))
        .route("/v2/pipeline", post(handle_hrana_v2))
        .fallback(handle_fallback)
        .with_state(state);

    let layered_app = app
        // TODO: error mismatch needs to be fixed
        // why? the option layer from tower merges the errors by boxing them
        // and this breaks how axum uses infalliable. Though the option layer
        // and any surrounding layers here don't really use the error part of tower
        // we can write an `InfalliableEither` which we can combine with an
        // `infalliable_option_layer` to correctly get the types to work.
        // Eventually, we can upstream this into axum as well.
        // .layer(option_layer(idle_shutdown_layer))
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
        );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server = hyper::server::Server::builder(AddrIncoming::from_listener(listener)?)
        .tcp_nodelay(true)
        .serve(layered_app.into_make_service());

    server.await.context("Http server exited with an error")?;

    Ok(())
}

/// Axum authenticated extractor
#[tonic::async_trait]
impl<S> FromRequestParts<S> for Authenticated
where
    Arc<Auth>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = axum::response::Response<String>;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth = <Arc<Auth> as FromRef<S>>::from_ref(state);

        let auth_header = parts.headers.get(hyper::header::AUTHORIZATION);
        let auth = match auth.authenticate_http(auth_header) {
            Ok(auth) => auth,
            Err(err) => {
                return Err(Response::builder()
                    .status(hyper::StatusCode::UNAUTHORIZED)
                    .body(err.to_string())
                    .unwrap());
            }
        };

        Ok(auth)
    }
}

impl<D> FromRef<AppState<D>> for Arc<Auth> {
    fn from_ref(input: &AppState<D>) -> Self {
        input.auth.clone()
    }
}
