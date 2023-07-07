use std::path::PathBuf;

use color_eyre::eyre::Result;
use http::admin::{run_admin_server, AdminServerConfig};
use hyper::server::conn::AddrIncoming;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;

mod allocation;
mod databases;
mod http;
mod manager;
mod meta;

#[tokio::main]
async fn main() -> Result<()> {
    init();

    let admin_api_listener = tokio::net::TcpListener::bind("0.0.0.0:3456").await?;
    run_admin_server(
        AdminServerConfig {
            db_path: PathBuf::from("database"),
        },
        AddrIncoming::from_listener(admin_api_listener)?,
    )
    .await
    .unwrap();

    Ok(())
}

fn init() {
    let registry = tracing_subscriber::registry();

    registry
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_filter(
                    tracing_subscriber::EnvFilter::builder()
                        .with_default_directive(LevelFilter::INFO.into())
                        .from_env_lossy(),
                ),
        )
        .init();

    color_eyre::install().unwrap();
}
