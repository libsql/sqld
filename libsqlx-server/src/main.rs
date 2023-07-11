use std::fs::read_to_string;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use color_eyre::eyre::Result;
use config::{AdminApiConfig, UserApiConfig};
use http::admin::run_admin_api;
use http::user::run_user_api;
use hyper::server::conn::AddrIncoming;
use manager::Manager;
use meta::Store;
use tokio::task::JoinSet;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;

mod allocation;
mod config;
mod database;
mod hrana;
mod http;
mod linc;
mod manager;
mod meta;

#[derive(Debug, Parser)]
struct Args {
    /// Path to the node configuration file
    #[clap(long, short)]
    config: PathBuf,
}

async fn spawn_admin_api(
    set: &mut JoinSet<Result<()>>,
    config: &AdminApiConfig,
    meta_store: Arc<Store>,
) -> Result<()> {
    let admin_api_listener = tokio::net::TcpListener::bind(config.addr).await?;
    let fut = run_admin_api(
        http::admin::Config { meta_store },
        AddrIncoming::from_listener(admin_api_listener)?,
    );
    set.spawn(fut);

    Ok(())
}

async fn spawn_user_api(
    set: &mut JoinSet<Result<()>>,
    config: &UserApiConfig,
    manager: Arc<Manager>,
) -> Result<()> {
    let user_api_listener = tokio::net::TcpListener::bind(config.addr).await?;
    set.spawn(run_user_api(
        http::user::Config { manager },
        AddrIncoming::from_listener(user_api_listener)?,
    ));

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init();
    let args = Args::parse();
    let config_str = read_to_string(args.config)?;
    let config: config::Config = toml::from_str(&config_str)?;
    config.validate()?;

    let mut join_set = JoinSet::new();

    let store = Arc::new(Store::new(&config.db_path));
    let manager = Arc::new(Manager::new(config.db_path.clone(), store.clone(), 100));

    spawn_admin_api(&mut join_set, &config.admin_api, store.clone()).await?;
    spawn_user_api(&mut join_set, &config.user_api, manager).await?;

    join_set.join_next().await;

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
