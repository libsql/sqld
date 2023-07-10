use std::{path::PathBuf, sync::Arc};

use color_eyre::eyre::Result;
use http::{
    admin::{run_admin_api, AdminApiConfig},
    user::{run_user_api, UserApiConfig},
};
use hyper::server::conn::AddrIncoming;
use manager::Manager;
use meta::Store;
use tokio::task::JoinSet;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;

mod allocation;
mod database;
mod hrana;
mod http;
mod manager;
mod meta;
mod linc;

#[tokio::main]
async fn main() -> Result<()> {
    init();
    let mut join_set = JoinSet::new();

    let db_path = PathBuf::from("database");
    let store = Arc::new(Store::new(&db_path));
    let admin_api_listener = tokio::net::TcpListener::bind("0.0.0.0:3456").await?;
    join_set.spawn(run_admin_api(
        AdminApiConfig {
            meta_store: store.clone(),
        },
        AddrIncoming::from_listener(admin_api_listener)?,
    ));

    let manager = Arc::new(Manager::new(db_path.clone(), store, 100));
    let user_api_listener = tokio::net::TcpListener::bind("0.0.0.0:3457").await?;
    join_set.spawn(run_user_api(
        UserApiConfig { manager },
        AddrIncoming::from_listener(user_api_listener)?,
    ));

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
