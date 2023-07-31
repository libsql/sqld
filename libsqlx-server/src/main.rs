use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use compactor::{run_compactor_loop, CompactionQueue};
use config::{AdminApiConfig, ClusterConfig, UserApiConfig};
use http::admin::run_admin_api;
use http::user::run_user_api;
use hyper::server::conn::AddrIncoming;
use linc::bus::Bus;
use manager::Manager;
use meta::Store;
use replica_commit_store::ReplicaCommitStore;
use snapshot_store::SnapshotStore;
use tokio::fs::create_dir_all;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;

mod allocation;
mod compactor;
mod config;
mod database;
mod error;
mod hrana;
mod http;
mod linc;
mod manager;
mod meta;
mod replica_commit_store;
mod snapshot_store;

pub type Result<T, E = error::Error> = std::result::Result<T, E>;

#[derive(Debug, Parser)]
struct Args {
    /// Path to the node configuration file
    #[clap(long, short)]
    config: PathBuf,
}

async fn spawn_admin_api(
    set: &mut JoinSet<color_eyre::Result<()>>,
    config: &AdminApiConfig,
    bus: Arc<Bus<Arc<Manager>>>,
) -> color_eyre::Result<()> {
    let admin_api_listener = TcpListener::bind(config.addr).await?;
    let fut = run_admin_api(
        http::admin::Config { bus },
        AddrIncoming::from_listener(admin_api_listener)?,
    );
    set.spawn(fut);

    Ok(())
}

async fn spawn_user_api(
    set: &mut JoinSet<color_eyre::Result<()>>,
    config: &UserApiConfig,
    manager: Arc<Manager>,
    bus: Arc<Bus<Arc<Manager>>>,
) -> color_eyre::Result<()> {
    let user_api_listener = TcpListener::bind(config.addr).await?;
    let hrana_server = Arc::new(hrana::http::Server::new(None));
    set.spawn({
        let hrana_server = hrana_server.clone();
        async move {
            hrana_server.run_expire().await;
            Ok(())
        }
    });
    set.spawn(run_user_api(
        http::user::Config {
            manager,
            bus,
            hrana_server,
        },
        AddrIncoming::from_listener(user_api_listener)?,
    ));

    Ok(())
}

async fn spawn_cluster_networking(
    set: &mut JoinSet<color_eyre::Result<()>>,
    config: &ClusterConfig,
    bus: Arc<Bus<Arc<Manager>>>,
) -> color_eyre::Result<()> {
    let server = linc::server::Server::new(bus.clone());

    let listener = TcpListener::bind(config.addr).await?;
    set.spawn(server.run(listener));

    let pool = linc::connection_pool::ConnectionPool::new(
        bus,
        config.peers.iter().map(|p| (p.id, p.addr.clone())),
    );
    if pool.managed_count() > 0 {
        set.spawn(pool.run::<TcpStream>());
    }

    Ok(())
}

async fn init_dirs(db_path: &Path) -> color_eyre::Result<()> {
    create_dir_all(&db_path).await?;
    create_dir_all(db_path.join("tmp")).await?;
    create_dir_all(db_path.join("snapshot_queue")).await?;
    create_dir_all(db_path.join("snapshots")).await?;
    create_dir_all(db_path.join("dbs")).await?;
    create_dir_all(db_path.join("meta")).await?;

    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> color_eyre::Result<()> {
    init()?;

    let args = Args::parse();
    let config_str = read_to_string(args.config)?;
    let config: config::Config = toml::from_str(&config_str)?;
    config.validate()?;

    let mut join_set = JoinSet::new();

    init_dirs(&config.db_path).await?;

    let env = heed::EnvOpenOptions::new()
        .max_dbs(1000)
        .map_size(100 * 1024 * 1024)
        .open(config.db_path.join("meta"))?;

    let snapshot_store = Arc::new(SnapshotStore::new(config.db_path.clone(), env.clone())?);
    let compaction_queue = Arc::new(CompactionQueue::new(
        env.clone(),
        config.db_path.clone(),
        snapshot_store,
    )?);
    let store = Arc::new(Store::new(env.clone())?);
    let replica_commit_store = Arc::new(ReplicaCommitStore::new(env.clone())?);
    let manager = Arc::new(Manager::new(
        config.db_path.clone(),
        store.clone(),
        100,
        compaction_queue.clone(),
        replica_commit_store,
    ));
    let bus = Arc::new(Bus::new(config.cluster.id, manager.clone()));

    join_set.spawn(run_compactor_loop(compaction_queue));
    spawn_cluster_networking(&mut join_set, &config.cluster, bus.clone()).await?;
    spawn_admin_api(&mut join_set, &config.admin_api, bus.clone()).await?;
    spawn_user_api(&mut join_set, &config.user_api, manager, bus).await?;

    join_set.join_next().await;

    Ok(())
}

fn init() -> color_eyre::Result<()> {
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

    color_eyre::install()?;

    Ok(())
}
