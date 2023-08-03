use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hyper::Uri;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tonic::transport::Channel;

use crate::database::Database;
use crate::database::config::DatabaseConfigStore;
use crate::database::dump::loader::DumpLoader;
use crate::database::libsql::{LibSqlDb, LibSqlDbFactory};
use crate::replication::primary::logger::{REPLICATION_METHODS, ReplicationLoggerHookCtx};
use crate::replication::{ReplicationLogger, SnapshotCallback};
use crate::stats::Stats;
use crate::{DB_CREATE_TIMEOUT, run_periodic_compactions, check_fresh_db, init_bottomless_replicator, MAX_CONCURRENT_DBS};
use crate::replication::replica::Replicator;
use crate::database::write_proxy::{WriteProxyDatabase, WriteProxyDbFactory};
use crate::database::factory::{DbFactory, TrackedDb};

#[async_trait::async_trait]
pub trait NamespaceFactory: Sync + Send + 'static {
    type Database: Database;
    type Meta: Send + Sync + 'static;

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database, Self::Meta>>;
}

pub struct PrimaryNamespaceFactory { 
    config: PrimaryNamespaceConfig,
}

impl PrimaryNamespaceFactory {
    pub fn new(config: PrimaryNamespaceConfig) -> Self {
        Self {
            config
        }
    }
}


#[async_trait::async_trait]
impl NamespaceFactory for PrimaryNamespaceFactory {
    type Database = TrackedDb<LibSqlDb>;
    type Meta = PrimaryMeta;

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database, Self::Meta>> {
        Namespace::new_primary(&self.config, name).await
    }
}

pub struct ReplicaNamespaceFactory {
    config: ReplicaNamespaceConfig,
}

impl ReplicaNamespaceFactory {
    pub fn new(config: ReplicaNamespaceConfig) -> Self { Self { config } }
}

#[async_trait::async_trait]
impl NamespaceFactory for ReplicaNamespaceFactory {
    type Database = TrackedDb<WriteProxyDatabase>;
    type Meta = ();

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database, Self::Meta>> {
        Namespace::new_replica(&self.config, name).await
    }
}

pub struct Namespaces<F: NamespaceFactory> {
    inner: Mutex<HashMap<Bytes, Namespace<F::Database, F::Meta>>>,
    factory: F,
}

impl<F: NamespaceFactory> Namespaces<F> {
    pub fn new(factory: F) -> Self {
        Self {
            inner: Default::default(),
            factory,
        }

    }

    pub async fn with<Fun, R>(&self, namespace: Bytes, f: Fun) -> anyhow::Result<R>
        where Fun: FnOnce(&Namespace<F::Database, F::Meta>) -> R,
    {
        let mut lock = self.inner.lock().await;
        match lock.entry(namespace.clone()) {
            Entry::Occupied(e) => {
                Ok(f(e.get()))
            },
            Entry::Vacant(e) => {
                let factory = self.factory.create(namespace).await?;
                Ok(f(e.insert(factory)))
            },
        }
    }
}

#[allow(dead_code)]
pub struct Namespace<D, T> {
    name: Bytes,
    pub db_factory: Arc<dyn DbFactory<Db = D>>,
    tasks: JoinSet<anyhow::Result<()>>,
    pub meta: T,
    path: PathBuf,
}

pub struct ReplicaNamespaceConfig {
    pub base_path: PathBuf,
    pub channel: Channel,
    pub uri: Uri,
    pub allow_replica_overwrite: bool,
    pub extensions: Vec<PathBuf>,
    pub stats: Stats,
    pub config_store: Arc<DatabaseConfigStore>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
}

#[async_trait::async_trait]
trait NamespaceCommon {
    async fn reset(&mut self) -> anyhow::Result<()>;
}

impl Namespace<TrackedDb<WriteProxyDatabase>, ()> {
    pub async fn new_replica(config: &ReplicaNamespaceConfig, name: Bytes) -> anyhow::Result<Self> {
        let name_str = std::str::from_utf8(&name)?;
        let db_path = config.base_path.join("dbs").join(name_str);
        tokio::fs::create_dir_all(&db_path).await?;
        let mut join_set = JoinSet::new();
        let replicator = Replicator::new(
            db_path.clone(),
            config.channel.clone(),
            config.uri.clone(),
            config.allow_replica_overwrite,
            name.clone(),
            &mut join_set,
        ).await?;

        let applied_frame_no_receiver = replicator.current_frame_no_notifier.clone();

        join_set.spawn(replicator.run());

        let db_factory = WriteProxyDbFactory::new(
            db_path.clone(),
            config.extensions.clone(),
            config.channel.clone(),
            config.uri.clone(),
            config.stats.clone(),
            config.config_store.clone(),
            applied_frame_no_receiver,
            config.max_response_size,
            config.max_total_response_size,
            name.clone(),
        )
            .throttled(MAX_CONCURRENT_DBS, Some(DB_CREATE_TIMEOUT), config.max_total_response_size);


        Ok(Self {
            name,
            db_factory: Arc::new(db_factory),
            tasks: join_set,
            meta: (),
            path: db_path,
        })
    }
}

#[async_trait::async_trait]
impl NamespaceCommon for Namespace<TrackedDb<WriteProxyDatabase>, ()> {
    async fn reset(&mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        tokio::fs::remove_dir_all(&self.path).await?;
        todo!()
    }
}

pub struct PrimaryNamespaceConfig {
    pub base_path: PathBuf,
    pub max_log_size: u64,
    pub db_is_dirty: bool,
    pub max_log_duration: Option<Duration>,
    pub snapshot_callback: SnapshotCallback,
    pub bottomless_replication: Option<bottomless::replicator::Options>,
    pub extensions: Vec<PathBuf>,
    pub stats: Stats,
    pub config_store: Arc<DatabaseConfigStore>,
    pub max_response_size: u64,
    pub load_from_dump: Option<PathBuf>,
    pub max_total_response_size: u64,
}

pub struct PrimaryMeta {
    pub logger: Arc<ReplicationLogger>,
}

impl Namespace<TrackedDb<LibSqlDb>, PrimaryMeta> {
    pub async fn new_primary(config: &PrimaryNamespaceConfig, name: Bytes) -> anyhow::Result<Self> {
        let mut join_set = JoinSet::new();
        let name_str = std::str::from_utf8(&name)?;
        let db_path = config.base_path.join("dbs").join(name_str);
        tokio::fs::create_dir_all(&db_path).await?;
        let is_fresh_db = check_fresh_db(&db_path);
        let logger = Arc::new(ReplicationLogger::open(
                &db_path,
                config.max_log_size,
                config.max_log_duration,
                config.db_is_dirty,
                Box::new({ 
                    let name = name.clone();
                    let cb = config.snapshot_callback.clone();
                    move |path| cb(path, &name)
                }),
        )?);

        join_set.spawn(run_periodic_compactions(logger.clone()));

        let bottomless_replicator = if let Some(options) = &config.bottomless_replication {
            Some(Arc::new(std::sync::Mutex::new(
                        init_bottomless_replicator(db_path.join("data"), options.clone()).await?,
            )))
        } else {
            None
        };

        // load dump is necessary
        let dump_loader = DumpLoader::new(
            db_path.clone(),
            logger.clone(),
            bottomless_replicator.clone(),
        )
            .await?;
        if let Some(ref path) = config.load_from_dump {
            if !is_fresh_db {
                anyhow::bail!("cannot load from a dump if a database already exists.\nIf you're sure you want to load from a dump, delete your database folder at `{}`", db_path.display());
            }
            dump_loader.load_dump(path.into()).await?;
        }

        let db_factory: Arc<_> = LibSqlDbFactory::new(
            db_path.clone(),
            &REPLICATION_METHODS,
            {
                let logger = logger.clone();
                let bottomless_replicator = bottomless_replicator.clone();
                move || ReplicationLoggerHookCtx::new(logger.clone(), bottomless_replicator.clone())
            },
            config.stats.clone(),
            config.config_store.clone(),
            config.extensions.clone(),
            config.max_response_size,
            config.max_total_response_size,
        )
            .await?
            .throttled(MAX_CONCURRENT_DBS, Some(DB_CREATE_TIMEOUT), config.max_total_response_size)
            .into();

        Ok(Self {
            name,
            db_factory,
            tasks: join_set,
            meta: PrimaryMeta { logger },
            path: db_path,
        })
    }

}
