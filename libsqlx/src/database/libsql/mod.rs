use std::path::PathBuf;
use std::sync::Arc;

use sqld_libsql_bindings::wal_hook::{TransparentMethods, WalHook, TRANSPARENT_METHODS};
use sqld_libsql_bindings::WalMethodsHook;

use crate::database::frame::Frame;
use crate::database::{Database, InjectError, InjectableDatabase};
use crate::error::Error;
use crate::result_builder::QueryBuilderConfig;

use connection::RowStats;
use injector::Injector;
use replication_log::logger::{
    ReplicationLogger, ReplicationLoggerHook, ReplicationLoggerHookCtx, REPLICATION_METHODS,
};

use self::injector::InjectorCommitHandler;

pub use connection::LibsqlConnection;
pub use replication_log::logger::{LogCompactor, LogFile};
pub use replication_log::merger::SnapshotMerger;

mod connection;
mod injector;
pub(crate) mod replication_log;

pub struct PrimaryType {
    logger: Arc<ReplicationLogger>,
}

impl LibsqlDbType for PrimaryType {
    type ConnectionHook = ReplicationLoggerHook;

    fn hook() -> &'static WalMethodsHook<Self::ConnectionHook> {
        &REPLICATION_METHODS
    }

    fn hook_context(&self) -> <Self::ConnectionHook as WalHook>::Context {
        ReplicationLoggerHookCtx {
            buffer: Vec::new(),
            logger: self.logger.clone(),
        }
    }
}

pub struct ReplicaType {
    commit_handler: Option<Box<dyn InjectorCommitHandler>>,
    injector_buffer_capacity: usize,
}

impl LibsqlDbType for ReplicaType {
    type ConnectionHook = TransparentMethods;

    fn hook() -> &'static WalMethodsHook<Self::ConnectionHook> {
        &TRANSPARENT_METHODS
    }

    fn hook_context(&self) -> <Self::ConnectionHook as WalHook>::Context {}
}

pub trait LibsqlDbType {
    type ConnectionHook: WalHook;

    /// Return a static reference to the instanciated WAL hook
    fn hook() -> &'static WalMethodsHook<Self::ConnectionHook>;
    /// returns new context for the wal hook
    fn hook_context(&self) -> <Self::ConnectionHook as WalHook>::Context;
}

pub struct PlainType;

impl LibsqlDbType for PlainType {
    type ConnectionHook = TransparentMethods;

    fn hook() -> &'static WalMethodsHook<Self::ConnectionHook> {
        &TRANSPARENT_METHODS
    }

    fn hook_context(&self) -> <Self::ConnectionHook as WalHook>::Context {}
}

/// A generic wrapper around a libsql database.
/// `LibsqlDatabase` can be specialized into either a `ReplicaType` or a `PrimaryType`.
/// In `PrimaryType` mode, the LibsqlDatabase maintains a replication log that can be replicated to
/// a `LibsqlDatabase` in replica mode, thanks to the methods provided by `InjectableDatabase`
/// implemented for `LibsqlDatabase<ReplicaType>`.
pub struct LibsqlDatabase<T> {
    /// The connection factory for this database
    db_path: PathBuf,
    extensions: Option<Arc<[PathBuf]>>,
    response_size_limit: u64,
    row_stats_callback: Option<Arc<dyn RowStatsHandler>>,
    /// type-specific data for the database
    ty: T,
}

/// Handler trait for gathering row stats when executing queries.
pub trait RowStatsHandler: Send + Sync {
    fn handle_row_stats(&self, stats: RowStats);
}

impl<F> RowStatsHandler for F
where
    F: Fn(RowStats) + Send + Sync,
{
    fn handle_row_stats(&self, stats: RowStats) {
        (self)(stats)
    }
}

impl LibsqlDatabase<ReplicaType> {
    /// Creates a new replica type database
    pub fn new_replica(
        db_path: PathBuf,
        injector_buffer_capacity: usize,
        injector_commit_handler: impl InjectorCommitHandler,
    ) -> crate::Result<Self> {
        let ty = ReplicaType {
            commit_handler: Some(Box::new(injector_commit_handler)),
            injector_buffer_capacity,
        };

        Ok(Self::new(db_path, ty))
    }
}

impl LibsqlDatabase<PlainType> {
    pub fn new_plain(db_path: PathBuf) -> crate::Result<Self> {
        Ok(Self::new(db_path, PlainType))
    }
}

impl LibsqlDatabase<PrimaryType> {
    pub fn new_primary(
        db_path: PathBuf,
        compactor: impl LogCompactor,
        // whether the log is dirty and might need repair
        dirty: bool,
    ) -> crate::Result<Self> {
        let ty = PrimaryType {
            logger: Arc::new(ReplicationLogger::open(
                &db_path,
                dirty,
                compactor,
                Box::new(|_| ()),
            )?),
        };
        Ok(Self::new(db_path, ty))
    }
}

impl<T> LibsqlDatabase<T> {
    /// Create a new instance with the passed `LibsqlDbType`.
    fn new(db_path: PathBuf, ty: T) -> Self {
        Self {
            db_path,
            extensions: None,
            response_size_limit: u64::MAX,
            row_stats_callback: None,
            ty,
        }
    }

    /// Load extensions for connection to this database.
    pub fn with_extensions(mut self, ext: impl IntoIterator<Item = PathBuf>) -> Self {
        self.extensions = Some(ext.into_iter().collect());
        self
    }

    /// Register a callback
    pub fn with_row_stats_handler(mut self, handler: Arc<dyn RowStatsHandler>) -> Self {
        self.row_stats_callback = Some(handler);
        self
    }
}

impl<T: LibsqlDbType> Database for LibsqlDatabase<T> {
    type Connection = LibsqlConnection<<T::ConnectionHook as WalHook>::Context>;

    fn connect(&self) -> Result<Self::Connection, Error> {
        Ok(
            LibsqlConnection::<<T::ConnectionHook as WalHook>::Context>::new(
                &self.db_path,
                self.extensions.clone(),
                T::hook(),
                self.ty.hook_context(),
                self.row_stats_callback.clone(),
                QueryBuilderConfig {
                    max_size: Some(self.response_size_limit),
                },
            )?,
        )
    }
}

impl InjectableDatabase for LibsqlDatabase<ReplicaType> {
    fn injector(&mut self) -> crate::Result<Box<dyn super::Injector>> {
        let Some(commit_handler) = self.ty.commit_handler.take() else { panic!("there can be only one injector") };
        Ok(Box::new(Injector::new(
            &self.db_path,
            commit_handler,
            self.ty.injector_buffer_capacity,
        )?))
    }
}

impl super::Injector for Injector {
    fn inject(&mut self, frame: Frame) -> Result<(), InjectError> {
        self.inject_frame(frame).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use rusqlite::types::Value;

    use crate::connection::Connection;
    use crate::database::libsql::replication_log::logger::LogFile;
    use crate::program::Program;
    use crate::result_builder::{QueryResultBuilderError, ResultBuilder};

    use super::*;

    struct ReadRowBuilder(Vec<rusqlite::types::Value>);

    impl ResultBuilder for ReadRowBuilder {
        fn add_row_value(
            &mut self,
            v: rusqlite::types::ValueRef,
        ) -> Result<(), QueryResultBuilderError> {
            self.0.push(v.into());
            Ok(())
        }
    }

    #[test]
    fn inject_libsql_db() {
        let temp = tempfile::tempdir().unwrap();
        let replica = ReplicaType {
            commit_handler: Some(Box::new(())),
            injector_buffer_capacity: 10,
        };
        let mut db = LibsqlDatabase::new(temp.path().to_path_buf(), replica);

        let mut conn = db.connect().unwrap();
        let mut builder = ReadRowBuilder(Vec::new());
        conn.execute_program(Program::seq(&["select count(*) from test"]), &mut builder)
            .unwrap();
        assert!(builder.0.is_empty());

        let file = File::open("assets/test/simple_wallog").unwrap();
        let log = LogFile::new(file).unwrap();
        let mut injector = db.injector().unwrap();
        log.frames_iter()
            .unwrap()
            .for_each(|f| injector.inject(f.unwrap()).unwrap());

        let mut builder = ReadRowBuilder(Vec::new());
        conn.execute_program(Program::seq(&["select count(*) from test"]), &mut builder)
            .unwrap();
        assert_eq!(builder.0[0], Value::Integer(5));
    }

    #[test]
    fn roundtrip_primary_replica() {
        let temp_primary = tempfile::tempdir().unwrap();
        let temp_replica = tempfile::tempdir().unwrap();

        let primary = LibsqlDatabase::new(
            temp_primary.path().to_path_buf(),
            PrimaryType {
                logger: Arc::new(ReplicationLogger::open(temp_primary.path(), false, ()).unwrap()),
            },
        );

        let mut replica = LibsqlDatabase::new(
            temp_replica.path().to_path_buf(),
            ReplicaType {
                commit_handler: Some(Box::new(())),
                injector_buffer_capacity: 10,
            },
        );

        let mut primary_conn = primary.connect().unwrap();
        primary_conn
            .execute_program(
                Program::seq(&["create table test (x)", "insert into test values (42)"]),
                &mut (),
            )
            .unwrap();

        let logfile = primary.ty.logger.log_file.read();

        let mut injector = replica.injector().unwrap();
        for frame in logfile.frames_iter().unwrap() {
            let frame = frame.unwrap();
            injector.inject(frame).unwrap();
        }

        let mut replica_conn = replica.connect().unwrap();
        let mut builder = ReadRowBuilder(Vec::new());
        replica_conn
            .execute_program(Program::seq(&["select * from test limit 1"]), &mut builder)
            .unwrap();

        assert_eq!(builder.0.len(), 1);
        assert_eq!(builder.0[0], Value::Integer(42));
    }

    #[test]
    fn primary_compact_log() {
        struct Compactor(Arc<AtomicBool>);

        impl LogCompactor for Compactor {
            fn should_compact(&self, log: &LogFile) -> bool {
                log.header().frame_count > 2
            }

            fn compact(
                &self,
                _file: LogFile,
                _path: PathBuf,
                _size_after: u32,
            ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
                self.0.store(true, Relaxed);
                Ok(())
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let compactor_called = Arc::new(AtomicBool::new(false));
        let db = LibsqlDatabase::new_primary(
            temp.path().to_path_buf(),
            Compactor(compactor_called.clone()),
            false,
        )
        .unwrap();

        let mut conn = db.connect().unwrap();
        conn.execute_program(
            Program::seq(&["create table test (x)", "insert into test values (12)"]),
            &mut (),
        )
        .unwrap();
        assert!(compactor_called.load(Relaxed));
    }

    #[test]
    fn no_compaction_uncommited_frames() {
        struct Compactor(Arc<AtomicBool>);

        impl LogCompactor for Compactor {
            fn should_compact(&self, log: &LogFile) -> bool {
                assert_eq!(log.uncommitted_frame_count, 0);
                self.0.store(true, Relaxed);
                false
            }

            fn compact(
                &self,
                _file: LogFile,
                _path: PathBuf,
                _size_after: u32,
            ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
                unreachable!()
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let compactor_called = Arc::new(AtomicBool::new(false));
        let db = LibsqlDatabase::new_primary(
            temp.path().to_path_buf(),
            Compactor(compactor_called.clone()),
            false,
        )
        .unwrap();

        let mut conn = db.connect().unwrap();
        conn.execute_program(
            Program::seq(&[
                "begin",
                "create table test (x)",
                "insert into test values (12)",
            ]),
            &mut (),
        )
        .unwrap();
        conn.inner_connection().cache_flush().unwrap();
        assert!(!compactor_called.load(Relaxed));
        conn.execute_program(Program::seq(&["commit"]), &mut ())
            .unwrap();
        assert!(compactor_called.load(Relaxed));
    }
}
