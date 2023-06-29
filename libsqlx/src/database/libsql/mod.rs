use std::path::PathBuf;
use std::sync::Arc;

use sqld_libsql_bindings::wal_hook::{TransparentMethods, WalHook, TRANSPARENT_METHODS};
use sqld_libsql_bindings::WalMethodsHook;

use crate::database::frame::Frame;
use crate::database::{Database, InjectError, InjectableDatabase};
use crate::error::Error;
use crate::result_builder::QueryBuilderConfig;

use connection::{LibsqlConnection, RowStats};
use injector::Injector;
use replication_log::logger::{
    ReplicationLogger, ReplicationLoggerHook, ReplicationLoggerHookCtx, REPLICATION_METHODS,
};

use self::injector::InjectorCommitHandler;
use self::replication_log::logger::LogCompactor;

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
    // frame injector for the database
    injector: Injector,
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
pub trait RowStatsHandler {
    fn handle_row_stats(&self, stats: RowStats);
}

impl<F> RowStatsHandler for F
where
    F: Fn(RowStats),
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
            injector: Injector::new(&db_path, injector_commit_handler, injector_buffer_capacity)?,
        };

        Ok(Self::new(db_path, ty))
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
            logger: Arc::new(ReplicationLogger::open(&db_path, dirty, compactor)?),
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
        LibsqlConnection::<<T::ConnectionHook as WalHook>::Context>::new(
            &self.db_path,
            self.extensions.clone(),
            T::hook(),
            self.ty.hook_context(),
            self.row_stats_callback.clone(),
            QueryBuilderConfig {
                max_size: Some(self.response_size_limit),
            },
        )
    }
}

impl InjectableDatabase for LibsqlDatabase<ReplicaType> {
    fn inject_frame(&mut self, frame: Frame) -> Result<(), InjectError> {
        self.ty.injector.inject_frame(frame).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::cell::Cell;
    use std::fs::File;
    use std::rc::Rc;

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
            injector: Injector::new(temp.path(), (), 10).unwrap(),
        };
        let mut db = LibsqlDatabase::new(temp.path().to_path_buf(), replica);

        let mut conn = db.connect().unwrap();
        let res = conn
            .execute_program(
                Program::seq(&["select count(*) from test"]),
                ReadRowBuilder(Vec::new()),
            )
            .unwrap();
        assert!(res.0.is_empty());

        let file = File::open("assets/test/simple_wallog").unwrap();
        let log = LogFile::new(file).unwrap();
        log.frames_iter()
            .unwrap()
            .for_each(|f| db.inject_frame(f.unwrap()).unwrap());

        let res = conn
            .execute_program(
                Program::seq(&["select count(*) from test"]),
                ReadRowBuilder(Vec::new()),
            )
            .unwrap();
        assert_eq!(res.0[0], Value::Integer(5));
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
                injector: Injector::new(temp_replica.path(), (), 10).unwrap(),
            },
        );

        let mut primary_conn = primary.connect().unwrap();
        primary_conn
            .execute_program(
                Program::seq(&["create table test (x)", "insert into test values (42)"]),
                (),
            )
            .unwrap();

        let logfile = primary.ty.logger.log_file.read();

        for frame in logfile.frames_iter().unwrap() {
            let frame = frame.unwrap();
            replica.inject_frame(frame).unwrap();
        }

        let mut replica_conn = replica.connect().unwrap();
        let result = replica_conn
            .execute_program(
                Program::seq(&["select * from test limit 1"]),
                ReadRowBuilder(Vec::new()),
            )
            .unwrap();

        assert_eq!(result.0.len(), 1);
        assert_eq!(result.0[0], Value::Integer(42));
    }

    #[test]
    fn primary_compact_log() {
        struct Compactor(Rc<Cell<bool>>);

        impl LogCompactor for Compactor {
            fn should_compact(&self, log: &LogFile) -> bool {
                log.header().frame_count > 2
            }

            fn compact(
                &self,
                _file: LogFile,
                _path: PathBuf,
                _size_after: u32,
            ) -> anyhow::Result<()> {
                self.0.set(true);
                Ok(())
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let compactor_called = Rc::new(Cell::new(false));
        let db = LibsqlDatabase::new_primary(
            temp.path().to_path_buf(),
            Compactor(compactor_called.clone()),
            false,
        )
        .unwrap();

        let mut conn = db.connect().unwrap();
        conn.execute_program(
            Program::seq(&["create table test (x)", "insert into test values (12)"]),
            (),
        )
        .unwrap();
        assert!(compactor_called.get());
    }

    #[test]
    fn no_compaction_uncommited_frames() {
        struct Compactor(Rc<Cell<bool>>);

        impl LogCompactor for Compactor {
            fn should_compact(&self, log: &LogFile) -> bool {
                assert_eq!(log.uncommitted_frame_count, 0);
                self.0.set(true);
                false
            }

            fn compact(
                &self,
                _file: LogFile,
                _path: PathBuf,
                _size_after: u32,
            ) -> anyhow::Result<()> {
                unreachable!()
            }
        }

        let temp = tempfile::tempdir().unwrap();
        let compactor_called = Rc::new(Cell::new(false));
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
            (),
        )
        .unwrap();
        conn.inner_connection().cache_flush().unwrap();
        assert!(!compactor_called.get());
        conn.execute_program(Program::seq(&["commit"]), ()).unwrap();
        assert!(compactor_called.get());
    }
}
