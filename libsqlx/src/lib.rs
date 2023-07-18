pub mod analysis;
pub mod error;
pub mod query;

mod connection;
mod database;
pub mod program;
pub mod result_builder;
mod seal;

pub type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

pub use connection::{Connection, DescribeResponse};
pub use database::libsql;
pub use database::libsql::replication_log::logger::{LogReadError, ReplicationLogger};
pub use database::libsql::replication_log::FrameNo;
pub use database::proxy;
pub use database::Frame;
pub use database::{Database, InjectableDatabase, Injector};

pub use sqld_libsql_bindings::wal_hook::WalHook;

pub use rusqlite;
