pub mod analysis;
pub mod error;
pub mod query;

mod connection;
mod database;
pub mod program;
pub mod result_builder;
mod seal;

pub type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

pub use connection::Connection;
pub use database::libsql;
pub use database::proxy;
pub use database::Database;
pub use database::libsql::replication_log::FrameNo;

pub use rusqlite;
