pub mod analysis;
pub mod error;
pub mod query;

mod connection;
mod database;
mod program;
mod result_builder;
mod seal;
mod semaphore;

pub type Result<T, E = crate::error::Error> = std::result::Result<T, E>;

pub use connection::Connection;
pub use database::libsql;
pub use database::proxy;
pub use database::Database;
pub use program::Program;
pub use result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilderError, ResultBuilder, ResultBuilderExt,
};
