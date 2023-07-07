use std::time::Duration;

use crate::connection::Connection;
use crate::error::Error;
use self::frame::Frame;

mod frame;
pub mod libsql;
pub mod proxy;
#[cfg(test)]
mod test_utils;

pub type FrameNo = u64;

pub const TXN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum InjectError {}

pub trait Database {
    type Connection: Connection;
    /// Create a new connection to the database
    fn connect(&self) -> Result<Self::Connection, Error>;
}

pub trait InjectableDatabase {
    fn injector(&mut self) -> crate::Result<Box<dyn Injector>>;
}

// Trait implemented by databases that support frame injection
pub trait Injector {
    fn inject(&mut self, frame: Frame) -> Result<(), InjectError>;
}
