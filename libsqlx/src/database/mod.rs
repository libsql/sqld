use std::time::Duration;

use crate::connection::{Connection, DescribeResponse};
use crate::error::Error;
use crate::program::Program;
use crate::result_builder::ResultBuilder;
use crate::semaphore::{Permit, Semaphore};

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

    /// returns a database with a limit on the number of conccurent connections
    fn throttled(self, limit: usize, timeout: Option<Duration>) -> ThrottledDatabase<Self>
    where
        Self: Sized,
    {
        ThrottledDatabase::new(limit, self, timeout)
    }
}

// Trait implemented by databases that support frame injection
pub trait InjectableDatabase {
    fn inject_frame(&mut self, frame: Frame) -> Result<(), InjectError>;
}

/// A Database that limits the number of conccurent connections to the underlying database.
pub struct ThrottledDatabase<T> {
    semaphore: Semaphore,
    db: T,
    timeout: Option<Duration>,
}

impl<T> ThrottledDatabase<T> {
    fn new(conccurency: usize, db: T, timeout: Option<Duration>) -> Self {
        Self {
            semaphore: Semaphore::new(conccurency),
            db,
            timeout,
        }
    }
}

impl<F: Database> Database for ThrottledDatabase<F> {
    type Connection = TrackedDb<F::Connection>;

    fn connect(&self) -> Result<Self::Connection, Error> {
        let permit = match self.timeout {
            Some(t) => self
                .semaphore
                .acquire_timeout(t)
                .ok_or(Error::DbCreateTimeout)?,
            None => self.semaphore.acquire(),
        };

        let inner = self.db.connect()?;
        Ok(TrackedDb { permit, inner })
    }
}

pub struct TrackedDb<DB> {
    inner: DB,
    #[allow(dead_code)] // just hold on to it
    permit: Permit,
}

impl<C: Connection> Connection for TrackedDb<C> {
    #[inline]
    fn execute_program<B: ResultBuilder>(&mut self, pgm: Program, builder: B) -> crate::Result<B> {
        self.inner.execute_program(pgm, builder)
    }

    #[inline]
    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        self.inner.describe(sql)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct DummyConn;

    impl Connection for DummyConn {
        fn execute_program<B>(&mut self, _pgm: Program, _builder: B) -> crate::Result<B>
        where
            B: ResultBuilder,
        {
            unreachable!()
        }

        fn describe(&self, _sql: String) -> crate::Result<DescribeResponse> {
            unreachable!()
        }
    }

    struct DummyDatabase;

    impl Database for DummyDatabase {
        type Connection = DummyConn;

        fn connect(&self) -> Result<Self::Connection, Error> {
            Ok(DummyConn)
        }
    }

    #[test]
    fn throttle_db_creation() {
        let db = DummyDatabase.throttled(1, Some(Duration::from_millis(100)));
        let conn = db.connect().unwrap();

        assert!(db.connect().is_err());

        drop(conn);

        assert!(db.connect().is_ok());
    }
}
