use std::sync::Arc;

use parking_lot::Mutex;

use crate::connection::{Connection, DescribeResponse};
use crate::database::FrameNo;
use crate::program::Program;
use crate::result_builder::{Column, QueryBuilderConfig, QueryResultBuilderError, ResultBuilder};
use crate::Result;

use super::WaitFrameNoCb;

#[derive(Debug, Default)]
pub(crate) struct ConnState {
    is_txn: bool,
    last_frame_no: Option<FrameNo>,
}

/// A connection that proxies write operations to the `WriteDb` and the read operations to the
/// `ReadDb`
pub struct WriteProxyConnection<R, W> {
    pub(crate) read_conn: R,
    pub(crate) write_conn: W,
    pub(crate) wait_frame_no_cb: WaitFrameNoCb,
    pub(crate) state: Arc<Mutex<ConnState>>,
}

impl<R, W> WriteProxyConnection<R, W> {
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.write_conn
    }

    pub fn writer(&self) -> &W {
        &self.write_conn
    }

    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.read_conn
    }

    pub fn reader(&self) -> &R {
        &self.read_conn
    }
}

struct MaybeRemoteExecBuilder<W> {
    builder: Option<Box<dyn ResultBuilder>>,
    conn: W,
    pgm: Program,
    state: Arc<Mutex<ConnState>>,
}

impl<W> ResultBuilder for MaybeRemoteExecBuilder<W>
where
    W: Connection + Send + 'static,
{
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().init(config)
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder
            .as_mut()
            .unwrap()
            .finish_step(affected_row_count, last_insert_rowid)
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().step_error(error)
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().cols_description(cols)
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().begin_rows()
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().begin_row()
    }

    fn add_row_value(
        &mut self,
        v: rusqlite::types::ValueRef,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().add_row_value(v)
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().finish_row()
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.as_mut().unwrap().finish_rows()
    }

    fn finnalize(
        &mut self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        if is_txn {
            // a read only connection is not allowed to leave an open transaction. We mispredicted the
            // final state of the connection, so we rollback, and execute again on the write proxy.
            let builder = ExtractFrameNoBuilder {
                builder: self
                    .builder
                    .take()
                    .expect("finnalize called more than once"),
                state: self.state.clone(),
            };

            self.conn
                .execute_program(&self.pgm, Box::new(builder))
                .unwrap();

            Ok(false)
        } else {
            self.builder.as_mut().unwrap().finnalize(is_txn, frame_no)
        }
    }
}

impl<R, W> Connection for WriteProxyConnection<R, W>
where
    R: Connection,
    W: Connection + Clone + Send + 'static,
{
    fn execute_program(
        &mut self,
        pgm: &Program,
        builder: Box<dyn ResultBuilder>,
    ) -> crate::Result<()> {
        if !self.state.lock().is_txn && pgm.is_read_only() {
            if let Some(frame_no) = self.state.lock().last_frame_no {
                (self.wait_frame_no_cb)(frame_no);
            }

            let builder = MaybeRemoteExecBuilder {
                builder: Some(builder),
                conn: self.write_conn.clone(),
                state: self.state.clone(),
                pgm: pgm.clone(),
            };
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            self.read_conn.execute_program(pgm, Box::new(builder))?;
            // rollback(&mut self.conn.read_db);
            Ok(())
        } else {
            let builder = ExtractFrameNoBuilder {
                builder,
                state: self.state.clone(),
            };
            self.write_conn.execute_program(pgm, Box::new(builder))?;
            Ok(())
        }
    }

    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        if let Some(frame_no) = self.state.lock().last_frame_no {
            (self.wait_frame_no_cb)(frame_no);
        }
        self.read_conn.describe(sql)
    }
}

struct ExtractFrameNoBuilder {
    builder: Box<dyn ResultBuilder>,
    state: Arc<Mutex<ConnState>>,
}

impl ResultBuilder for ExtractFrameNoBuilder {
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.builder.init(config)
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder
            .finish_step(affected_row_count, last_insert_rowid)
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.builder.step_error(error)
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder.cols_description(cols)
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.begin_rows()
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.begin_row()
    }

    fn add_row_value(
        &mut self,
        v: rusqlite::types::ValueRef,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder.add_row_value(v)
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.finish_row()
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.finish_rows()
    }

    fn finnalize(
        &mut self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        let mut state = self.state.lock();
        state.last_frame_no = frame_no;
        state.is_txn = is_txn;
        self.builder.finnalize(is_txn, frame_no)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use crate::Connection;
    use crate::database::test_utils::MockDatabase;
    use crate::database::{proxy::database::WriteProxyDatabase, Database};
    use crate::program::Program;

    #[test]
    fn simple_write_proxied() {
        let write_called = Arc::new(Mutex::new(false));
        let write_db = MockDatabase::new().with_execute({
            let write_called = write_called.clone();
            move |_, mut b| {
                b.finnalize(false, Some(42)).unwrap();
                *write_called.lock() =true;
                Ok(())
            }
        });

        let read_called = Arc::new(Mutex::new(false));
        let read_db = MockDatabase::new().with_execute({
            let read_called = read_called.clone();
            move |_, _| {
                *read_called.lock() = true;
                Ok(())
            }
        });

        let wait_called = Arc::new(Mutex::new(false));
        let db = WriteProxyDatabase::new(
            read_db,
            write_db,
            Arc::new({
                let wait_called = wait_called.clone();
                move |fno| {
                    assert_eq!(fno, 42);
                    *wait_called.lock() = true;
                }
            }),
        );

        let mut conn = db.connect().unwrap();
        conn.execute_program(&Program::seq(&["insert into test values (12)"]), Box::new(()))
            .unwrap();

        assert!(!*wait_called.lock());
        assert!(!*read_called.lock());
        assert!(*write_called.lock());

        conn.execute_program(&Program::seq(&["select * from test"]), Box::new(()))
            .unwrap();

        assert!(*read_called.lock());
        assert!(*wait_called.lock());
    }
}
