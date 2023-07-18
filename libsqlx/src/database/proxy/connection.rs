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
pub struct WriteProxyConnection<ReadDb, WriteDb> {
    pub(crate) read_db: ReadDb,
    pub(crate) write_db: WriteDb,
    pub(crate) wait_frame_no_cb: WaitFrameNoCb,
    pub(crate) state: ConnState,
}

struct MaybeRemoteExecBuilder<'a, 'b, B, W> {
    builder: B,
    conn: &'a mut W,
    pgm: &'b Program,
    state: &'a mut ConnState,
}

impl<'a, 'b, B, W> ResultBuilder for MaybeRemoteExecBuilder<'a, 'b, B, W>
where
    W: Connection,
    B: ResultBuilder,
{
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
        self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        if is_txn {
            // a read only connection is not allowed to leave an open transaction. We mispredicted the
            // final state of the connection, so we rollback, and execute again on the write proxy.
            let builder = ExtractFrameNoBuilder {
                builder: self.builder,
                state: self.state,
            };

            self.conn.execute_program(self.pgm, builder).unwrap();

            Ok(false)
        } else {
            self.builder.finnalize(is_txn, frame_no)
        }
    }
}

impl<ReadDb, WriteDb> Connection for WriteProxyConnection<ReadDb, WriteDb>
where
    ReadDb: Connection,
    WriteDb: Connection,
{
    fn execute_program<B: ResultBuilder>(
        &mut self,
        pgm: &Program,
        builder: B,
    ) -> crate::Result<()> {
        if !self.state.is_txn && pgm.is_read_only() {
            if let Some(frame_no) = self.state.last_frame_no {
                (self.wait_frame_no_cb)(frame_no);
            }

            let builder = MaybeRemoteExecBuilder {
                builder,
                conn: &mut self.write_db,
                state: &mut self.state,
                pgm,
            };
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            self.read_db.execute_program(pgm, builder)?;
            // rollback(&mut self.conn.read_db);
            Ok(())
        } else {
            let builder = ExtractFrameNoBuilder {
                builder,
                state: &mut self.state,
            };
            self.write_db.execute_program(pgm, builder)?;
            Ok(())
        }
    }

    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        if let Some(frame_no) = self.state.last_frame_no {
            (self.wait_frame_no_cb)(frame_no);
        }
        self.read_db.describe(sql)
    }
}

struct ExtractFrameNoBuilder<'a, B> {
    builder: B,
    state: &'a mut ConnState,
}

impl<B: ResultBuilder> ResultBuilder for ExtractFrameNoBuilder<'_, B> {
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
        self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        self.state.last_frame_no = frame_no;
        self.state.is_txn = is_txn;
        self.builder.finnalize(is_txn, frame_no)
    }
}

#[cfg(test)]
mod test {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;

    use crate::connection::Connection;
    use crate::database::test_utils::MockDatabase;
    use crate::database::{proxy::database::WriteProxyDatabase, Database};
    use crate::program::Program;

    #[test]
    fn simple_write_proxied() {
        let write_called = Rc::new(Cell::new(false));
        let write_db = MockDatabase::new().with_execute({
            let write_called = write_called.clone();
            move |_, b| {
                b.finnalize(false, Some(42)).unwrap();
                write_called.set(true);
                Ok(())
            }
        });

        let read_called = Rc::new(Cell::new(false));
        let read_db = MockDatabase::new().with_execute({
            let read_called = read_called.clone();
            move |_, _| {
                read_called.set(true);
                Ok(())
            }
        });

        let wait_called = Rc::new(Cell::new(false));
        let db = WriteProxyDatabase::new(
            read_db,
            write_db,
            Arc::new({
                let wait_called = wait_called.clone();
                move |fno| {
                    assert_eq!(fno, 42);
                    wait_called.set(true);
                }
            }),
        );

        let mut conn = db.connect().unwrap();
        conn.execute_program(Program::seq(&["insert into test values (12)"]), &mut ())
            .unwrap();

        assert!(!wait_called.get());
        assert!(!read_called.get());
        assert!(write_called.get());

        conn.execute_program(Program::seq(&["select * from test"]), &mut ())
            .unwrap();

        assert!(read_called.get());
        assert!(wait_called.get());
    }
}
