use crate::connection::{Connection, DescribeResponse};
use crate::database::FrameNo;
use crate::program::Program;
use crate::result_builder::{QueryBuilderConfig, ResultBuilder};
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
    pub(crate) state: parking_lot::Mutex<ConnState>,
}

impl<ReadDb, WriteDb> Connection for WriteProxyConnection<ReadDb, WriteDb>
where
    ReadDb: Connection,
    WriteDb: Connection,
{
    fn execute_program(&mut self, pgm: Program, builder: &mut dyn ResultBuilder) -> Result<()> {
        let mut state = self.state.lock();
        let mut builder = ExtractFrameNoBuilder::new(builder);
        if !state.is_txn && pgm.is_read_only() {
            if let Some(frame_no) = state.last_frame_no {
                (self.wait_frame_no_cb)(frame_no);
            }
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            self.read_db.execute_program(pgm.clone(), &mut builder)?;

            // still in transaction state after running a read-only txn
            if builder.is_txn {
                // TODO: rollback
                // self.read_db.rollback().await?;
                self.write_db.execute_program(pgm, &mut builder)?;
                state.is_txn = builder.is_txn;
                state.last_frame_no = builder.frame_no;
                Ok(())
            } else {
                Ok(())
            }
        } else {
            self.write_db.execute_program(pgm, &mut builder)?;
            state.is_txn = builder.is_txn;
            state.last_frame_no = builder.frame_no;
            Ok(())
        }
    }

    fn describe(&self, sql: String) -> Result<DescribeResponse> {
        if let Some(frame_no) = self.state.lock().last_frame_no {
            (self.wait_frame_no_cb)(frame_no);
        }
        self.read_db.describe(sql)
    }
}

struct ExtractFrameNoBuilder<'a> {
    inner: &'a mut dyn ResultBuilder,
    frame_no: Option<FrameNo>,
    is_txn: bool,
}

impl<'a> ExtractFrameNoBuilder<'a> {
    fn new(inner: &'a mut dyn ResultBuilder) -> Self {
        Self {
            inner,
            frame_no: None,
            is_txn: false,
        }
    }
}

impl<'a> ResultBuilder for ExtractFrameNoBuilder<'a> {
    fn init(
        &mut self,
        config: &QueryBuilderConfig,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.init(config)
    }

    fn begin_step(
        &mut self,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner
            .finish_step(affected_row_count, last_insert_rowid)
    }

    fn step_error(
        &mut self,
        error: crate::error::Error,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.step_error(error)
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = crate::result_builder::Column>,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.cols_description(cols)
    }

    fn begin_rows(
        &mut self,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.begin_rows()
    }

    fn begin_row(
        &mut self,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.begin_row()
    }

    fn add_row_value(
        &mut self,
        v: rusqlite::types::ValueRef,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.add_row_value(v)
    }

    fn finish_row(
        &mut self,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.finish_row()
    }

    fn finish_rows(
        &mut self,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.inner.finish_rows()
    }

    fn finish(
        &mut self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> std::result::Result<(), crate::result_builder::QueryResultBuilderError> {
        self.frame_no = frame_no;
        self.is_txn = is_txn;
        self.inner.finish(is_txn, frame_no)
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
                b.finish(false, Some(42)).unwrap();
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
