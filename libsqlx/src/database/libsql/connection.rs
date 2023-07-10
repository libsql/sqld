use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rusqlite::{OpenFlags, Statement, StatementStatus};
use sqld_libsql_bindings::wal_hook::{WalHook, WalMethodsHook};

use crate::connection::{Connection, DescribeCol, DescribeParam, DescribeResponse};
use crate::database::TXN_TIMEOUT;
use crate::error::Error;
use crate::program::{Cond, Program, Step};
use crate::query::Query;
use crate::result_builder::{QueryBuilderConfig, ResultBuilder};
use crate::seal::Seal;
use crate::Result;

use super::RowStatsHandler;

pub struct RowStats {
    pub rows_read: u64,
    pub rows_written: u64,
}

impl From<&Statement<'_>> for RowStats {
    fn from(stmt: &Statement) -> Self {
        Self {
            rows_read: stmt.get_status(StatementStatus::RowsRead) as u64,
            rows_written: stmt.get_status(StatementStatus::RowsWritten) as u64,
        }
    }
}

pub fn open_db<'a, W>(
    path: &Path,
    wal_methods: &'static WalMethodsHook<W>,
    hook_ctx: &'a mut W::Context,
    flags: Option<OpenFlags>,
) -> std::result::Result<sqld_libsql_bindings::Connection<'a>, rusqlite::Error>
where
    W: WalHook,
{
    let flags = flags.unwrap_or(
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    );

    sqld_libsql_bindings::Connection::open(path, flags, wal_methods, hook_ctx)
}

pub struct LibsqlConnection<C> {
    timeout_deadline: Option<Instant>,
    conn: sqld_libsql_bindings::Connection<'static>, // holds a ref to _context, must be dropped first.
    row_stats_handler: Option<Arc<dyn RowStatsHandler>>,
    builder_config: QueryBuilderConfig,
    _context: Seal<Box<C>>,
}

impl<C> LibsqlConnection<C> {
    pub(crate) fn new<W: WalHook>(
        path: &Path,
        extensions: Option<Arc<[PathBuf]>>,
        wal_methods: &'static WalMethodsHook<W>,
        hook_ctx: W::Context,
        row_stats_callback: Option<Arc<dyn RowStatsHandler>>,
        builder_config: QueryBuilderConfig,
    ) -> Result<LibsqlConnection<W::Context>> {
        let mut ctx = Box::new(hook_ctx);
        let this = LibsqlConnection {
            conn: open_db(
                path,
                wal_methods,
                unsafe { &mut *(ctx.as_mut() as *mut _) },
                None,
            )?,
            timeout_deadline: None,
            builder_config,
            row_stats_handler: row_stats_callback,
            _context: Seal::new(ctx),
        };

        if let Some(extensions) = extensions {
            for ext in extensions.iter() {
                unsafe {
                    let _guard = rusqlite::LoadExtensionGuard::new(&this.conn).unwrap();
                    if let Err(e) = this.conn.load_extension(ext, None) {
                        tracing::error!("failed to load extension: {}", ext.display());
                        Err(e)?;
                    }
                    tracing::debug!("Loaded extension {}", ext.display());
                }
            }
        }

        Ok(this)
    }

    #[cfg(test)]
    pub fn inner_connection(&self) -> &sqld_libsql_bindings::Connection<'static> {
        &self.conn
    }

    fn run(&mut self, pgm: Program, builder: &mut dyn ResultBuilder) -> Result<()> {
        let mut results = Vec::with_capacity(pgm.steps.len());

        builder.init(&self.builder_config)?;
        let is_autocommit_before = self.conn.is_autocommit();

        for step in pgm.steps() {
            let res = self.execute_step(step, &results, builder)?;
            results.push(res);
        }

        // A transaction is still open, set up a timeout
        if is_autocommit_before && !self.conn.is_autocommit() {
            self.timeout_deadline = Some(Instant::now() + TXN_TIMEOUT)
        }

        builder.finish(!self.conn.is_autocommit(), None)?;

        Ok(())
    }

    fn execute_step(
        &mut self,
        step: &Step,
        results: &[bool],
        builder: &mut dyn ResultBuilder,
    ) -> Result<bool> {
        builder.begin_step()?;
        let mut enabled = match step.cond.as_ref() {
            Some(cond) => match eval_cond(cond, results) {
                Ok(enabled) => enabled,
                Err(e) => {
                    builder.step_error(e).unwrap();
                    false
                }
            },
            None => true,
        };

        let (affected_row_count, last_insert_rowid) = if enabled {
            match self.execute_query(&step.query, builder) {
                // builder error interupt the execution of query. we should exit immediately.
                Err(e @ Error::BuilderError(_)) => return Err(e),
                Err(e) => {
                    builder.step_error(e)?;
                    enabled = false;
                    (0, None)
                }
                Ok(x) => x,
            }
        } else {
            (0, None)
        };

        builder.finish_step(affected_row_count, last_insert_rowid)?;

        Ok(enabled)
    }

    fn execute_query(
        &self,
        query: &Query,
        builder: &mut dyn ResultBuilder,
    ) -> Result<(u64, Option<i64>)> {
        tracing::trace!("executing query: {}", query.stmt.stmt);

        let mut stmt = self.conn.prepare(&query.stmt.stmt)?;

        let cols = stmt.columns();
        let cols_count = cols.len();
        builder.cols_description(&mut cols.iter().map(Into::into))?;
        drop(cols);

        query
            .params
            .bind(&mut stmt)
            .map_err(|e|Error::LibSqlInvalidQueryParams(e.to_string()))?;

        let mut qresult = stmt.raw_query();
        builder.begin_rows()?;
        while let Some(row) = qresult.next()? {
            builder.begin_row()?;
            for i in 0..cols_count {
                let val = row.get_ref(i)?;
                builder.add_row_value(val)?;
            }
            builder.finish_row()?;
        }

        builder.finish_rows()?;

        // sqlite3_changes() is only modified for INSERT, UPDATE or DELETE; it is not reset for SELECT,
        // but we want to return 0 in that case.
        let affected_row_count = match query.stmt.is_iud {
            true => self.conn.changes(),
            false => 0,
        };

        // sqlite3_last_insert_rowid() only makes sense for INSERTs into a rowid table. we can't detect
        // a rowid table, but at least we can detect an INSERT
        let last_insert_rowid = match query.stmt.is_insert {
            true => Some(self.conn.last_insert_rowid()),
            false => None,
        };

        drop(qresult);

        if let Some(ref handler) = self.row_stats_handler {
            handler.handle_row_stats(RowStats::from(&stmt))
        }

        Ok((affected_row_count, last_insert_rowid))
    }
}

fn eval_cond(cond: &Cond, results: &[bool]) -> Result<bool> {
    let get_step_res = |step: usize| -> Result<bool> {
        let res = results.get(step).ok_or(Error::InvalidBatchStep(step))?;

        Ok(*res)
    };

    Ok(match cond {
        Cond::Ok { step } => get_step_res(*step)?,
        Cond::Err { step } => !get_step_res(*step)?,
        Cond::Not { cond } => !eval_cond(cond, results)?,
        Cond::And { conds } => conds
            .iter()
            .try_fold(true, |x, cond| eval_cond(cond, results).map(|y| x & y))?,
        Cond::Or { conds } => conds
            .iter()
            .try_fold(false, |x, cond| eval_cond(cond, results).map(|y| x | y))?,
    })
}

impl<C> Connection for LibsqlConnection<C> {
    fn execute_program(
        &mut self,
        pgm: Program,
        builder: &mut dyn ResultBuilder,
    ) -> crate::Result<()> {
        self.run(pgm, builder)
    }

    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        let stmt = self.conn.prepare(&sql)?;

        let params = (1..=stmt.parameter_count())
            .map(|param_i| {
                let name = stmt.parameter_name(param_i).map(|n| n.into());
                DescribeParam { name }
            })
            .collect();

        let cols = stmt
            .columns()
            .into_iter()
            .map(|col| {
                let name = col.name().into();
                let decltype = col.decl_type().map(|t| t.into());
                DescribeCol { name, decltype }
            })
            .collect();

        let is_explain = stmt.is_explain() != 0;
        let is_readonly = stmt.readonly();

        Ok(DescribeResponse {
            params,
            cols,
            is_explain,
            is_readonly,
        })
    }
}

#[cfg(test)]
mod test {
    // use itertools::Itertools;
    //
    // use crate::result_builder::{test::test_driver, IgnoreResult};
    //
    // use super::*;

    // fn setup_test_conn(ctx: &mut ()) -> Conn<C> {
    //     let mut conn = Conn {
    //         timeout_deadline: None,
    //         conn: sqld_libsql_bindings::Connection::test(ctx),
    //         timed_out: false,
    //         builder_config: QueryBuilderConfig::default(),
    //         row_stats_callback: None,
    //     };
    //
    //     let stmts = std::iter::once("create table test (x)")
    //         .chain(std::iter::repeat("insert into test values ('hello world')").take(100))
    //         .collect_vec();
    //     conn.run(Program::seq(&stmts), IgnoreResult).unwrap();
    //
    //     conn
    // }
    //
    // #[test]
    // fn test_libsql_conn_builder_driver() {
    //     test_driver(1000, |b| {
    //         let ctx = &mut ();
    //         let mut conn = setup_test_conn(ctx);
    //         conn.run(Program::seq(&["select * from test"]), b)
    //     })
    // }
}
