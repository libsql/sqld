use rusqlite::types::Value;

use crate::program::{Program, Step};
use crate::query::Query;
use crate::result_builder::{QueryBuilderConfig, QueryResultBuilderError, ResultBuilder};

#[derive(Debug, Clone)]
pub struct DescribeResponse {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<DescribeCol>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

#[derive(Debug, Clone)]
pub struct DescribeParam {
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DescribeCol {
    pub name: String,
    pub decltype: Option<String>,
}

pub trait Connection {
    /// Executes a query program
    fn execute_program(
        &mut self,
        pgm: Program,
        result_builder: &mut dyn ResultBuilder,
    ) -> crate::Result<()>;

    /// Parse the SQL statement and return information about it.
    fn describe(&self, sql: String) -> crate::Result<DescribeResponse>;

    /// execute a single query
    fn execute(&mut self, query: Query) -> crate::Result<Vec<Vec<Value>>> {
        #[derive(Default)]
        struct RowsBuilder {
            error: Option<crate::error::Error>,
            rows: Vec<Vec<Value>>,
            current_row: Vec<Value>,
        }

        impl ResultBuilder for RowsBuilder {
            fn init(
                &mut self,
                _config: &QueryBuilderConfig,
            ) -> std::result::Result<(), QueryResultBuilderError> {
                self.error = None;
                self.rows.clear();
                self.current_row.clear();

                Ok(())
            }

            fn add_row_value(
                &mut self,
                v: rusqlite::types::ValueRef,
            ) -> Result<(), QueryResultBuilderError> {
                self.current_row.push(v.into());
                Ok(())
            }

            fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
                let row = std::mem::take(&mut self.current_row);
                self.rows.push(row);

                Ok(())
            }

            fn step_error(
                &mut self,
                error: crate::error::Error,
            ) -> Result<(), QueryResultBuilderError> {
                self.error.replace(error);
                Ok(())
            }
        }

        let pgm = Program::new(vec![Step { cond: None, query }]);
        let mut builder = RowsBuilder::default();
        self.execute_program(pgm, &mut builder)?;
        if let Some(err) = builder.error.take() {
            Err(err)
        } else {
            Ok(builder.rows)
        }
    }
}

impl Connection for Box<dyn Connection> {
    fn execute_program(
        &mut self,
        pgm: Program,
        result_builder: &mut dyn ResultBuilder,
    ) -> crate::Result<()> {
        self.as_mut().execute_program(pgm, result_builder)
    }

    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        self.as_ref().describe(sql)
    }
}
