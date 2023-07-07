use std::sync::Arc;

use crate::{
    connection::{Connection, DescribeResponse},
    program::Program,
    result_builder::ResultBuilder,
};

use super::Database;

pub struct MockDatabase {
    #[allow(clippy::type_complexity)]
    describe_fn: Arc<dyn Fn(String) -> crate::Result<DescribeResponse>>,
    #[allow(clippy::type_complexity)]
    execute_fn: Arc<dyn Fn(Program, &mut dyn ResultBuilder) -> crate::Result<()>>,
}

pub struct MockConnection {
    #[allow(clippy::type_complexity)]
    describe_fn: Arc<dyn Fn(String) -> crate::Result<DescribeResponse>>,
    #[allow(clippy::type_complexity)]
    execute_fn: Arc<dyn Fn(Program, &mut dyn ResultBuilder) -> crate::Result<()>>,
}

impl MockDatabase {
    pub fn new() -> Self {
        MockDatabase {
            describe_fn: Arc::new(|_| panic!("describe fn not set")),
            execute_fn: Arc::new(|_, _| panic!("execute fn not set")),
        }
    }

    pub fn with_execute(
        mut self,
        f: impl Fn(Program, &mut dyn ResultBuilder) -> crate::Result<()> + 'static,
    ) -> Self {
        self.execute_fn = Arc::new(f);
        self
    }
}

impl Database for MockDatabase {
    type Connection = MockConnection;

    fn connect(&self) -> Result<Self::Connection, crate::error::Error> {
        Ok(MockConnection {
            describe_fn: self.describe_fn.clone(),
            execute_fn: self.execute_fn.clone(),
        })
    }
}

impl Connection for MockConnection {
    fn execute_program(
        &mut self,
        pgm: crate::program::Program,
        reponse_builder: &mut dyn ResultBuilder,
    ) -> crate::Result<()> {
        (self.execute_fn)(pgm, reponse_builder)?;
        Ok(())
    }

    fn describe(&self, sql: String) -> crate::Result<DescribeResponse> {
        (self.describe_fn)(sql)
    }
}
