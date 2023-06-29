use crate::program::Program;
use crate::result_builder::ResultBuilder;

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
    fn execute_program<B: ResultBuilder>(
        &mut self,
        pgm: Program,
        result_builder: B,
    ) -> crate::Result<B>;

    /// Parse the SQL statement and return information about it.
    fn describe(&self, sql: String) -> crate::Result<DescribeResponse>;
}
