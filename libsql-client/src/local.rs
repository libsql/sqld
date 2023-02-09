use async_trait::async_trait;

use super::{QueryResult, Statement};

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Debug)]
pub struct Connection {
    inner: rusqlite::Connection,
}

impl Connection {
    /// Establishes a database connection.
    ///
    /// # Arguments
    /// * `path` - path of the local database
    pub fn connect(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: rusqlite::Connection::open(path).map_err(|e| anyhow::anyhow!("{e}"))?,
        })
    }

    //FIXME: implement
    fn row_to_result(_row: &rusqlite::Row) -> QueryResult {
        let result_set = super::ResultSet {
            columns: vec!["dummy".into()],
            rows: vec![super::Row {
                cells: std::collections::HashMap::new(),
            }],
        };
        let meta = super::Meta { duration: 42 };
        QueryResult::Success((result_set, meta))
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// let db = libsql_client::Connection::connect("https://example.com", "admin", "s3cr3tp4ss");
    /// let result = db
    ///     .batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"])
    ///     .await;
    /// # }
    /// ```
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        let mut result = vec![];
        for stmt in stmts {
            let row = match self
                .inner
                .query_row(&stmt.into().q, (/*FIXME: params*/), |r| {
                    Ok(Self::row_to_result(r))
                }) {
                Ok(row) => row,
                Err(_) =>
                /*empty*/
                {
                    unimplemented!()
                }
            };
            result.push(row)
        }
        Ok(result)
    }
}

#[async_trait(?Send)]
impl super::Connection for Connection {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.batch(stmts).await
    }
}
