use std::sync::Arc;

use crate::query::Query;

#[derive(Debug, Clone)]
pub struct Program {
    pub steps: Arc<[Step]>,
}

impl Program {
    pub fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: steps.into(),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.steps.iter().all(|s| s.query.stmt.is_read_only())
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// transforms a collection of queries into a batch program. The execution of each query
    /// depends on the success of the previous one.
    pub fn from_queries(qs: impl IntoIterator<Item = Query>) -> Self {
        let steps = qs
            .into_iter()
            .enumerate()
            .map(|(idx, query)| Step {
                cond: (idx > 0).then(|| Cond::Ok { step: idx - 1 }),
                query,
            })
            .collect();

        Self { steps }
    }

    #[cfg(test)]
    pub fn seq(stmts: &[&str]) -> Self {
        use crate::{analysis::Statement, query::Params};

        let mut steps = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            let step = Step {
                cond: None,
                query: Query {
                    stmt: Statement::parse(stmt).next().unwrap().unwrap(),
                    params: Params::empty(),
                    want_rows: true,
                },
            };

            steps.push(step);
        }

        Self::new(steps)
    }
}

#[derive(Debug, Clone)]
pub struct Step {
    pub cond: Option<Cond>,
    pub query: Query,
}

#[derive(Debug, Clone)]
pub enum Cond {
    Ok { step: usize },
    Err { step: usize },
    Not { cond: Box<Self> },
    Or { conds: Vec<Self> },
    And { conds: Vec<Self> },
}
