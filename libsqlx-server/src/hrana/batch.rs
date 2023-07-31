use std::collections::HashMap;
use std::sync::Arc;

// use crate::auth::Authenticated;

use libsqlx::analysis::Statement;
use libsqlx::program::{Cond, Program, Step};
use libsqlx::query::{Params, Query};
use libsqlx::result_builder::{StepResult, StepResultsBuilder};
use tokio::sync::oneshot;

use crate::allocation::ConnectionHandle;

use super::error::HranaError;
use super::result_builder::HranaBatchProtoBuilder;
use super::stmt::{proto_stmt_to_query, StmtError};
use super::{proto, ProtocolError, Version};

fn proto_cond_to_cond(cond: &proto::BatchCond, max_step_i: usize) -> Result<Cond, HranaError> {
    let try_convert_step = |step: i32| -> Result<usize, ProtocolError> {
        let step = usize::try_from(step).map_err(|_| ProtocolError::BatchCondBadStep)?;
        if step >= max_step_i {
            return Err(ProtocolError::BatchCondBadStep);
        }
        Ok(step)
    };

    let cond = match cond {
        proto::BatchCond::Ok { step } => Cond::Ok {
            step: try_convert_step(*step)?,
        },
        proto::BatchCond::Error { step } => Cond::Err {
            step: try_convert_step(*step)?,
        },
        proto::BatchCond::Not { cond } => Cond::Not {
            cond: proto_cond_to_cond(cond, max_step_i)?.into(),
        },
        proto::BatchCond::And { conds } => Cond::And {
            conds: conds
                .iter()
                .map(|cond| proto_cond_to_cond(cond, max_step_i))
                .collect::<crate::Result<_, HranaError>>()?,
        },
        proto::BatchCond::Or { conds } => Cond::Or {
            conds: conds
                .iter()
                .map(|cond| proto_cond_to_cond(cond, max_step_i))
                .collect::<crate::Result<_, HranaError>>()?,
        },
    };

    Ok(cond)
}

pub fn proto_batch_to_program(
    batch: &proto::Batch,
    sqls: &HashMap<i32, String>,
    version: Version,
) -> Result<Program, HranaError> {
    let mut steps = Vec::with_capacity(batch.steps.len());
    for (step_i, step) in batch.steps.iter().enumerate() {
        let query = proto_stmt_to_query(&step.stmt, sqls, version)?;
        let cond = step
            .condition
            .as_ref()
            .map(|cond| proto_cond_to_cond(cond, step_i))
            .transpose()?;
        let step = Step { query, cond };

        steps.push(step);
    }

    Ok(Program::new(steps))
}

pub async fn execute_batch(
    conn: &ConnectionHandle,
    // auth: Authenticated,
    pgm: Program,
) -> Result<proto::BatchResult, HranaError> {
    let (builder, ret) = HranaBatchProtoBuilder::new();
    conn.execute(
        pgm,
        // auth,
        Box::new(builder),
    )
    .await;

    Ok(ret
        .await
        .map_err(|_| crate::error::Error::ConnectionClosed)?)
}

pub fn proto_sequence_to_program(sql: &str) -> Result<Program, HranaError> {
    let stmts = Statement::parse(sql)
        .collect::<crate::Result<Vec<_>, libsqlx::error::Error>>()
        .map_err(|err| StmtError::SqlParse { source: err.into() })?;

    let steps = stmts
        .into_iter()
        .enumerate()
        .map(|(step_i, stmt)| {
            let cond = match step_i {
                0 => None,
                _ => Some(Cond::Ok { step: step_i - 1 }),
            };
            let query = Query {
                stmt,
                params: Params::empty(),
                want_rows: false,
            };
            Step { cond, query }
        })
        .collect::<Arc<[Step]>>();

    Ok(Program { steps })
}

pub async fn execute_sequence(
    conn: &ConnectionHandle,
    // auth: Authenticated,
    pgm: Program,
) -> Result<(), HranaError> {
    let (send, ret) = oneshot::channel();
    let builder = StepResultsBuilder::new(send);
    conn.execute(
        pgm,
        // auth,
        Box::new(builder),
    )
    .await;

    ret.await
        .unwrap()
        .unwrap()
        .into_iter()
        .try_for_each(|result| match result {
            StepResult::Ok => Ok(()),
            StepResult::Err(e) => Err(crate::error::Error::from(e))?,
            StepResult::Skipped => todo!(), // Err(anyhow!("Statement in sequence was not executed")),
        })
}
