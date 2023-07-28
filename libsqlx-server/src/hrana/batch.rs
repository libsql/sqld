use std::collections::HashMap;

use crate::allocation::ConnectionHandle;
use crate::hrana::stmt::StmtError;

use super::result_builder::HranaBatchProtoBuilder;
use super::stmt::{proto_stmt_to_query, stmt_error_from_sqld_error};
use super::{proto, ProtocolError, Version};

use color_eyre::eyre::anyhow;
use libsqlx::analysis::Statement;
use libsqlx::program::{Cond, Program, Step};
use libsqlx::query::{Params, Query};
use libsqlx::result_builder::{StepResult, StepResultsBuilder};
use tokio::sync::oneshot;

fn proto_cond_to_cond(cond: &proto::BatchCond, max_step_i: usize) -> color_eyre::Result<Cond> {
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
                .collect::<color_eyre::Result<_>>()?,
        },
        proto::BatchCond::Or { conds } => Cond::Or {
            conds: conds
                .iter()
                .map(|cond| proto_cond_to_cond(cond, max_step_i))
                .collect::<color_eyre::Result<_>>()?,
        },
    };

    Ok(cond)
}

pub fn proto_batch_to_program(
    batch: &proto::Batch,
    sqls: &HashMap<i32, String>,
    version: Version,
) -> color_eyre::Result<Program> {
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
    db: &ConnectionHandle,
    pgm: Program,
) -> color_eyre::Result<proto::BatchResult> {
    let (builder, ret) = HranaBatchProtoBuilder::new();
    db.execute(pgm, Box::new(builder)).await;

    Ok(ret.await?)
}

pub fn proto_sequence_to_program(sql: &str) -> color_eyre::Result<Program> {
    let stmts = Statement::parse(sql)
        .collect::<libsqlx::Result<Vec<_>>>()
        .map_err(|err| anyhow!(StmtError::SqlParse { source: err.into() }))?;

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
        .collect();

    Ok(Program { steps })
}

pub async fn execute_sequence(conn: &ConnectionHandle, pgm: Program) -> color_eyre::Result<()> {
    let (snd, rcv) = oneshot::channel();
    let builder = StepResultsBuilder::new(snd);
    conn.execute(pgm, Box::new(builder)).await;

    rcv.await?
        .map_err(|e| anyhow!("{e}"))?
        .into_iter()
        .try_for_each(|result| match result {
            StepResult::Ok => Ok(()),
            StepResult::Err(e) => match stmt_error_from_sqld_error(e) {
                Ok(stmt_err) => Err(anyhow!(stmt_err)),
                Err(sqld_err) => Err(anyhow!(sqld_err)),
            },
            StepResult::Skipped => Err(anyhow!("Statement in sequence was not executed")),
        })
}
