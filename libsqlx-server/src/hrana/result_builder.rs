use std::fmt::{self, Write as _};
use std::io;

use bytes::Bytes;
use libsqlx::{result_builder::*, FrameNo};
use tokio::sync::oneshot;

use crate::hrana::stmt::proto_error_from_stmt_error;

use super::error::HranaError;
use super::proto;

pub struct SingleStatementBuilder {
    builder: StatementBuilder,
    ret: Option<oneshot::Sender<crate::Result<proto::StmtResult, HranaError>>>,
}

impl SingleStatementBuilder {
    pub fn new() -> (
        Self,
        oneshot::Receiver<crate::Result<proto::StmtResult, HranaError>>,
    ) {
        let (ret, rcv) = oneshot::channel();
        (
            Self {
                builder: StatementBuilder::default(),
                ret: Some(ret),
            },
            rcv,
        )
    }
}

impl ResultBuilder for SingleStatementBuilder {
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

    fn step_error(&mut self, error: libsqlx::error::Error) -> Result<(), QueryResultBuilderError> {
        self.builder.step_error(error)
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        self.builder.cols_description(cols)
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.builder.begin_row()
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.builder.add_row_value(v)
    }

    fn finnalize(
        &mut self,
        _is_txn: bool,
        _frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        let res = self.builder.take_ret();
        let _ = self.ret.take().unwrap().send(res);
        Ok(true)
    }

    fn finnalize_error(&mut self, _e: String) {
        todo!()
    }
}

#[derive(Debug, Default)]
struct StatementBuilder {
    has_step: bool,
    cols: Vec<proto::Col>,
    rows: Vec<Vec<proto::Value>>,
    err: Option<libsqlx::error::Error>,
    affected_row_count: u64,
    last_insert_rowid: Option<i64>,
    current_size: u64,
    max_response_size: u64,
}

impl StatementBuilder {
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        *self = Self {
            max_response_size: config.max_size.unwrap_or(u64::MAX),
            ..Default::default()
        };

        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        // SingleStatementBuilder only builds a single statement
        assert!(!self.has_step);
        self.has_step = true;
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.last_insert_rowid = last_insert_rowid;
        self.affected_row_count = affected_row_count;

        Ok(())
    }

    fn step_error(&mut self, error: libsqlx::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        let mut f = SizeFormatter(0);
        write!(&mut f, "{error}").unwrap();
        self.current_size = f.0;

        self.err = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.cols.is_empty());

        let mut cols_size = 0;

        self.cols.extend(cols.into_iter().map(Into::into).map(|c| {
            cols_size += estimate_cols_json_size(&c);
            proto::Col {
                name: Some(c.name.to_owned()),
                decltype: c.decl_ty.map(ToString::to_string),
            }
        }));

        self.current_size += cols_size;
        if self.current_size > self.max_response_size {
            return Err(QueryResultBuilderError::ResponseTooLarge(
                self.max_response_size,
            ));
        }

        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        self.rows.push(Vec::with_capacity(self.cols.len()));
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        let estimate_size = value_json_size(&v);
        if self.current_size + estimate_size > self.max_response_size {
            return Err(QueryResultBuilderError::ResponseTooLarge(
                self.max_response_size,
            ));
        }

        self.current_size += estimate_size;

        let val = match v {
            ValueRef::Null => proto::Value::Null,
            ValueRef::Integer(value) => proto::Value::Integer { value },
            ValueRef::Real(value) => proto::Value::Float { value },
            ValueRef::Text(s) => proto::Value::Text {
                value: String::from_utf8(s.to_vec())
                    .map_err(QueryResultBuilderError::from_any)?
                    .into(),
            },
            ValueRef::Blob(d) => proto::Value::Blob {
                value: Bytes::copy_from_slice(d),
            },
        };

        self.rows
            .last_mut()
            .expect("row must be initialized")
            .push(val);

        Ok(())
    }

    pub fn take_ret(&mut self) -> crate::Result<proto::StmtResult, HranaError> {
        match self.err.take() {
            Some(err) => Err(crate::error::Error::from(err))?,
            None => Ok(proto::StmtResult {
                cols: std::mem::take(&mut self.cols),
                rows: std::mem::take(&mut self.rows),
                affected_row_count: self.affected_row_count,
                last_insert_rowid: self.last_insert_rowid,
            }),
        }
    }
}

struct SizeFormatter(u64);

impl io::Write for SizeFormatter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Write for SizeFormatter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0 += s.len() as u64;
        Ok(())
    }
}

fn value_json_size(v: &ValueRef) -> u64 {
    let mut f = SizeFormatter(0);
    match v {
        ValueRef::Null => write!(&mut f, r#"{{"type":"null"}}"#).unwrap(),
        ValueRef::Integer(i) => write!(&mut f, r#"{{"type":"integer", "value": "{i}"}}"#).unwrap(),
        ValueRef::Real(x) => write!(&mut f, r#"{{"type":"integer","value": {x}"}}"#).unwrap(),
        ValueRef::Text(s) => {
            // error will be caught later.
            if let Ok(s) = std::str::from_utf8(s) {
                write!(&mut f, r#"{{"type":"text","value":"{s}"}}"#).unwrap()
            }
        }
        ValueRef::Blob(b) => return b.len() as u64,
    }

    f.0
}

fn estimate_cols_json_size(c: &Column) -> u64 {
    let mut f = SizeFormatter(0);
    write!(
        &mut f,
        r#"{{"name":"{}","decltype":"{}"}}"#,
        c.name,
        c.decl_ty.unwrap_or("null")
    )
    .unwrap();
    f.0
}

#[derive(Debug)]
pub struct HranaBatchProtoBuilder {
    step_results: Vec<Option<proto::StmtResult>>,
    step_errors: Vec<Option<crate::hrana::proto::Error>>,
    stmt_builder: StatementBuilder,
    current_size: u64,
    max_response_size: u64,
    step_empty: bool,
    ret: Option<oneshot::Sender<proto::BatchResult>>,
}

impl HranaBatchProtoBuilder {
    pub fn new() -> (Self, oneshot::Receiver<proto::BatchResult>) {
        let (ret, rcv) = oneshot::channel();
        (
            Self {
                step_results: Vec::new(),
                step_errors: Vec::new(),
                stmt_builder: StatementBuilder::default(),
                current_size: 0,
                max_response_size: u64::MAX,
                step_empty: false,
                ret: Some(ret),
            },
            rcv,
        )
    }

    pub fn into_ret(&mut self) -> proto::BatchResult {
        proto::BatchResult {
            step_results: std::mem::take(&mut self.step_results),
            step_errors: std::mem::take(&mut self.step_errors),
        }
    }
}

impl ResultBuilder for HranaBatchProtoBuilder {
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.max_response_size = config.max_size.unwrap_or(u64::MAX);
        self.stmt_builder.init(config)?;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.step_empty = true;
        self.stmt_builder.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder
            .finish_step(affected_row_count, last_insert_rowid)?;
        self.current_size += self.stmt_builder.current_size;

        let new_builder = StatementBuilder {
            current_size: 0,
            max_response_size: self.max_response_size - self.current_size,
            ..Default::default()
        };
        match std::mem::replace(&mut self.stmt_builder, new_builder).take_ret() {
            Ok(res) => {
                self.step_results.push((!self.step_empty).then_some(res));
                self.step_errors.push(None);
            }
            Err(e) => {
                self.step_results.push(None);
                self.step_errors.push(Some(proto_error_from_stmt_error(
                    Err(HranaError::from(e)).map_err(QueryResultBuilderError::from_any)?,
                )));
            }
        }

        Ok(())
    }

    fn step_error(&mut self, error: libsqlx::error::Error) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.step_error(error)
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        self.step_empty = false;
        self.stmt_builder.cols_description(cols)
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_row()
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.add_row_value(v)
    }

    fn finnalize(
        &mut self,
        _is_txn: bool,
        _frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        if let Some(ret) = self.ret.take() {
            let _ = ret.send(self.into_ret());
        }

        Ok(false)
    }

    fn finnalize_error(&mut self, _e: String) {
        todo!()
    }
}
