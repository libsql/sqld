use bytes::Bytes;
use rusqlite::types::ValueRef;

use crate::hrana::stmt::{proto_error_from_stmt_error, stmt_error_from_sqld_error};
use crate::query_result_builder::{Column, QueryResultBuilder, QueryResultBuilderError};

use super::proto;

#[derive(Debug, Default)]
pub struct SingleStatementBuilder {
    has_step: bool,
    cols: Vec<proto::Col>,
    rows: Vec<Vec<proto::Value>>,
    err: Option<crate::error::Error>,
    affected_row_count: u64,
    last_insert_rowid: Option<i64>,
}

impl QueryResultBuilder for SingleStatementBuilder {
    type Ret = Result<proto::StmtResult, crate::error::Error>;

    fn init(&mut self) -> Result<(), QueryResultBuilderError> {
        *self = Default::default();
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

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());

        self.err = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.cols.is_empty());

        self.cols
            .extend(cols.into_iter().map(Into::into).map(|c| proto::Col {
                name: Some(c.name.to_owned()),
                decltype: c.decl_ty.map(ToString::to_string),
            }));

        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.rows.is_empty());
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        self.rows.push(Vec::with_capacity(self.cols.len()));
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
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

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        match self.err {
            Some(err) => Err(err),
            None => Ok(proto::StmtResult {
                cols: self.cols,
                rows: self.rows,
                affected_row_count: self.affected_row_count,
                last_insert_rowid: self.last_insert_rowid,
            }),
        }
    }
}

#[derive(Debug, Default)]
pub struct HranaBatchProtoBuilder {
    step_results: Vec<Option<proto::StmtResult>>,
    step_errors: Vec<Option<crate::hrana::proto::Error>>,
    stmt_builder: SingleStatementBuilder,
}

impl QueryResultBuilder for HranaBatchProtoBuilder {
    type Ret = proto::BatchResult;

    fn init(&mut self) -> Result<(), QueryResultBuilderError> {
        *self = Default::default();
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder
            .finish_step(affected_row_count, last_insert_rowid)?;

        match std::mem::take(&mut self.stmt_builder).into_ret() {
            Ok(res) => {
                self.step_results.push(Some(res));
                self.step_errors.push(None);
            }
            Err(e) => {
                self.step_results.push(None);
                self.step_errors.push(Some(proto_error_from_stmt_error(
                    &stmt_error_from_sqld_error(e).map_err(QueryResultBuilderError::from_any)?,
                )));
            }
        }

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.step_error(error)
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.cols_description(cols)
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_rows()
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_row()
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.add_row_value(v)
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.finish_row()
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        proto::BatchResult {
            step_results: self.step_results,
            step_errors: self.step_errors,
        }
    }
}

// TODO: this is quite annoying to integrate with hrana, but saves a bunch of allocations!
// struct HranaBatchJsonSerializer<F> {
//     formatter: F,
//     /// buffer holding the data being serialized
//     buffer: Vec<u8>,
//     /// current step is an error
//     step_error: bool,
//     /// buffer position to rollback to
//     checkpoint: usize,
//     /// number of row in the current step
//     step_row_count: usize,
//     /// buffered errors
//     error_buffer: Vec<Option<crate::Error>>,
//     /// current number of steps
//     step_count: usize,
//     /// number of col in the current row
//     row_col_count: usize,
// }
//
// impl<F> HranaBatchJsonSerializer<F> {
//     fn new(formatter: F) -> Self {
//         Self {
//             formatter,
//             buffer: Vec::new(),
//             step_error: false,
//             checkpoint: 0,
//             step_row_count: 0,
//             error_buffer: Vec::new(),
//             step_count: 0,
//             row_col_count: 0,
//         }
//     }
//
//     fn into_buffer(self) -> Vec<u8> {
//         self.buffer
//     }
//
//     fn serialize_errors(&mut self) -> anyhow::Result<()>
//     where
//         F: Formatter,
//     {
//         self.formatter.begin_object_key(&mut self.buffer, false)?;
//         serde_json::to_writer(&mut self.buffer, "step_errors")?;
//         self.formatter.end_object_key(&mut self.buffer)?;
//
//         self.formatter.begin_object_value(&mut self.buffer)?;
//         self.formatter.begin_array(&mut self.buffer)?;
//         let mut first = true;
//         for err in std::mem::take(&mut self.error_buffer).drain(..) {
//             self.serialize_array_value(&err.as_ref().map(ToString::to_string), first)?;
//             first = false;
//         }
//         self.formatter.end_array(&mut self.buffer)?;
//         self.formatter.end_object_value(&mut self.buffer)?;
//
//         Ok(())
//     }
//
//     fn serialize_key_value<V>(&mut self, k: &str, v: &V, first: bool) -> anyhow::Result<()>
//     where
//         V: Serialize + Sized,
//         F: Formatter,
//     {
//         self.serialize_key(k, first)?;
//         self.serialize_value(v)?;
//
//         Ok(())
//     }
//
//     fn serialize_key(&mut self, key: &str, first: bool) -> anyhow::Result<()>
//     where
//         F: Formatter,
//     {
//         self.formatter.begin_object_key(&mut self.buffer, first)?;
//         serde_json::to_writer(&mut self.buffer, key)?;
//         self.formatter.end_object_key(&mut self.buffer)?;
//         Ok(())
//     }
//
//     fn serialize_value<V>(&mut self, v: &V) -> anyhow::Result<()>
//     where
//         V: Serialize,
//         F: Formatter,
//     {
//         self.formatter.begin_object_value(&mut self.buffer)?;
//         serde_json::to_writer(&mut self.buffer, v)?;
//         self.formatter.end_object_value(&mut self.buffer)?;
//
//         Ok(())
//     }
//
//     fn serialize_array_value<V>(&mut self, v: &V, first: bool) -> anyhow::Result<()>
//     where
//         V: Serialize + Sized,
//         F: Formatter,
//     {
//         self.formatter.begin_array_value(&mut self.buffer, first)?;
//         serde_json::to_writer(&mut self.buffer, v)?;
//         self.formatter.end_array_value(&mut self.buffer)?;
//         Ok(())
//     }
// }
//
// struct HranaValueSerializer<'a>(&'a ValueRef<'a>);
//
// impl<'a> Serialize for HranaValueSerializer<'a> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         #[derive(Serialize)]
//         #[serde(tag = "type", rename_all = "lowercase")]
//         enum HranaValue<'a> {
//             Null,
//             Text { value: &'a str },
//             Blob { base64: &'a [u8] },
//             Float { value: f64 },
//             Integer { value: i64 },
//         }
//
//         let value = match self.0 {
//             ValueRef::Null => HranaValue::Null,
//             ValueRef::Integer(i) => HranaValue::Integer { value: *i },
//             ValueRef::Real(x) => HranaValue::Float { value: *x },
//             ValueRef::Text(value) => HranaValue::Text { value:  std::str::from_utf8(value).expect("invalid string") },
//             ValueRef::Blob(base64) => HranaValue::Blob { base64 },
//         };
//
//         value.serialize(serializer)
//     }
// }
//
// impl<F: Formatter> QueryResultBuilder for HranaBatchJsonSerializer<F> {
//     type Ret = Vec<u8>;
//
//     fn init(&mut self) -> anyhow::Result<()> {
//         self.formatter.begin_object(&mut self.buffer)?;
//
//         self.serialize_key_value("type", &"batch", true)?;
//
//         self.serialize_key("result", false)?;
//
//         self.formatter.begin_object_value(&mut self.buffer)?;
//         self.formatter.begin_object(&mut self.buffer)?;
//         self.serialize_key("step_results", true)?;
//         self.formatter.begin_object_value(&mut self.buffer)?;
//         self.formatter.begin_array(&mut self.buffer)?;
//
//         Ok(())
//     }
//
//     fn begin_step(&mut self) -> anyhow::Result<()> {
//         self.checkpoint = self.buffer.len();
//         self.formatter
//             .begin_array_value(&mut self.buffer, self.step_count == 0)?;
//         self.formatter.begin_object(&mut self.buffer)?;
//
//         Ok(())
//     }
//
//     fn finish_step(
//         &mut self,
//         affected_row_count: usize,
//         last_insert_rowid: Option<usize>,
//     ) -> anyhow::Result<()> {
//         if !self.step_error {
//             self.serialize_key_value("affected_row_count", &affected_row_count, false)?;
//             self.serialize_key_value("last_inserted_row_id", &last_inserted_row_id, false)?;
//             self.formatter.end_object(&mut self.buffer)?;
//         } else {
//             self.serialize_array_value(&None::<()>, self.step_count == 0)?;
//         }
//         self.step_error = false;
//         self.step_count += 1;
//         Ok(())
//     }
//
//     fn error(&mut self, error: crate::error::Error) -> anyhow::Result<()> {
//         self.buffer.truncate(self.checkpoint);
//         self.step_error = true;
//         self.error_buffer.push(Some(error));
//         Ok(())
//     }
//
//     fn cols_description(&mut self, cols: Vec<String>) -> anyhow::Result<()> {
//         self.serialize_key_value("cols", &cols, true)?;
//
//         Ok(())
//     }
//
//     fn begin_rows(&mut self) -> anyhow::Result<()> {
//         self.serialize_key("rows", false)?;
//
//         self.formatter.begin_object_value(&mut self.buffer)?;
//         self.formatter.begin_array(&mut self.buffer)?;
//         self.step_row_count = 0;
//
//         Ok(())
//     }
//
//     fn begin_row(&mut self) -> anyhow::Result<()> {
//         self.formatter
//             .begin_array_value(&mut self.buffer, self.step_row_count == 0)?;
//
//         self.row_col_count = 0;
//         self.formatter.begin_array(&mut self.buffer)?;
//         Ok(())
//     }
//
//     fn add_row_value(&mut self, val: &ValueRef) -> anyhow::Result<()> {
//         self.serialize_array_value(&HranaValueSerializer(val), self.row_col_count == 0)?;
//         self.row_col_count += 1;
//         Ok(())
//     }
//
//     fn finish_row(&mut self) -> anyhow::Result<()> {
//         self.formatter.end_array(&mut self.buffer)?;
//
//         self.formatter.end_array_value(&mut self.buffer)?;
//
//         self.step_row_count += 1;
//         Ok(())
//     }
//
//     fn finish_rows(&mut self) -> anyhow::Result<()> {
//         self.formatter.end_array(&mut self.buffer)?;
//         self.formatter.end_object_value(&mut self.buffer)?;
//         self.error_buffer.push(None);
//         Ok(())
//     }
//
//     fn finish(&mut self) -> anyhow::Result<()> {
//         self.formatter.end_array(&mut self.buffer)?;
//         self.formatter.end_object_value(&mut self.buffer)?;
//
//         self.serialize_errors()?;
//
//         self.formatter.end_object(&mut self.buffer)?;
//         self.formatter.end_object_value(&mut self.buffer)?;
//         self.formatter.end_object(&mut self.buffer)?;
//         Ok(())
//     }
//
//     fn into_ret(self) -> Self::Ret {
//         self.buffer
//     }
// }
