use pgwire::api::results::{query_response, DataRowEncoder, FieldFormat, FieldInfo, Response};
use pgwire::api::Type;
use pgwire::{error::ErrorInfo, messages::data::DataRow};
use rusqlite::types::ValueRef;

use crate::query_result_builder::{Column, QueryResultBuilder, QueryResultBuilderError};

pub struct PgResponseBuilder {
    responses: Vec<Response<'static>>,
    include_col_def: bool,
    current_rows: Vec<DataRow>,
    current_row_encoder: Option<DataRowEncoder>,
    current_col_defs: Option<Vec<FieldInfo>>,
    current_err: Option<crate::error::Error>,
    step_ncols: usize,
}

impl PgResponseBuilder {
    pub fn new(include_col_def: bool) -> Self {
        Self {
            responses: Vec::new(),
            include_col_def,
            current_rows: Vec::new(),
            current_row_encoder: None,
            current_col_defs: None,
            current_err: None,
            step_ncols: 0,
        }
    }
}

fn type_from_str(s: &str) -> Type {
    match s.to_lowercase().as_str() {
        "integer" | "int" | "tinyint" | "smallint" | "mediumint" | "bigint"
            | "unsigned big int" | "int2" | "int8" => Type::INT8,
            "real" | "double" | "double precision" | "float" => Type::FLOAT8,
            "text" | "character" | "varchar" | "varying character" | "nchar"
                | "native character" | "nvarchar" | "clob" => Type::TEXT,
            "blob" => Type::BYTEA,
            "numeric" | "decimal" | "boolean" | "date" | "datetime" => Type::NUMERIC,
            _ => Type::UNKNOWN,
    }
}

impl QueryResultBuilder for PgResponseBuilder {
    type Ret = Vec<Response<'static>>;

    fn init(&mut self) -> Result<(), QueryResultBuilderError> {
        *self = Self::new(self.include_col_def);

        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.current_rows.is_empty());
        assert!(self.current_col_defs.is_none());
        assert!(self.current_err.is_none());
        Ok(())
    }

    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        match self.current_err.take() {
            Some(err) => {
                let resp = Response::Error(
                    ErrorInfo::new("ERROR".into(), "XX000".into(), err.to_string()).into(),
                );
                self.current_rows.clear();
                self.current_col_defs = None;
                self.responses.push(resp);
            }
            None => {
                let rows = std::mem::take(&mut self.current_rows);
                let resp = query_response(
                    self.current_col_defs.take(),
                    futures::stream::iter(rows.into_iter().map(Result::Ok)),
                );
                self.responses.push(Response::Query(resp));
            }
        }

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.current_err.is_none());
        self.current_err = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(self.current_col_defs.is_none());

        self.step_ncols = 0;
        if self.include_col_def {
            let desc = cols
                .into_iter()
                .map(|c| {
                    self.step_ncols += 1;
                    let c = c.into();
                    let ty = c
                        .decl_ty
                        .map(type_from_str)
                        .unwrap_or(Type::UNKNOWN);
                    FieldInfo::new(c.name.into(), None, None, ty, FieldFormat::Text)
                })
                .collect();
            self.current_col_defs = Some(desc);
        }

        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.current_row_encoder.is_none());
        self.current_row_encoder = Some(DataRowEncoder::new(self.step_ncols));

        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        let encoder = self
            .current_row_encoder
            .as_mut()
            .expect("current row encoder should have been set in a previous call to begin_row");

        match v {
            ValueRef::Null => {
                encoder
                    .encode_text_format_field(None::<&u8>)
                    .map_err(QueryResultBuilderError::from_any)?;
            }
            ValueRef::Integer(i) => {
                encoder
                    .encode_text_format_field(Some(&i))
                    .map_err(QueryResultBuilderError::from_any)?;
            }
            ValueRef::Real(f) => {
                encoder
                    .encode_text_format_field(Some(&f))
                    .map_err(QueryResultBuilderError::from_any)?;
            }
            ValueRef::Text(t) => {
                encoder
                    .encode_text_format_field(Some(
                        &std::str::from_utf8(t).map_err(QueryResultBuilderError::from_any)?,
                    ))
                    .map_err(QueryResultBuilderError::from_any)?;
            }
            ValueRef::Blob(b) => {
                encoder
                    .encode_text_format_field(Some(&hex::encode(b)))
                    .map_err(QueryResultBuilderError::from_any)?;
            }
        }

        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        let data_row = self
            .current_row_encoder
            .take()
            .unwrap()
            .finish()
            .map_err(QueryResultBuilderError::from_any)?;
        self.current_rows.push(data_row);

        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        self.responses
    }
}
