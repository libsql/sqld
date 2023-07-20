use std::fmt;
use std::io::{self, ErrorKind};

use bytesize::ByteSize;
pub use rusqlite::types::ValueRef;

use crate::database::FrameNo;

#[derive(Debug)]
pub enum QueryResultBuilderError {
    ResponseTooLarge(u64),
    Internal(anyhow::Error),
}

impl fmt::Display for QueryResultBuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResultBuilderError::ResponseTooLarge(s) => {
                write!(f, "query response exceeds the maximum size of {}. Try reducing the number of queried rows.", ByteSize(*s))
            }
            QueryResultBuilderError::Internal(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for QueryResultBuilderError {}

impl From<anyhow::Error> for QueryResultBuilderError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value)
    }
}

impl QueryResultBuilderError {
    pub fn from_any<E: Into<anyhow::Error>>(e: E) -> Self {
        Self::Internal(e.into())
    }
}

impl From<io::Error> for QueryResultBuilderError {
    fn from(value: io::Error) -> Self {
        if value.kind() == ErrorKind::OutOfMemory
            && value.get_ref().is_some()
            && value.get_ref().unwrap().is::<QueryResultBuilderError>()
        {
            return *value
                .into_inner()
                .unwrap()
                .downcast::<QueryResultBuilderError>()
                .unwrap();
        }
        Self::Internal(value.into())
    }
}

/// Identical to rusqlite::Column, with visible fields.
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct Column<'a> {
    pub name: &'a str,
    pub decl_ty: Option<&'a str>,
}

impl<'a> From<(&'a str, Option<&'a str>)> for Column<'a> {
    fn from((name, decl_ty): (&'a str, Option<&'a str>)) -> Self {
        Self { name, decl_ty }
    }
}

impl<'a> From<&'a rusqlite::Column<'a>> for Column<'a> {
    fn from(value: &'a rusqlite::Column<'a>) -> Self {
        Self {
            name: value.name(),
            decl_ty: value.decl_type(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryBuilderConfig {
    pub max_size: Option<u64>,
}

pub trait ResultBuilder: Send + 'static {
    /// (Re)initialize the builder. This method can be called multiple times.
    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// start serializing new step
    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// finish serializing current step
    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// emit an error to serialize.
    fn step_error(&mut self, _error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// add cols description for current step.
    /// This is called called at most once per step, and is always the first method being called
    fn cols_description(
        &mut self,
        _cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// start adding rows
    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// begin a new row for the current step
    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// add value to current row
    fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// finish current row
    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// end adding rows
    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }
    /// finish the builder, and pass the transaction state.
    /// If false is returned, and is_txn is true, then the transaction is rolledback.
    fn finnalize(
        &mut self,
        _is_txn: bool,
        _frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        Ok(true)
    }

    /// There was a fatal error and the request was aborted
    fn finnalize_error(&mut self, _e: String) {}
}

pub trait ResultBuilderExt: ResultBuilder {
    /// Returns a `QueryResultBuilder` that wraps Self and takes at most `n` steps
    fn take(self, limit: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            limit,
            count: 0,
            inner: self,
        }
    }
}

impl<T: ResultBuilder> ResultBuilderExt for T {}

#[derive(Debug)]
pub enum StepResult {
    Ok,
    Err(crate::error::Error),
    Skipped,
}

/// A `QueryResultBuilder` that ignores rows, but records the outcome of each step in a `StepResult`
pub struct StepResultsBuilder<R> {
    current: Option<crate::error::Error>,
    step_results: Vec<StepResult>,
    is_skipped: bool,
    ret: Option<R>,
}

pub trait RetChannel<T>: Send + 'static {
    fn send(self, t: T);
}

#[cfg(feature = "tokio")]
impl<T: Send + 'static> RetChannel<T> for tokio::sync::oneshot::Sender<T> {
    fn send(self, t: T) {
        let _ = self.send(t);
    }
}

impl<R> StepResultsBuilder<R> {
    pub fn new(ret: R) -> Self {
        Self {
            current: None,
            step_results: Vec::new(),
            is_skipped: false,
            ret: Some(ret),
        }
    }
}

impl<R: RetChannel<Vec<StepResult>>> ResultBuilder for StepResultsBuilder<R> {
    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.current = None;
        self.step_results.clear();
        self.is_skipped = false;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.is_skipped = true;
        Ok(())
    }

    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        let res = match self.current.take() {
            Some(e) => StepResult::Err(e),
            None if self.is_skipped => StepResult::Skipped,
            None => StepResult::Ok,
        };

        self.step_results.push(res);

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.current.is_none());
        self.current = Some(error);

        Ok(())
    }

    fn cols_description(
        &mut self,
        _cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        self.is_skipped = false;
        Ok(())
    }

    fn finnalize(
        &mut self,
        _is_txn: bool,
        _frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        self.ret
            .take()
            .expect("finnalize called more than once")
            .send(std::mem::take(&mut self.step_results));
        Ok(true)
    }
}

impl ResultBuilder for () {}

// A builder that wraps another builder, but takes at most `n` steps
pub struct Take<B> {
    limit: usize,
    count: usize,
    inner: B,
}

impl<B> Take<B> {
    pub fn into_inner(self) -> B {
        self.inner
    }
}

impl<B: ResultBuilder> ResultBuilder for Take<B> {
    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.count = 0;
        self.inner.init(config)
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_step()
        } else {
            Ok(())
        }
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner
                .finish_step(affected_row_count, last_insert_rowid)?;
            self.count += 1;
        }

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.step_error(error)
        } else {
            Ok(())
        }
    }

    fn cols_description(
        &mut self,
        cols: &mut dyn Iterator<Item = Column>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.cols_description(cols)
        } else {
            Ok(())
        }
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_rows()
        } else {
            Ok(())
        }
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.begin_row()
        } else {
            Ok(())
        }
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.add_row_value(v)
        } else {
            Ok(())
        }
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.finish_row()
        } else {
            Ok(())
        }
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.count < self.limit {
            self.inner.finish_rows()
        } else {
            Ok(())
        }
    }

    fn finnalize(
        &mut self,
        is_txn: bool,
        frame_no: Option<FrameNo>,
    ) -> Result<bool, QueryResultBuilderError> {
        self.inner.finnalize(is_txn, frame_no)
    }
}

#[cfg(test)]
pub mod test {
    #![allow(dead_code)]
    use std::fmt;

    use arbitrary::{Arbitrary, Unstructured};
    use itertools::Itertools;
    use rand::{
        distributions::{Standard, WeightedIndex},
        prelude::Distribution,
        thread_rng, Fill, Rng,
    };
    use FsmState::*;

    use super::*;

    /// a dummy QueryResultBuilder that encodes the QueryResultBuilder FSM. It can be passed to a
    /// driver to ensure that it is not mis-used

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    #[repr(usize)]
    // do not reorder!
    enum FsmState {
        Init = 0,
        Finish,
        BeginStep,
        FinishStep,
        StepError,
        ColsDescription,
        FinishRows,
        BeginRows,
        FinishRow,
        BeginRow,
        AddRowValue,
        BuilderError,
    }

    #[rustfmt::skip]
    static TRANSITION_TABLE: [[bool; 12]; 12] = [
      //FROM:
      //Init    Finish  BeginStep FinishStep StepError ColsDes FinishRows BegRows FinishRow  BegRow AddRowVal BuidlerErr TO:
        [true , true ,  true ,    true ,     true ,    true ,  true ,     true ,  true ,     true , true ,    false], // Init,
        [true , false,  false,    true ,     false,    false,  false,     false,  false,     false, false,    false], // Finish,
        [true , false,  false,    true ,     false,    false,  false,     false,  false,     false, false,    false], // BeginStep
        [false, false,  true ,    false,     true ,    false,  true ,     false,  false,     false, false,    false], // FinishStep
        [false, false,  true ,    false,     false,    true ,  true ,     true ,  true ,     true , true ,    false], // StepError
        [false, false,  true ,    false,     false,    false,  false,     false,  false,     false, false,    false], // ColsDescr
        [false, false,  false,    false,     false,    false,  false,     true ,  true ,     false, false,    false], // FinishRows
        [false, false,  false,    false,     false,    true ,  false,     false,  false,     false, false,    false], // BeginRows
        [false, false,  false,    false,     false,    false,  false,     false,  false,     true , true ,    false], // FinishRow
        [false, false,  false,    false,     false,    false,  false,     true ,  true ,     false, false,    false], // BeginRow,
        [false, false,  false,    false,     false,    false,  false,     false,  false,     true , true ,    false], // AddRowValue
        [true , true ,  true ,    true ,     true ,    true ,  true ,     true ,  true ,     true , true ,    false], // BuilderError
    ];

    impl FsmState {
        /// returns a random valid transition from the current state
        fn rand_transition(self, allow_init: bool) -> Self {
            let valid_next_states = TRANSITION_TABLE[..TRANSITION_TABLE.len() - 1] // ignore
                // builder error
                .iter()
                .enumerate()
                .skip(if allow_init { 0 } else { 1 })
                .filter_map(|(i, ss)| ss[self as usize].then_some(i))
                .collect_vec();
            // distribution is somewhat tweaked to be biased towards more real-world test cases
            let weigths = valid_next_states
                .iter()
                .enumerate()
                .map(|(p, i)| i.pow(p as _))
                .collect_vec();
            let dist = WeightedIndex::new(weigths).unwrap();
            unsafe { std::mem::transmute(valid_next_states[dist.sample(&mut thread_rng())]) }
        }

        /// moves towards the finish step as fast as possible
        fn toward_finish(self) -> Self {
            match self {
                Init => Finish,
                BeginStep => FinishStep,
                FinishStep => Finish,
                StepError => FinishStep,
                BeginRows | BeginRow | AddRowValue | FinishRow | FinishRows | ColsDescription => {
                    StepError
                }
                Finish => Finish,
                BuilderError => Finish,
            }
        }
    }

    pub fn random_builder_driver<B: ResultBuilder>(mut max_steps: usize, mut b: B) -> B {
        let mut rand_data = [0; 10_000];
        rand_data.try_fill(&mut rand::thread_rng()).unwrap();
        let mut u = Unstructured::new(&rand_data);
        let mut trace = Vec::new();

        #[derive(Arbitrary)]
        pub enum ValueRef<'a> {
            Null,
            Integer(i64),
            Real(f64),
            Text(&'a str),
            Blob(&'a [u8]),
        }

        impl<'a> From<ValueRef<'a>> for rusqlite::types::ValueRef<'a> {
            fn from(value: ValueRef<'a>) -> Self {
                match value {
                    ValueRef::Null => rusqlite::types::ValueRef::Null,
                    ValueRef::Integer(i) => rusqlite::types::ValueRef::Integer(i),
                    ValueRef::Real(x) => rusqlite::types::ValueRef::Real(x),
                    ValueRef::Text(s) => rusqlite::types::ValueRef::Text(s.as_bytes()),
                    ValueRef::Blob(b) => rusqlite::types::ValueRef::Blob(b),
                }
            }
        }

        let mut state = Init;
        trace.push(state);
        loop {
            match state {
                Init => b.init(&QueryBuilderConfig::default()).unwrap(),
                BeginStep => b.begin_step().unwrap(),
                FinishStep => b
                    .finish_step(
                        Arbitrary::arbitrary(&mut u).unwrap(),
                        Arbitrary::arbitrary(&mut u).unwrap(),
                    )
                    .unwrap(),
                StepError => b.step_error(crate::error::Error::LibSqlTxBusy).unwrap(),
                ColsDescription => b
                    .cols_description(&mut <Vec<Column>>::arbitrary(&mut u).unwrap().into_iter())
                    .unwrap(),
                BeginRows => b.begin_rows().unwrap(),
                BeginRow => b.begin_row().unwrap(),
                AddRowValue => b
                    .add_row_value(ValueRef::arbitrary(&mut u).unwrap().into())
                    .unwrap(),
                FinishRow => b.finish_row().unwrap(),
                FinishRows => b.finish_rows().unwrap(),
                Finish => {
                    b.finnalize(false, None).unwrap();
                    break;
                }
                BuilderError => return b,
            }

            if max_steps > 0 {
                state = state.rand_transition(false);
            } else {
                state = state.toward_finish()
            }

            trace.push(state);

            max_steps = max_steps.saturating_sub(1);
        }

        // this can be usefull to help debug the generated test case
        dbg!(trace);

        b
    }

    pub struct FsmQueryBuilder {
        state: FsmState,
        inject_errors: bool,
    }

    impl fmt::Display for FsmState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                Init => "init",
                BeginStep => "begin_step",
                FinishStep => "finish_step",
                StepError => "step_error",
                ColsDescription => "cols_description",
                BeginRows => "begin_rows",
                BeginRow => "begin_row",
                AddRowValue => "add_row_value",
                FinishRow => "finish_row",
                FinishRows => "finish_rows",
                Finish => "finish",
                BuilderError => "a builder error",
            };

            f.write_str(s)
        }
    }

    impl FsmQueryBuilder {
        fn new(inject_errors: bool) -> Self {
            Self {
                state: Init,
                inject_errors,
            }
        }

        fn transition(&mut self, to: FsmState) -> Result<(), QueryResultBuilderError> {
            let from = self.state as usize;
            if TRANSITION_TABLE[to as usize][from] {
                self.state = to;
            } else {
                panic!("{} can't be called after {}", to, self.state);
            }

            Ok(())
        }

        fn maybe_inject_error(&mut self) -> Result<(), QueryResultBuilderError> {
            if self.inject_errors {
                let val: f32 = thread_rng().sample(Standard);
                // < 0.1% change to generate error
                if val < 0.001 {
                    self.state = BuilderError;
                    Err(anyhow::anyhow!("dummy"))?;
                }
            }

            Ok(())
        }
    }

    impl ResultBuilder for FsmQueryBuilder {
        fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(Init)
        }

        fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginStep)
        }

        fn finish_step(
            &mut self,
            _affected_row_count: u64,
            _last_insert_rowid: Option<i64>,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishStep)
        }

        fn step_error(
            &mut self,
            _error: crate::error::Error,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(StepError)
        }

        fn cols_description(
            &mut self,
            _cols: &mut dyn Iterator<Item = Column>,
        ) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(ColsDescription)
        }

        fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginRows)
        }

        fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(BeginRow)
        }

        fn add_row_value(&mut self, _v: ValueRef) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(AddRowValue)
        }

        fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishRow)
        }

        fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(FinishRows)
        }

        fn finnalize(
            &mut self,
            _is_txn: bool,
            _frame_no: Option<FrameNo>,
        ) -> Result<bool, QueryResultBuilderError> {
            self.maybe_inject_error()?;
            self.transition(Finish)?;
            Ok(true)
        }
    }

    pub fn test_driver(
        iter: usize,
        f: impl Fn(FsmQueryBuilder) -> Result<FsmQueryBuilder, crate::error::Error>,
    ) {
        for _ in 0..iter {
            // inject random errors
            let builder = FsmQueryBuilder::new(true);
            match f(builder) {
                Ok(b) => {
                    assert_eq!(b.state, Finish);
                }
                Err(e) => {
                    assert!(matches!(e, crate::error::Error::BuilderError(_)));
                }
            }
        }
    }

    #[test]
    fn test_fsm_ok() {
        let mut builder = FsmQueryBuilder::new(false);
        builder.init(&QueryBuilderConfig::default()).unwrap();

        builder.begin_step().unwrap();
        builder
            .cols_description(&mut [("hello", None).into()].into_iter())
            .unwrap();
        builder.begin_rows().unwrap();
        builder.begin_row().unwrap();
        builder.add_row_value(ValueRef::Null).unwrap();
        builder.finish_row().unwrap();
        builder
            .step_error(crate::error::Error::LibSqlTxBusy)
            .unwrap();
        builder.finish_step(0, None).unwrap();

        builder.begin_step().unwrap();
        builder
            .cols_description(&mut [("hello", None).into()].into_iter())
            .unwrap();
        builder.begin_rows().unwrap();
        builder.begin_row().unwrap();
        builder.add_row_value(ValueRef::Null).unwrap();
        builder.finish_row().unwrap();
        builder.finish_rows().unwrap();
        builder.finish_step(0, None).unwrap();

        builder.finnalize(false, None).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_fsm_invalid() {
        let mut builder = FsmQueryBuilder::new(false);
        builder.init(&QueryBuilderConfig::default()).unwrap();
        builder.begin_step().unwrap();
        builder.begin_rows().unwrap();
    }

    #[allow(dead_code)]
    fn is_trait_objectifiable(_: Box<dyn ResultBuilder>) {}
}
