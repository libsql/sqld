use std::path::PathBuf;
use std::sync::Arc;

use futures_core::future::BoxFuture;
use parking_lot::Mutex as PMutex;
use rusqlite::types::ValueRef;
use sqld_libsql_bindings::wal_hook::{TransparentMethods, TRANSPARENT_METHODS};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_stream::StreamExt;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use uuid::Uuid;

use crate::auth::Authenticated;
use crate::error::Error;
use crate::namespace::NamespaceName;
use crate::query::Value;
use crate::query_analysis::TxnStatus;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;
use crate::rpc::proxy::rpc::proxy_client::ProxyClient;
use crate::rpc::proxy::rpc::{
    self, AddRowValue, ColsDescription, DisconnectMessage, ExecReq, ExecResp, Finish, FinishStep,
    StepError,
};
use crate::rpc::NAMESPACE_METADATA_KEY;
use crate::stats::Stats;
use crate::{Result, DEFAULT_AUTO_CHECKPOINT};

use super::config::DatabaseConfigStore;
use super::libsql::{LibSqlConnection, MakeLibSqlConn};
use super::program::DescribeResult;
use super::Connection;
use super::{MakeConnection, Program};

pub struct MakeWriteProxyConn {
    client: ProxyClient<Channel>,
    stats: Arc<Stats>,
    applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    max_response_size: u64,
    max_total_response_size: u64,
    namespace: NamespaceName,
    make_read_only_conn: MakeLibSqlConn<TransparentMethods>,
}

impl MakeWriteProxyConn {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        db_path: PathBuf,
        extensions: Arc<[PathBuf]>,
        channel: Channel,
        uri: tonic::transport::Uri,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        max_response_size: u64,
        max_total_response_size: u64,
        namespace: NamespaceName,
    ) -> crate::Result<Self> {
        let client = ProxyClient::with_origin(channel, uri);
        let make_read_only_conn = MakeLibSqlConn::new(
            db_path.clone(),
            &TRANSPARENT_METHODS,
            || (),
            stats.clone(),
            config_store.clone(),
            extensions.clone(),
            max_response_size,
            max_total_response_size,
            DEFAULT_AUTO_CHECKPOINT,
            applied_frame_no_receiver.clone(),
        )
        .await?;

        Ok(Self {
            client,
            stats,
            applied_frame_no_receiver,
            max_response_size,
            max_total_response_size,
            namespace,
            make_read_only_conn,
        })
    }
}

#[async_trait::async_trait]
impl MakeConnection for MakeWriteProxyConn {
    type Connection = WriteProxyConnection;
    async fn create(&self) -> Result<Self::Connection> {
        let db = WriteProxyConnection::new(
            self.client.clone(),
            self.stats.clone(),
            self.applied_frame_no_receiver.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
                max_total_size: Some(self.max_total_response_size),
                auto_checkpoint: DEFAULT_AUTO_CHECKPOINT,
            },
            self.namespace.clone(),
            self.make_read_only_conn.create().await?,
        )
        .await?;
        Ok(db)
    }
}

#[derive(Debug)]
pub struct WriteProxyConnection {
    /// Lazily initialized read connection
    read_conn: LibSqlConnection<TransparentMethods>,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<TxnStatus>,
    client_id: Uuid,
    /// FrameNo of the last write performed by this connection on the primary.
    /// any subsequent read on this connection must wait for the replicator to catch up with this
    /// frame_no
    last_write_frame_no: PMutex<Option<FrameNo>>,
    /// Notifier from the repliator of the currently applied frameno
    applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    builder_config: QueryBuilderConfig,
    stats: Arc<Stats>,
    namespace: NamespaceName,

    remote_conn: Mutex<Option<RemoteConnection>>,
}

impl WriteProxyConnection {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        write_proxy: ProxyClient<Channel>,
        stats: Arc<Stats>,
        applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        builder_config: QueryBuilderConfig,
        namespace: NamespaceName,
        read_conn: LibSqlConnection<TransparentMethods>,
    ) -> Result<Self> {
        Ok(Self {
            read_conn,
            write_proxy,
            state: Mutex::new(TxnStatus::Init),
            client_id: Uuid::new_v4(),
            last_write_frame_no: Default::default(),
            applied_frame_no_receiver,
            builder_config,
            stats,
            namespace,
            remote_conn: Default::default(),
        })
    }

    async fn with_remote_conn<F, Ret>(
        &self,
        auth: Authenticated,
        builder_config: QueryBuilderConfig,
        cb: F,
    ) -> crate::Result<Ret>
    where
        F: FnOnce(&mut RemoteConnection) -> BoxFuture<'_, crate::Result<Ret>>,
    {
        let mut remote_conn = self.remote_conn.lock().await;
        // TODO: catch broken connection, and reset it to None.
        if remote_conn.is_some() {
            cb(remote_conn.as_mut().unwrap()).await
        } else {
            let conn = RemoteConnection::connect(
                self.write_proxy.clone(),
                self.namespace.clone(),
                auth,
                builder_config,
            )
            .await?;
            let conn = remote_conn.insert(conn);
            cb(conn).await
        }
    }

    async fn execute_remote<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        status: &mut TxnStatus,
        auth: Authenticated,
        builder: B,
    ) -> Result<(B, TxnStatus)> {
        self.stats.inc_write_requests_delegated();
        *status = TxnStatus::Invalid;
        let res = self
            .with_remote_conn(auth, self.builder_config, |conn| {
                Box::pin(conn.execute(pgm, builder))
            })
            .await;

        let (builder, new_status, new_frame_no) = match res {
            Ok(res) => res,
            Err(e @ Error::StreamDisconnect) => {
                // drop the connection
                self.remote_conn.lock().await.take();
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        *status = new_status;
        if let Some(current_frame_no) = new_frame_no {
            self.update_last_write_frame_no(current_frame_no);
        }

        Ok((builder, new_status))
    }

    fn update_last_write_frame_no(&self, new_frame_no: FrameNo) {
        let mut last_frame_no = self.last_write_frame_no.lock();
        if last_frame_no.is_none() || new_frame_no > last_frame_no.unwrap() {
            *last_frame_no = Some(new_frame_no);
        }
    }

    /// wait for the replicator to have caught up with the replication_index if `Some` or our
    /// current write frame_no
    async fn wait_replication_sync(&self, replication_index: Option<FrameNo>) -> Result<()> {
        let current_fno = replication_index.or_else(|| *self.last_write_frame_no.lock());
        match current_fno {
            Some(current_frame_no) => {
                let mut receiver = self.applied_frame_no_receiver.clone();
                receiver
                    .wait_for(|last_applied| match last_applied {
                        Some(x) => *x >= current_frame_no,
                        None => true,
                    })
                    .await
                    .map_err(|_| Error::ReplicatorExited)?;

                Ok(())
            }
            None => Ok(()),
        }
    }
}

struct RemoteConnection {
    response_stream: Streaming<ExecResp>,
    request_sender: mpsc::Sender<ExecReq>,
    current_request_id: u32,
    builder_config: QueryBuilderConfig,
}

impl RemoteConnection {
    async fn connect(
        mut client: ProxyClient<Channel>,
        namespace: NamespaceName,
        auth: Authenticated,
        builder_config: QueryBuilderConfig,
    ) -> crate::Result<Self> {
        let (request_sender, receiver) = mpsc::channel(1);

        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let mut req = Request::new(stream);
        let namespace = BinaryMetadataValue::from_bytes(namespace.as_slice());
        req.metadata_mut()
            .insert_bin(NAMESPACE_METADATA_KEY, namespace);
        auth.upgrade_grpc_request(&mut req);
        let response_stream = client.stream_exec(req).await.unwrap().into_inner();

        Ok(Self {
            response_stream,
            request_sender,
            current_request_id: 0,
            builder_config,
        })
    }

    async fn execute<B: QueryResultBuilder>(
        &mut self,
        program: Program,
        mut builder: B,
    ) -> crate::Result<(B, TxnStatus, Option<FrameNo>)> {
        let request_id = self.current_request_id;
        self.current_request_id += 1;

        let req = ExecReq {
            request_id,
            request: Some(rpc::exec_req::Request::Execute(program.into())),
        };

        self.request_sender.send(req).await.unwrap(); // TODO: the stream was close!
        let mut txn_status = TxnStatus::Invalid;
        let mut new_frame_no = None;

        'outer: while let Some(resp) = self.response_stream.next().await {
            match resp {
                Ok(resp) => {
                    if resp.request_id != request_id {
                        todo!("stream misuse: connection should be serialized");
                    }
                    for message in resp.messages {
                        use rpc::message::Payload;

                        match message.payload.unwrap() {
                            Payload::DescribeResult(_) => todo!("invalid response"),

                            Payload::Init(_) => builder.init(&self.builder_config)?,
                            Payload::BeginStep(_) => builder.begin_step()?,
                            Payload::FinishStep(FinishStep {
                                affected_row_count,
                                last_insert_rowid,
                            }) => builder.finish_step(affected_row_count, last_insert_rowid)?,
                            Payload::StepError(StepError { error }) => builder
                                .step_error(crate::error::Error::RpcQueryError(error.unwrap()))?,
                            Payload::ColsDescription(ColsDescription { columns }) => {
                                let cols = columns.iter().map(|c| Column {
                                    name: &c.name,
                                    decl_ty: c.decltype.as_deref(),
                                });
                                builder.cols_description(cols)?
                            }
                            Payload::BeginRows(_) => builder.begin_rows()?,
                            Payload::BeginRow(_) => builder.begin_row()?,
                            Payload::AddRowValue(AddRowValue { val }) => {
                                let value: Value = bincode::deserialize(&val.unwrap().data)
                                    .map_err(QueryResultBuilderError::from_any)?;
                                builder.add_row_value(ValueRef::from(&value))?;
                            }
                            Payload::FinishRow(_) => builder.finish_row()?,
                            Payload::FinishRows(_) => builder.finish_rows()?,
                            Payload::Finish(f @ Finish { last_frame_no, .. }) => {
                                txn_status = TxnStatus::from(f.state());
                                new_frame_no = last_frame_no;
                                builder.finish(last_frame_no, txn_status)?;
                                break 'outer;
                            }
                            Payload::Error(error) => {
                                return Err(crate::error::Error::RpcQueryError(error))
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("received error from connection stream: {e}");
                    return Err(Error::StreamDisconnect)
                },
            }
        }

        Ok((builder, txn_status, new_frame_no))
    }
}

#[async_trait::async_trait]
impl Connection for WriteProxyConnection {
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<(B, TxnStatus)> {
        let mut state = self.state.lock().await;

        // This is a fresh namespace, and it is not replicated yet, proxy the first request.
        if self.applied_frame_no_receiver.borrow().is_none() {
            self.execute_remote(pgm, &mut state, auth, builder).await
        } else if *state == TxnStatus::Init && pgm.is_read_only() {
            self.wait_replication_sync(replication_index).await?;
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let (builder, new_state) = self
                .read_conn
                .execute_program(pgm.clone(), auth.clone(), builder, replication_index)
                .await?;
            if new_state != TxnStatus::Init {
                self.read_conn.rollback(auth.clone()).await?;
                self.execute_remote(pgm, &mut state, auth, builder).await
            } else {
                Ok((builder, new_state))
            }
        } else {
            self.execute_remote(pgm, &mut state, auth, builder).await
        }
    }

    async fn describe(
        &self,
        sql: String,
        auth: Authenticated,
        replication_index: Option<FrameNo>,
    ) -> Result<DescribeResult> {
        self.wait_replication_sync(replication_index).await?;
        self.read_conn.describe(sql, auth, replication_index).await
    }

    async fn is_autocommit(&self) -> Result<bool> {
        let state = self.state.lock().await;
        Ok(match *state {
            TxnStatus::Txn => false,
            TxnStatus::Init | TxnStatus::Invalid => true,
        })
    }

    async fn checkpoint(&self) -> Result<()> {
        self.wait_replication_sync(None).await?;
        self.read_conn.checkpoint().await
    }

    async fn vacuum_if_needed(&self) -> Result<()> {
        tracing::warn!("vacuum is not supported on write proxy");
        Ok(())
    }

    fn diagnostics(&self) -> String {
        format!("{:?}", self.state)
    }
}

impl Drop for WriteProxyConnection {
    fn drop(&mut self) {
        // best effort attempt to disconnect
        let mut remote = self.write_proxy.clone();
        let client_id = self.client_id.to_string();
        tokio::spawn(async move {
            let _ = remote.disconnect(DisconnectMessage { client_id }).await;
        });
    }
}

#[cfg(test)]
pub mod test {
    use arbitrary::{Arbitrary, Unstructured};
    use bytes::Bytes;
    use rand::Fill;

    use super::*;
    use crate::{query_result_builder::test::test_driver, rpc::proxy::rpc::{ExecuteResults, query_result::RowResult}};

    /// generate an arbitraty rpc value. see build.rs for usage.
    pub fn arbitrary_rpc_value(u: &mut Unstructured) -> arbitrary::Result<Vec<u8>> {
        let data = bincode::serialize(&crate::query::Value::arbitrary(u)?).unwrap();

        Ok(data)
    }

    /// generate an arbitraty `Bytes` value. see build.rs for usage.
    pub fn arbitrary_bytes(u: &mut Unstructured) -> arbitrary::Result<Bytes> {
        let v: Vec<u8> = Arbitrary::arbitrary(u)?;

        Ok(v.into())
    }

    fn execute_results_to_builder<B: QueryResultBuilder>(
        execute_result: ExecuteResults,
        mut builder: B,
        config: &QueryBuilderConfig,
    ) -> Result<B> {
        builder.init(config)?;
        for result in execute_result.results {
            match result.row_result {
                Some(RowResult::Row(rows)) => {
                    builder.begin_step()?;
                    builder.cols_description(rows.column_descriptions.iter().map(|c| Column {
                        name: &c.name,
                        decl_ty: c.decltype.as_deref(),
                    }))?;

                    builder.begin_rows()?;
                    for row in rows.rows {
                        builder.begin_row()?;
                        for value in row.values {
                            let value: Value = bincode::deserialize(&value.data)
                                // something is wrong, better stop right here
                                .map_err(QueryResultBuilderError::from_any)?;
                            builder.add_row_value(ValueRef::from(&value))?;
                        }
                        builder.finish_row()?;
                    }

                    builder.finish_rows()?;

                    builder.finish_step(rows.affected_row_count, rows.last_insert_rowid)?;
                }
                Some(RowResult::Error(err)) => {
                    builder.begin_step()?;
                    builder.step_error(Error::RpcQueryError(err))?;
                    builder.finish_step(0, None)?;
                }
                None => (),
            }
        }

        builder.finish(execute_result.current_frame_no, TxnStatus::Init)?;

        Ok(builder)
    }

    /// In this test, we generate random ExecuteResults, and ensures that the `execute_results_to_builder` drives the builder FSM correctly.
    #[test]
    fn test_execute_results_to_builder() {
        test_driver(1000, |b| -> std::result::Result<crate::query_result_builder::test::FsmQueryBuilder, Error> {
            let mut data = [0; 10_000];
            data.try_fill(&mut rand::thread_rng()).unwrap();
            let mut un = Unstructured::new(&data);
            let res = ExecuteResults::arbitrary(&mut un).unwrap();
            execute_results_to_builder(res, b, &QueryBuilderConfig::default())
        });
    }
}
