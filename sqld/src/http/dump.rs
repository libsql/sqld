use std::future::Future;
use std::io::Write;

use axum::extract::State as AxumState;
use hyper::HeaderMap;
use pin_project_lite::pin_project;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::connection::dump::exporter::export_dump;
use crate::error::Error;
use crate::namespace::MakeNamespace;

use super::db_factory::namespace_from_headers;
use super::AppState;

struct SenderWriter(mpsc::Sender<bytes::Bytes>);

impl Write for SenderWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.0
            .blocking_send(bytes::Bytes::copy_from_slice(buf))
            .map(|_| len)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pin_project! {
    struct DumpStream<S> {
        join_handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
        #[pin]
        stream: S,
    }
}

impl<S> futures::Stream for DumpStream<S>
where
    S: futures::Stream,
{
    type Item = Result<S::Item, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        match futures::ready!(this.stream.poll_next(cx)) {
            Some(item) => std::task::Poll::Ready(Some(Ok(item))),
            None => {
                // The stream was closed but we need to check if the dump task errored, if it did,
                // forward the error.
                if let Some(mut join_handle) = this.join_handle.take() {
                    match std::pin::Pin::new(&mut join_handle).poll(cx) {
                        std::task::Poll::Pending => {
                            *this.join_handle = Some(join_handle);
                            return std::task::Poll::Pending;
                        }
                        std::task::Poll::Ready(Ok(Err(err))) => {
                            return std::task::Poll::Ready(Some(Err(err)));
                        }
                        std::task::Poll::Ready(Err(err)) => {
                            return std::task::Poll::Ready(Some(Err(anyhow::anyhow!(err)
                                .context("Dump task crashed")
                                .into())));
                        }
                        _ => {}
                    }
                }
                std::task::Poll::Ready(None)
            }
        }
    }
}

pub(super) async fn handle_dump<F: MakeNamespace>(
    AxumState(state): AxumState<AppState<F>>,
    headers: HeaderMap,
) -> Result<axum::body::StreamBody<impl futures::Stream<Item = Result<bytes::Bytes, Error>>>, Error>
{
    let namespace = namespace_from_headers(
        &headers,
        state.disable_default_namespace,
        state.disable_namespaces,
    )?;

    let db_path = state
        .db_path
        .join("dbs")
        .join(std::str::from_utf8(namespace.as_ref()).expect("namespace to be a utf-8 string"))
        .join("data");

    let connection = rusqlite::Connection::open(db_path)?;

    let (tx, rx) = mpsc::channel(64);

    let join_handle = tokio::task::spawn_blocking(move || {
        export_dump(connection, SenderWriter(tx)).map_err(|e| e.into())
    });

    let stream = ReceiverStream::new(rx);
    let stream = DumpStream {
        stream,
        join_handle: Some(join_handle),
    };
    let stream = axum::body::StreamBody::new(stream);

    Ok(stream)
}
