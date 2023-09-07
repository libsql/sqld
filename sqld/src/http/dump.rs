use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::task;

use axum::extract::State as AxumState;
use futures::stream::Fuse;
use futures::StreamExt;
use hyper::HeaderMap;
use pin_project_lite::pin_project;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::connection::dump::exporter::export_dump;
use crate::error::Error;
use crate::namespace::MakeNamespace;

use super::db_factory::namespace_from_headers;
use super::AppState;

struct SenderWriter(mpsc::Sender<Vec<u8>>);

impl Write for SenderWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.0
            .blocking_send(Vec::from(buf))
            .map(|_| len)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pin_project! {
    /// Return a stream of lines from a stream of bytes keeping the new line
    struct StreamLines<S> {
        #[pin]
        stream: Fuse<S>,
        buffer: Vec<u8>,
    }
}

impl<S> futures::Stream for StreamLines<S>
where
    S: futures::Stream,
    S::Item: IntoIterator<Item = u8>,
{
    type Item = String;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(last_line_index) = this.buffer.iter().position(|val| *val == b'\n') {
                // Keep the new line character
                let remaining = this.buffer.split_off(last_line_index + 1);

                let line = std::mem::replace(this.buffer, remaining);
                let line = String::from_utf8(line).expect("to be a valid utf-8 string");

                return task::Poll::Ready(Some(line));
            }

            match futures::ready!(this.stream.as_mut().poll_next(cx)) {
                Some(buffer) => {
                    this.buffer.extend(buffer);
                }
                None => {
                    if !this.buffer.is_empty() {
                        let line = std::mem::take(this.buffer);
                        let line = String::from_utf8(line).expect("to be a valid utf-8 string");

                        return task::Poll::Ready(Some(line));
                    } else {
                        return task::Poll::Ready(None);
                    }
                }
            };
        }
    }
}

pin_project! {
    struct DumpStream<S> {
        join_handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
        // has to be fused because we might poll the stream again after it returns `None`
        #[pin]
        stream: Fuse<S>,
    }
}

impl<S> futures::Stream for DumpStream<S>
where
    S: futures::Stream,
{
    type Item = Result<S::Item, Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        let this = self.project();

        match futures::ready!(this.stream.poll_next(cx)) {
            Some(item) => task::Poll::Ready(Some(Ok(item))),
            None => {
                // The stream was closed but we need to check if the dump task errored and if it
                // did forward the error.
                if let Some(mut join_handle) = this.join_handle.take() {
                    match Pin::new(&mut join_handle).poll(cx) {
                        task::Poll::Pending => {
                            *this.join_handle = Some(join_handle);
                            return task::Poll::Pending;
                        }
                        task::Poll::Ready(Ok(Err(err))) => {
                            return task::Poll::Ready(Some(Err(err)));
                        }
                        task::Poll::Ready(Err(err)) => {
                            return task::Poll::Ready(Some(Err(anyhow::anyhow!(err)
                                .context("Dump task crashed")
                                .into())));
                        }
                        _ => {}
                    }
                }
                task::Poll::Ready(None)
            }
        }
    }
}

pub(super) async fn handle_dump<F: MakeNamespace>(
    AxumState(state): AxumState<AppState<F>>,
    headers: HeaderMap,
) -> Result<axum::body::StreamBody<impl futures::Stream<Item = Result<String, Error>>>, Error> {
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

    // This is optional but it allows each chunk sent to contain a line
    let stream = StreamLines {
        stream: stream.fuse(),
        buffer: Vec::with_capacity(64),
    };

    let stream = DumpStream {
        stream: stream.fuse(),
        join_handle: Some(join_handle),
    };

    let stream = axum::body::StreamBody::new(stream);

    Ok(stream)
}
