use std::error::Error as StdError;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use hyper::server::accept::Accept as HyperAccept;
use hyper::Uri;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tower::make::MakeConnection;

pub trait Connector:
    MakeConnection<Uri, Connection = Self::Conn, Future = Self::Fut, Error = Self::Err> + Send + 'static
{
    type Conn: Unpin + Send + 'static;
    type Fut: Send + 'static;
    type Err: StdError + Send + Sync;
}

impl<T> Connector for T
where
    T: MakeConnection<Uri> + Send + 'static,
    T::Connection: Unpin + Send + 'static,
    T::Future: Send + 'static,
    T::Error: StdError + Send + Sync,
{
    type Conn = Self::Connection;
    type Fut = Self::Future;
    type Err = Self::Error;
}

pub trait Conn:
    AsyncRead + AsyncWrite + Unpin + Send + 'static + Connected<ConnectInfo = TcpConnectInfo>
{
}

pub trait Accept: HyperAccept<Conn = Self::Connection, Error = IoError> + Send + 'static {
    type Connection: Conn;
}

pub struct AddrIncoming {
    listener: tokio::net::TcpListener,
}

impl Drop for AddrIncoming {
    fn drop(&mut self) {
        dbg!();
    }
}

impl AddrIncoming {
    pub fn new(listener: tokio::net::TcpListener) -> Self {
        Self { listener }
    }
}

impl HyperAccept for AddrIncoming {
    type Conn = AddrStream;
    type Error = IoError;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        match ready!(self.listener.poll_accept(cx)) {
            Ok((stream, remote_addr)) => {
                // disable naggle algorithm
                stream.set_nodelay(true)?;
                let local_addr = stream.local_addr()?;
                Poll::Ready(Some(Ok(AddrStream {
                    stream,
                    local_addr,
                    remote_addr,
                })))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pin_project! {
    pub struct AddrStream<S = tokio::net::TcpStream> {
        #[pin]
        stream: S,
        remote_addr: SocketAddr,
        local_addr: SocketAddr,
    }
}

impl Accept for AddrIncoming {
    type Connection = AddrStream;
}

impl<S> Conn for AddrStream<S> where S: Unpin + AsyncWrite + AsyncRead + Send + 'static {}

impl<S> AsyncRead for AddrStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for AddrStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().stream.poll_shutdown(cx)
    }
}

impl<S> Connected for AddrStream<S> {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: Some(self.local_addr),
            remote_addr: Some(self.remote_addr),
        }
    }
}

#[cfg(test)]
pub mod test {
    use futures_core::Future;
    use tower::Service;

    use super::*;

    type TurmoilAddrStream = AddrStream<turmoil::net::TcpStream>;

    pub struct TurmoilAcceptor {
        acceptor: Pin<
            Box<dyn HyperAccept<Conn = TurmoilAddrStream, Error = IoError> + Send + Sync + 'static>,
        >,
    }

    impl TurmoilAcceptor {
        pub async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
            let stream = async_stream::stream! {
                let listener = turmoil::net::TcpListener::bind(addr).await?;
                loop {
                    yield listener.accept().await.and_then(|(stream, remote_addr)| Ok(AddrStream {
                        remote_addr,
                        local_addr: stream.local_addr()?,
                        stream,
                    }));
                }
            };
            let acceptor = hyper::server::accept::from_stream(stream);
            Ok(Self {
                acceptor: Box::pin(acceptor),
            })
        }
    }

    impl Accept for TurmoilAcceptor {
        type Connection = TurmoilAddrStream;
    }

    impl HyperAccept for TurmoilAcceptor {
        type Conn = TurmoilAddrStream;
        type Error = IoError;

        fn poll_accept(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
            self.acceptor.as_mut().poll_accept(cx)
        }
    }

    struct TurmoilConnector;

    impl Service<Uri> for TurmoilConnector {
        type Response = turmoil::net::TcpStream;
        type Error = IoError;
        type Future = Pin<Box<dyn Future<Output = std::io::Result<Self::Response>>>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, uri: Uri) -> Self::Future {
            Box::pin(async move {
                let addr = turmoil::lookup(uri.host().unwrap());
                let port = uri.port().unwrap().as_u16();
                turmoil::net::TcpStream::connect((addr, port)).await
            })
        }
    }
}
