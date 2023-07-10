use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use futures::Future;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::net::TcpStream;

pub trait Connector
where
    Self: Sized + AsyncRead + AsyncWrite + Unpin + 'static + Send,
{
    type Future: Future<Output = io::Result<Self>> + Send;

    fn connect(addr: String) -> Self::Future;
}

impl Connector for TcpStream {
    type Future = Pin<Box<dyn Future<Output = io::Result<Self>> + Send>>;

    fn connect(addr: String) -> Self::Future {
        Box::pin(TcpStream::connect(addr))
    }
}

pub trait Listener {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Future<'a>: Future<Output = io::Result<(Self::Stream, SocketAddr)>> + 'a
    where
        Self: 'a;

    fn accept(&self) -> Self::Future<'_>;
}

pub struct AcceptFut<'a>(&'a TcpListener);

impl<'a> Future for AcceptFut<'a> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.poll_accept(cx)
    }
}

impl Listener for TcpListener {
    type Stream = TcpStream;
    type Future<'a> = AcceptFut<'a>;

    fn accept(&self) -> Self::Future<'_> {
        AcceptFut(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use turmoil::net::{TcpListener, TcpStream};

    impl Listener for TcpListener {
        type Stream = TcpStream;
        type Future<'a> =
            Pin<Box<dyn Future<Output = io::Result<(Self::Stream, SocketAddr)>> + 'a>>;

        fn accept(&self) -> Self::Future<'_> {
            Box::pin(self.accept())
        }
    }

    impl Connector for TcpStream {
        type Future = Pin<Box<dyn Future<Output = io::Result<Self>> + Send + 'static>>;

        fn connect(addr: String) -> Self::Future {
            Box::pin(Self::connect(addr))
        }
    }
}
