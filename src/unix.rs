use anyhow::anyhow;
use async_stream::stream;
use futures::Stream;
use futures::TryFutureExt;
use tempfile::{tempdir, TempDir};
use tokio::net::UnixListener;

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

use super::error::Error;

const SOCKET_FILENAME: &str = "landslide_jsonrpc.sock";

//own this so it doesn't go out of scope and get deleted
pub struct TempSocket(TempDir);
impl TempSocket {
    pub fn new() -> Result<TempSocket, Error> {
        Ok(Self(tempdir()?))
    }

    pub fn socket_filename(&self) -> Result<String, Error> {
        let socket_path_buf = self.0.path().join(SOCKET_FILENAME);
        let socket_path = String::from(socket_path_buf.to_str().ok_or_else(|| {
            anyhow!(
                "Unable to convert PathBuf {:?} to a String.",
                socket_path_buf
            )
        })?);
        Ok(socket_path)
    }
}

pub async fn incoming_from_path(
    path: &str,
) -> Result<impl Stream<Item = Result<UnixStream, std::io::Error>>, Error> {
    let uds = UnixListener::bind(path)?;

    Ok(stream! {
        loop {
            let item = uds.accept().map_ok(|(st, _)| UnixStream(st)).await;

            yield item;
        }
    })
}

#[derive(Debug)]
pub struct UnixStream(pub tokio::net::UnixStream);

impl Connected for UnixStream {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        UdsConnectInfo {
            peer_addr: self.0.peer_addr().ok().map(Arc::new),
            peer_cred: self.0.peer_cred().ok(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct UdsConnectInfo {
    pub peer_addr: Option<Arc<tokio::net::unix::SocketAddr>>,
    pub peer_cred: Option<tokio::net::unix::UCred>,
}

impl AsyncRead for UnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
