use std::fmt::Debug;
use thiserror::Error as ThisError;

use tokio::sync::mpsc::error::SendError;

use tonic::transport::Error as TonicError;

use http::uri::InvalidUri;

#[macro_export]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

#[macro_export]
macro_rules! log_and_escalate {
    ($e:expr) => {
        match $e {
            Err(err) => {
                log::error!("{:?}", err);
                return Err(Error::from(err));
            }
            Ok(o) => o,
        }
    };
}

#[macro_export]
macro_rules! log_and_escalate_status {
    ($e:expr) => {
        match $e {
            Err(err) => {
                log::error!("{:?}", err);
                return Err(tonic::Status::unknown(format!("{:?}", err)));
            }
            Ok(o) => o,
        }
    };
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("No ports were available to bind the plugin's gRPC server to.")]
    NoTCPPortAvailable,
    #[error("This executable is meant to be a go-plugin to other processes. Do not run this directly. The Magic Handshake failed.")]
    GRPCHandshakeMagicCookieValueMismatch,
    #[error("The requested ServiceId {0} does not exist and timed out waiting for it.")]
    ServiceIdDoesNotExist(u32),
    #[error("Error with IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error with tonic (gRPC) transport: {0}")]
    TonicTransport(#[from] TonicError),
    #[error("Error parsing string into a network address: {0}")]
    AddrParser(#[from] std::net::AddrParseError),
    #[error("Error sending on a mpsc channel: {0}")]
    Send(String),
    #[error("Invalid Uri: {0}")]
    InvalidUri(#[from] InvalidUri),
    #[error("Service endpoint type unknown: {0}")]
    NetworkTypeUnknown(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl<T> From<SendError<T>> for Error {
    fn from(_err: SendError<T>) -> Self {
        Self::Send(format!(
            "unable to send {} on a mpsc channel",
            std::any::type_name::<T>()
        ))
    }
}
