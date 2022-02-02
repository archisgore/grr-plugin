
use std::error::Error as StdError;
use std::fmt::Debug;
use std::fmt::{Display, Formatter, Result as FmtResult};

use tokio::sync::mpsc::error::SendError;


use tonic::transport::Error as TonicError;


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
                log::error!("{},({}:{}), {:?}", function!(), file!(), line!(), err);
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
                log::error!("{},({}:{}), {:?}", function!(), file!(), line!(), err);
                return Err(tonic::Status::unknown(format!("{:?}", err)));
            }
            Ok(o) => o,
        }
    };
}

#[derive(Debug)]
pub enum Error {
    NoTCPPortAvailable,
    GRPCHandshakeMagicCookieValueMismatch,
    Io(std::io::Error),
    Generic(String),
    TonicTransport(TonicError),
    AddrParser(std::net::AddrParseError),
    Send(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NoTCPPortAvailable => write!(
                f,
                "No ports were available to bind the plugin's gRPC server to."
            ),
            Self::GRPCHandshakeMagicCookieValueMismatch => write!(f, "This executable is meant to be a go-plugin to other processes. Do not run this directly. The Magic Handshake failed."),
            Self::Generic(s) => write!(f, "{}", s),
            Self::Io(e) => write!(f, "Error with IO: {:?}", e),
            Self::TonicTransport(e) => write!(f, "Error with tonic (gRPC) transport: {:?}", e),
            Self::AddrParser(e) => write!(f, "Error parsing string into a network address: {:?}", e),
            Self::Send(s) => write!(f, "Error sending on a mpsc channel: {}", s),
        }
    }
}

impl StdError for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<TonicError> for Error {
    fn from(err: TonicError) -> Self {
        Self::TonicTransport(err)
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Self::AddrParser(err)
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(_err: SendError<T>) -> Self {
        Self::Send(format!(
            "unable to send {} on a mpsc channel",
            std::any::type_name::<T>()
        ))
    }
}
