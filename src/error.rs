use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use tonic::transport::Error as TonicError;

#[macro_export]
macro_rules! log_and_escalate {
    ($e:expr) => {
        match $e {
            Err(err) => {
                log::error!("{},({}:{}), {:?}", function!(), file!(), line!(), err);
                return Err(err.into());
            }
            Ok(o) => o,
        }
    };
}

#[derive(Debug)]
pub enum Error {
    NoTCPPortAvailable,
    GRPCHandshakeMagicCookieValueMismatch,
    StdIoError(std::io::Error),
    Generic(String),
    TonicTransportError(TonicError),
    AddrParseError(std::net::AddrParseError),
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
            Self::StdIoError(e) => write!(f, "Error with IO: {:?}", e),
            Self::TonicTransportError(e) => write!(f, "Error with tonic (gRPC) transport: {:?}", e),
            Self::AddrParseError(e) => write!(f, "Error parsing string into a network address: {:?}", e),
        }
    }
}

impl StdError for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::StdIoError(err)
    }
}

impl From<TonicError> for Error {
    fn from(err: TonicError) -> Self {
        Self::TonicTransportError(err)
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Self::AddrParseError(err)
    }
}
