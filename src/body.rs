// Tonic requires Body to return a tonic::Status error, but doesn't provide such an implementation. Stupid, I know.
use tonic::transport::Body as TonicTransportBody;
use bytes::Bytes;
use tonic::Status;
use std::task::{Context, Poll};
use std::pin::Pin;
use http_body::Body;
use pin_project::pin_project;
use std::error::Error;
use tonic::body::BoxBody;

#[pin_project(project = PinnedRpcResponse)]
pub enum RpcResponseBody {
    Status(#[pin] StatusResponse),
    Body(#[pin] BodyWrapper)
}

impl RpcResponseBody {
    pub fn from_body(body: TonicTransportBody) -> BoxBody {
        BoxBody::new(Self::Body(BodyWrapper{
            body,
        }))
    }

    pub fn from_string(body: String) -> BoxBody {
        BoxBody::new(Self::from_bytes(body.into_bytes()))
    }

    pub fn from_bytes(body: Vec<u8>) -> BoxBody {
        BoxBody::new(Self::Body(BodyWrapper{
            body: TonicTransportBody::from(body),
        }))
    }

    pub fn from_status(status: Status) -> BoxBody {
        BoxBody::new(Self::Status(StatusResponse{
            status: Some(status),
        }))
    }

    pub fn from_error(err: impl Error) -> BoxBody {
        BoxBody::new(Self::from_status(Status::unknown(format!("{:?}", err))))
    }
}

impl Body for RpcResponseBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_data(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Result<<Self as http_body::Body>::Data, <Self as http_body::Body>::Error>>> {
        match self.project() {
            PinnedRpcResponse::Status(s) => s.poll_data(ctx),
            PinnedRpcResponse::Body(b) => b.poll_data(ctx),
        }
    }

    fn poll_trailers(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<Option<http::HeaderMap>, <Self as http_body::Body>::Error>> {
        match self.project() {
            PinnedRpcResponse::Status(s) => s.poll_trailers(ctx),
            PinnedRpcResponse::Body(b) => b.poll_trailers(ctx),
        }
    }
}



pub struct StatusResponse {
    status: Option<Status>,
}

impl Body for StatusResponse {
    type Data = Bytes;
    type Error = Status;

    fn poll_data(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Result<<Self as http_body::Body>::Data, <Self as http_body::Body>::Error>>> {
        // take status out of self (since it can't be cloned)
        let status = self.status.take();

        match status {
            // we always return an error
            Some(s) => Poll::from(Some(Err(s))),
            None => Poll::from(Some(Err(Status::unknown("poll_data was called multiple times on this future, despite a previous call having return an error with status. That error is now long-forgotten.")))),
        }
    }

    fn poll_trailers(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<Option<http::HeaderMap>, <Self as http_body::Body>::Error>> {
        // take status out of self (since it can't be cloned)
        let status = self.status.take();

        match status {
            // we always return an error
            Some(s) => Poll::from(Err(s)),
            None => Poll::from(Err(Status::unknown("poll_data was called multiple times on this future, despite a previous call having return an error with status. That error is now long-forgotten."))),
        }
    }
}


#[pin_project]
pub struct BodyWrapper {
    #[pin]
    body: TonicTransportBody,
}

impl Body for BodyWrapper {
    type Data = Bytes;
    type Error = Status;

    fn poll_data(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Result<<Self as http_body::Body>::Data, <Self as http_body::Body>::Error>>> {
        // Map Poll<Option<Result<_, Err>>> -> Poll<Option<Result<_, Status>>>
        self.project().body.as_mut().poll_data(ctx)
            .map(|mpr| mpr
                .map(|pr| pr.map_err(|err| Status::unknown(format!("Error when polling data from body: {:?}", err)))))
    }

    fn poll_trailers(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<Option<http::HeaderMap>, <Self as http_body::Body>::Error>> {
        self.project().body.as_mut().poll_trailers(ctx)
            .map(|mpr| mpr
                .map_err(|err| Status::unknown(format!("Error when polling data from body: {:?}", err))))

    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sendable() {
        let sendable: Box<dyn Send> = Box::new(RpcResponseBody::from_status(Status::unknown("foobar")));
        let sendable: Box<dyn Send> = Box::new(RpcResponseBody::from_string("foobar".to_string()));
    }
}
