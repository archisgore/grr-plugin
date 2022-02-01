// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
use http::{Request, Response, StatusCode};
use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};
use tonic::transport::Body;
use tonic::body::BoxBody;
use super::body::RpcResponseBody;
use tower::Service;

const LOG_PREFIX: &str = "GrrPlugin::Controller: ";

#[derive(Clone)]
pub struct Controller {

}

impl tonic::transport::NamedService for Controller {
    const NAME: &'static str = "grr_plugin_gRPC_Controller";
}

impl Service<Request<Body>> for Controller {    
    type Response = http::Response<BoxBody>;
    type Error = http::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;


    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), <Self as tower::Service<Request<Body>>>::Error>> {
        log::debug!("{} - 'poll_ready' called", LOG_PREFIX);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Request<Body>) -> <Self as tower::Service<Request<Body>>>::Future {
        log::debug!("{} - 'call' called", LOG_PREFIX);
        // create a response in a future.
        let fut = async {
            // Create the HTTP response
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(RpcResponseBody::from_string("hello, world!\n".to_string()))
                .expect("Error creating response body.");

            Ok(resp)
        };

        // Return the response as an immediate future
        Box::pin(fut)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tonic::transport::Body as TonicTransportBody;
    use assert_matches::assert_matches;

    #[test]
    fn test_sendable() {
        let _sendable: Box<dyn Send> = Box::new(Controller{});

        let waker = futures::task::noop_waker_ref();
        let mut ctx = std::task::Context::from_waker(waker);        

        let mut c = Controller{};
        assert_matches!(c.poll_ready(&mut ctx), Poll::Ready(Ok(())));

        let req = Request::new(TonicTransportBody::from("foobar"));
        let _sendable: Box<dyn Send> = Box::new(c.call(req));
    }
}
