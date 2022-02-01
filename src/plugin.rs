use http::{Request, Response};
use hyper::Body;
use std::clone::Clone;
use std::marker::Send;
use tonic::body::BoxBody;
use tonic::transport::NamedService;
use tower::Service;

pub trait Plugin {}

pub trait PluginServer:
    Plugin
    + Service<Request<Body>, Response = Response<BoxBody>>
    + NamedService
    + Clone
    + Send
    + 'static
where
    <Self as Service<http::Request<hyper::Body>>>::Future: Send + 'static,
    <Self as Service<http::Request<hyper::Body>>>::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
}
