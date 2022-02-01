// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_stdio_server::{GrpcStdio, GrpcStdioServer};
use grpc_plugins::StdioData;
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status};

const LOG_PREFIX: &str = "GrrPlugin::GrpcStdio: ";

pub fn new() -> GrpcStdioServer<GrpcStdioImpl> {
    GrpcStdioServer::new(GrpcStdioImpl {})
}

#[derive(Clone)]
pub struct GrpcStdioImpl {}

impl GrpcStdioImpl {
    fn new_inner_stream() -> impl Stream<Item = Result<StdioData, Status>> + Send + Unpin {
        let s = stream! {
            yield Err(Status::unknown(""))
        };

        Box::pin(s)
    }
}

impl NamedService for GrpcStdioImpl {
    const NAME: &'static str = "plugin.GRPCStdio";
}

#[async_trait]
impl GrpcStdio for GrpcStdioImpl {
    type StreamStdioStream =
        Box<dyn Stream<Item = Result<StdioData, Status>> + Send + Unpin + 'static>;

    async fn stream_stdio(
        &self,
        req: Request<()>,
    ) -> Result<Response<Self::StreamStdioStream>, Status> {
        log::info!("{} stream_stdio called.", LOG_PREFIX);

        let s: Box<dyn Stream<Item = Result<StdioData, Status>> + Send + Unpin> =
            Box::new(GrpcStdioImpl::new_inner_stream());

        Ok(Response::new(s))
    }
}
