// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_broker_server::{GrpcBroker, GrpcBrokerServer};
use grpc_plugins::{ConnInfo, Empty};
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status, Streaming};

const LOG_PREFIX: &str = "GrrPlugin::GrpcBroker: ";

pub fn new() -> GrpcBrokerServer<GrpcBrokerImpl> {
    GrpcBrokerServer::new(GrpcBrokerImpl {})
}

#[derive(Clone)]
pub struct GrpcBrokerImpl {}

impl NamedService for GrpcBrokerImpl {
    const NAME: &'static str = "plugin.GRPCBroker";
}

impl GrpcBrokerImpl {
    fn new_inner_stream() -> impl Stream<Item = Result<ConnInfo, Status>> + Send + Unpin {
        let s = stream! {
            yield Err(Status::unknown(""))
        };

        Box::pin(s)
    }
}

#[async_trait]
impl GrpcBroker for GrpcBrokerImpl {
    type StartStreamStream =
        Box<dyn Stream<Item = Result<ConnInfo, Status>> + Send + Unpin + 'static>;

    async fn start_stream(
        &self,
        req: Request<Streaming<ConnInfo>>,
    ) -> Result<Response<Self::StartStreamStream>, Status> {
        log::info!("{} start_stream called.", LOG_PREFIX);

        let boxed_stream: Box<dyn Stream<Item = Result<ConnInfo, Status>> + Send + Unpin> =
            Box::new(GrpcBrokerImpl::new_inner_stream());

        Ok(Response::new(boxed_stream))
    }
}
