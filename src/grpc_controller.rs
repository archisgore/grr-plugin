// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}



use grpc_plugins::grpc_controller_server::{GrpcController, GrpcControllerServer};
use grpc_plugins::{Empty};
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status};

const LOG_PREFIX: &str = "GrrPlugin::GrpcController: ";

pub fn new(trigger: triggered::Trigger) -> GrpcControllerServer<GrpcControllerImpl> {
    GrpcControllerServer::new(GrpcControllerImpl { trigger })
}

#[derive(Clone)]
pub struct GrpcControllerImpl {
    trigger: triggered::Trigger,
}

impl NamedService for GrpcControllerImpl {
    const NAME: &'static str = "plugin.GRPCController";
}

#[async_trait]
impl GrpcController for GrpcControllerImpl {
    async fn shutdown(&self, _req: Request<Empty>) -> Result<Response<Empty>, Status> {
        log::info!("{} shutdown called. Stopping grpc server...", LOG_PREFIX);

        self.trigger.trigger();
        Ok(Response::new(Empty {}))
    }
}
