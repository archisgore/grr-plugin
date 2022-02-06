// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

use grpc_plugins::grpc_controller_server::{GrpcController, GrpcControllerServer};
use grpc_plugins::Empty;
use tonic::{async_trait, Request, Response, Status};

pub fn new_server(trigger: triggered::Trigger) -> GrpcControllerServer<GrpcControllerImpl> {
    GrpcControllerServer::new(GrpcControllerImpl { trigger })
}

#[derive(Clone)]
pub struct GrpcControllerImpl {
    trigger: triggered::Trigger,
}

#[async_trait]
impl GrpcController for GrpcControllerImpl {
    async fn shutdown(&self, _req: Request<Empty>) -> Result<Response<Empty>, Status> {
        log::info!("shutdown called. Stopping grpc server...");

        self.trigger.trigger();
        Ok(Response::new(Empty {}))
    }
}
