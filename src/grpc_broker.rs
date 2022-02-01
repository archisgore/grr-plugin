// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_broker_server::{GrpcBroker, GrpcBrokerServer};
use grpc_plugins::ConnInfo;
use std::ops::DerefMut;
use std::pin::Pin;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status, Streaming};

const LOG_PREFIX: &str = "GrrPlugin::GrpcBroker: ";

pub fn new() -> GrpcBrokerServer<GrpcBrokerImpl> {
    GrpcBrokerServer::new(GrpcBrokerImpl::new())
}

struct GrpcBrokerInterior {
    tx: Option<UnboundedSender<Result<ConnInfo, Status>>>,
}

pub struct GrpcBrokerImpl {
    interior: RwLock<GrpcBrokerInterior>,
}

impl NamedService for GrpcBrokerImpl {
    const NAME: &'static str = "plugin.GRPCBroker";
}

impl GrpcBrokerImpl {
    pub fn new() -> GrpcBrokerImpl {
        GrpcBrokerImpl {
            interior: RwLock::new(GrpcBrokerInterior {
                tx: Option::<UnboundedSender<Result<ConnInfo, Status>>>::None,
            }),
        }
    }

    fn new_outgoing_stream() -> (
        <Self as GrpcBroker>::StartStreamStream,
        UnboundedSender<Result<ConnInfo, Status>>,
    ) {
        log::info!("{} new_outgoing_stream called.", LOG_PREFIX);
        let (tx, mut rx) = unbounded_channel::<Result<ConnInfo, Status>>();

        let s = Box::pin(stream! {
            loop {
                match rx.recv().await {
                    Some(result) => yield result,
                    None => yield Err(Status::unknown("received an empty message from plugin side to GrpcBroker")),
                }
            }
        });

        log::info!("{} outgoing stream created and returning...", LOG_PREFIX);
        (Box::pin(s), tx)
    }

    async fn blocking_incoming_conn(mut stream: Streaming<ConnInfo>) {
        log::info!(
            "{}blocking_incoming_conn - perpetually listening for incoming ConnInfo's",
            LOG_PREFIX
        );
        while let Some(conn_info_result) = stream.next().await {
            if let Err(e) = conn_info_result {
                log::error!(
                    "{}blocking_incoming_conn - an error occurred reading from the stream: {:?}",
                    LOG_PREFIX,
                    e
                );
                break;
            }

            //log::info!("{}Received conn_info: {:?}", LOG_PREFIX, conn_info);
        }

        log::info!(
            "{}blocking_incoming_conn - exiting due to stream returning None or an error",
            LOG_PREFIX
        );
    }
}

#[async_trait]
impl GrpcBroker for GrpcBrokerImpl {
    type StartStreamStream =
        Pin<Box<dyn Stream<Item = Result<ConnInfo, Status>> + Sync + Send + Unpin + 'static>>;

    async fn start_stream(
        &self,
        req: Request<Streaming<ConnInfo>>,
    ) -> Result<Response<Self::StartStreamStream>, Status> {
        log::info!("{} start_stream called.", LOG_PREFIX);

        let mut interior_write_guard = self.interior.write().await;
        let interior = interior_write_guard.deref_mut();

        let (outgoing_stream, tx) = GrpcBrokerImpl::new_outgoing_stream();
        interior.tx = Some(tx);

        log::info!(
            "{} spawning perpetual process to process incoming ConnInfo's...",
            LOG_PREFIX
        );
        tokio::spawn(async move { GrpcBrokerImpl::blocking_incoming_conn(req.into_inner()).await });

        Ok(Response::new(outgoing_stream))
    }
}
