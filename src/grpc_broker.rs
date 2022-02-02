// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

pub use grpc_plugins::ConnInfo;

use super::Error;
use crate::{function, log_and_escalate_status};
use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_broker_server::{GrpcBroker, GrpcBrokerServer};
use std::ops::DerefMut;
use std::pin::Pin;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status, Streaming};

const LOG_PREFIX: &str = "GrrPlugin::GrpcBroker: ";

pub async fn new_server(
    conn_info_receiver: UnboundedReceiver<Result<ConnInfo, Status>>,
    incoming_conninfo_stream_sender: UnboundedSender<Streaming<ConnInfo>>,
) -> Result<GrpcBrokerServer<GrpcBrokerImpl>, Error> {
    log::info!("{} new_server - called.", LOG_PREFIX);

    log::trace!("{} new_server - creating GrpcBrokerImpl.", LOG_PREFIX);
    let broker = GrpcBrokerImpl::new(conn_info_receiver, incoming_conninfo_stream_sender)?;

    log::info!("{} new_server - Returning a new broker as well as a Sender to send ConnInfo to the Plugin Client.", LOG_PREFIX);
    Ok(GrpcBrokerServer::new(broker))
}

struct GrpcBrokerInterior {
    // This is how the outgoing stream will be pulled by consumer
    outgoing_conninfo_receiver_receiver:
        UnboundedReceiver<<GrpcBrokerImpl as GrpcBroker>::StartStreamStream>,
    incoming_conninfo_stream_sender: UnboundedSender<Streaming<ConnInfo>>,
}

pub struct GrpcBrokerImpl {
    interior: RwLock<GrpcBrokerInterior>,
}

impl NamedService for GrpcBrokerImpl {
    const NAME: &'static str = "plugin.GRPCBroker";
}

impl GrpcBrokerImpl {
    pub fn new(
        conn_info_receiver: UnboundedReceiver<Result<ConnInfo, Status>>,
        incoming_conninfo_stream_sender: UnboundedSender<Streaming<ConnInfo>>,
    ) -> Result<GrpcBrokerImpl, Error> {
        log::info!("{} GrpcBrokerImpl::new - called.", LOG_PREFIX);

        log::trace!(
            "{} GrpcBrokerImpl::new - creating outgoing stream.",
            LOG_PREFIX
        );
        let outgoing_stream = Self::new_outgoing_stream(conn_info_receiver);

        // we use a channel to provide one-way send between the constructor where we have this outgoing stream,
        // and a gRPC method stream_start where it will be consumed.
        log::trace!("{} GrpcBrokerImpl::new - sending outgoing stream to an inner stream from which it can be pulled later...", LOG_PREFIX);
        let (outgoing_conninfo_receiver_transmitter, outgoing_conninfo_receiver_receiver) =
            unbounded_channel();
        outgoing_conninfo_receiver_transmitter.send(outgoing_stream)?;

        log::info!(
            "{} GrpcBrokerImpl::new - Creating a new GrpcBrokerImpl with interior mutability.",
            LOG_PREFIX
        );
        Ok(GrpcBrokerImpl {
            interior: RwLock::new(GrpcBrokerInterior {
                outgoing_conninfo_receiver_receiver,
                incoming_conninfo_stream_sender,
            }),
        })
    }

    fn new_outgoing_stream(
        mut conn_info_receiver: UnboundedReceiver<Result<ConnInfo, Status>>,
    ) -> <Self as GrpcBroker>::StartStreamStream {
        log::info!("{} new_outgoing_stream called.", LOG_PREFIX);

        let s = stream! {
            log::info!("{} outgoing_stream repeater initialized.", LOG_PREFIX);
            loop {
                log::info!("{} outgoing_stream loop iteration", LOG_PREFIX);
                match conn_info_receiver.recv().await {
                    Some(result) => {
                        log::info!("{} Sending Result<ConnInfo> to outgoing_stream: {:?}.", LOG_PREFIX, result);
                        yield result
                    },
                    None => {
                        log::info!("{} incoming receiver for outgoing_stream received an empty item. Unexpected.", LOG_PREFIX);
                        yield Err(Status::unknown("received an empty message from plugin side to GrpcBroker"))
                    },
                }
            }
        };

        let dyn_stream: Pin<
            Box<dyn Stream<Item = Result<ConnInfo, Status>> + Sync + Send + 'static>,
        > = Box::pin(s);

        log::info!("{} outgoing stream created and returning...", LOG_PREFIX);
        dyn_stream
    }
}

#[async_trait]
impl GrpcBroker for GrpcBrokerImpl {
    type StartStreamStream =
        Pin<Box<dyn Stream<Item = Result<ConnInfo, Status>> + Sync + Send + 'static>>;

    async fn start_stream(
        &self,
        req: Request<Streaming<ConnInfo>>,
    ) -> Result<Response<Self::StartStreamStream>, Status> {
        log::info!("{} start_stream called.", LOG_PREFIX);

        let mut interior_write_guard = self.interior.write().await;
        let interior = interior_write_guard.deref_mut();

        match interior.outgoing_conninfo_receiver_receiver.recv().await {
            None => {
                let errmsg = format!("{} start_stream - outgoing_stream was None, which, being initalized in the constructor, was vended off already. Was this method called twice? Did someone else .take() it?", LOG_PREFIX);
                log::error!("{}", errmsg);
                Err(Status::unknown(errmsg))
            }
            Some(os) => {
                log::info!(
                    "{} start_stream - sending the Stream of incoming ConnInfo to someone else to broker...",
                    LOG_PREFIX
                );
                log_and_escalate_status!(interior
                    .incoming_conninfo_stream_sender
                    .send(req.into_inner()));

                Ok(Response::new(os))
            }
        }
    }
}
