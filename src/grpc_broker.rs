// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

pub use grpc_plugins::ConnInfo;

use super::error::{into_status, Error};
use anyhow::Result;
use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_broker_server::{GrpcBroker, GrpcBrokerServer};
use std::ops::DerefMut;
use std::pin::Pin;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status, Streaming};

pub async fn new_server(
    conn_info_receiver: UnboundedReceiver<Result<ConnInfo, Status>>,
    incoming_conninfo_stream_sender: UnboundedSender<Streaming<ConnInfo>>,
) -> Result<GrpcBrokerServer<GrpcBrokerImpl>, Error> {
    log::trace!("new_server - called.");

    log::trace!("new_server - creating GrpcBrokerImpl.");
    let broker = GrpcBrokerImpl::new(conn_info_receiver, incoming_conninfo_stream_sender)?;

    log::trace!("new_server - Returning a new broker as well as a Sender to send ConnInfo to the Plugin Client.");
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
        log::trace!("GrpcBrokerImpl::new - called.");

        log::trace!("GrpcBrokerImpl::new - creating outgoing stream.");
        let outgoing_stream = Self::new_outgoing_stream(conn_info_receiver);

        // we use a channel to provide one-way send between the constructor where we have this outgoing stream,
        // and a gRPC method stream_start where it will be consumed.
        log::trace!("GrpcBrokerImpl::new - sending outgoing stream to an inner stream from which it can be pulled later...");
        let (outgoing_conninfo_receiver_transmitter, outgoing_conninfo_receiver_receiver) =
            unbounded_channel();
        outgoing_conninfo_receiver_transmitter.send(outgoing_stream)?;

        log::trace!(
            "GrpcBrokerImpl::new - Creating a new GrpcBrokerImpl with interior mutability."
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
        log::trace!("new_outgoing_stream called.");

        let s = stream! {
            log::trace!("outgoing_stream repeater initialized.");
            loop {
                log::trace!("outgoing_stream loop iteration");
                match conn_info_receiver.recv().await {
                    Some(result) => {
                        log::trace!("Sending Result<ConnInfo> to outgoing_stream: {:?}.", result);
                        yield result
                    },
                    None => {
                        let errmsg = "incoming receiver for outgoing_stream received an empty item. Unexpected.";
                        log::error!("{errmsg}");
                        yield Err(Status::unknown(errmsg))
                    },
                }
            }
        };

        let dyn_stream: Pin<
            Box<dyn Stream<Item = Result<ConnInfo, Status>> + Sync + Send + 'static>,
        > = Box::pin(s);

        log::trace!("outgoing stream created and returning...");
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
        log::trace!("called");

        let mut interior_write_guard = self.interior.write().await;
        let interior = interior_write_guard.deref_mut();

        match interior.outgoing_conninfo_receiver_receiver.recv().await {
            None => {
                let errmsg = "outgoing_stream was None, which, being initalized in the constructor, was vended off already. Was this method called twice? Did someone else .take() it?";
                log::error!("{}", errmsg);
                Err(Status::unknown(errmsg))
            }
            Some(os) => {
                log::trace!("sending the Stream of incoming ConnInfo to someone else to broker...");
                interior
                    .incoming_conninfo_stream_sender
                    .send(req.into_inner())
                    .map_err(|e| e.into())
                    .map_err(into_status)?;

                Ok(Response::new(os))
            }
        }
    }
}
