// Copied from: https://github.com/hashicorp/go-plugin/blob/master/grpc_controller.go
pub mod grpc_plugins {
    tonic::include_proto!("plugin");
}

pub use grpc_plugins::ConnInfo;

use super::error::Error;
use crate::{function, log_and_escalate};
use async_stream::stream;
use futures::stream::Stream;
use grpc_plugins::grpc_broker_server::{GrpcBroker, GrpcBrokerServer};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::transport::NamedService;
use tonic::{async_trait, Request, Response, Status, Streaming};

const LOG_PREFIX: &str = "GrrPlugin::GrpcBroker: ";

pub async fn new_server() -> (GrpcBrokerServer<GrpcBrokerImpl>, ConnInfoSender) {
    log::info!("{} new_server - called.", LOG_PREFIX);
    let broker = GrpcBrokerImpl::new();

    log::trace!("{} new_server - created inner broker impl.", LOG_PREFIX);
    let conn_info_sender = async {
        log::trace!(
            "{} new_server - acquiring a read lock on the broker's interior mutable state.",
            LOG_PREFIX
        );
        let interior_read_guard = broker.interior.read().await;
        let interior = interior_read_guard.deref();

        log::trace!(
            "{} new_server - cloning the broker's transmission channel so it can be sent to.",
            LOG_PREFIX
        );
        ConnInfoSender {
            tx: interior.tx.clone(),
        }
    }
    .await;

    log::info!("{} new_server - Returning a new broker as well as a Sender to send ConnInfo to the Plugin Client.", LOG_PREFIX);
    (GrpcBrokerServer::new(broker), conn_info_sender)
}

#[derive(Clone)]
pub struct ConnInfoSender {
    tx: UnboundedSender<Result<ConnInfo, Status>>,
}

impl ConnInfoSender {
    pub async fn send(&mut self, ci: ConnInfo) -> Result<(), Error> {
        log::info!("{} ConnInfoSender::send - called.", LOG_PREFIX);
        log::trace!(
            "{} ConnInfoSender::send - called for ConnInfo: {:?}.",
            LOG_PREFIX,
            ci
        );
        Ok(log_and_escalate!(self.tx.send(Ok(ci))))
    }
}

struct GrpcBrokerInterior {
    tx: UnboundedSender<Result<ConnInfo, Status>>,
    outgoing_stream: Option<<GrpcBrokerImpl as GrpcBroker>::StartStreamStream>,
}

pub struct GrpcBrokerImpl {
    interior: RwLock<GrpcBrokerInterior>,
}

impl NamedService for GrpcBrokerImpl {
    const NAME: &'static str = "plugin.GRPCBroker";
}

impl GrpcBrokerImpl {
    pub fn new() -> GrpcBrokerImpl {
        let (outgoing_stream, tx) = GrpcBrokerImpl::new_outgoing_stream();

        log::info!(
            "{} new - Creating a new GrpcBrokerImpl with interior mutability.",
            LOG_PREFIX
        );
        GrpcBrokerImpl {
            interior: RwLock::new(GrpcBrokerInterior {
                tx,
                outgoing_stream: Some(outgoing_stream),
            }),
        }
    }

    fn new_outgoing_stream() -> (
        <Self as GrpcBroker>::StartStreamStream,
        UnboundedSender<Result<ConnInfo, Status>>,
    ) {
        log::info!("{} new_outgoing_stream called.", LOG_PREFIX);
        let (tx, mut rx) = unbounded_channel::<Result<ConnInfo, Status>>();

        let s = stream! {
            log::info!("{} outgoing_stream repeater initialized.", LOG_PREFIX);
            loop {
                log::info!("{} outgoing_stream loop iteration", LOG_PREFIX);
                match rx.recv().await {
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
        (dyn_stream, tx)
    }

    // This function will run forever. tokio::spawn this!
    async fn blocking_incoming_conn(mut stream: Streaming<ConnInfo>) {
        log::info!(
            "{}blocking_incoming_conn - perpetually listening for incoming ConnInfo's",
            LOG_PREFIX
        );
        while let Some(conn_info_result) = stream.next().await {
            match conn_info_result {
                Err(e) => {
                    log::error!(
                        "{}blocking_incoming_conn - an error occurred reading from the stream: {:?}",
                        LOG_PREFIX,
                        e
                    );
                    break; //out of the while loop
                }
                Ok(conn_info) => log::info!("{}Received conn_info: {:?}", LOG_PREFIX, conn_info),
            }
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
        Pin<Box<dyn Stream<Item = Result<ConnInfo, Status>> + Sync + Send + 'static>>;

    async fn start_stream(
        &self,
        req: Request<Streaming<ConnInfo>>,
    ) -> Result<Response<Self::StartStreamStream>, Status> {
        log::info!("{} start_stream called.", LOG_PREFIX);

        let mut interior_write_guard = self.interior.write().await;
        let interior = interior_write_guard.deref_mut();

        let mos = interior.outgoing_stream.take();
        match mos {
            None => {
                log::error!("{} start_stream - outgoing_stream was None, which, being initalized in the constructor, was vended off already. Was this method called twice? Did someone else .take() it?", LOG_PREFIX);
                Err(Status::unknown("outgoing_stream was None, which, being initalized in the constructor, was vended off already. Was this method called twice? Did someone else .take() it?"))
            }
            Some(os) => {
                log::info!(
                    "{} spawning perpetual process to process incoming ConnInfo's...",
                    LOG_PREFIX
                );
                tokio::spawn(async move {
                    GrpcBrokerImpl::blocking_incoming_conn(req.into_inner()).await
                });

                Ok(Response::new(os))
            }
        }
    }
}
