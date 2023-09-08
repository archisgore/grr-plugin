// A go-plugin Server to write Rust-based plugins to Golang.

pub mod error;
mod grpc_broker;
mod grpc_broker_service;
mod grpc_controller;
mod grpc_stdio;
mod unique_port;
pub mod unix;

use error::Error;

use anyhow::{anyhow, Context, Result};
use http::{Request, Response};
use hyper::Body;
use std::clone::Clone;
use std::convert::Infallible;
use std::env;
use std::marker::Send;
use tonic::body::BoxBody;
use tonic::transport::NamedService;
use tower::Service;
use unix::TempSocket;

pub use grpc_broker::GRpcBroker;
pub use grpc_broker_service::grpc_plugins::ConnInfo;
pub use tonic::{Status, Streaming};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub type ServiceId = u32;

// The constants are for generating the go-plugin string
// https://github.com/hashicorp/go-plugin/blob/master/docs/guide-plugin-write-non-go.md
const GRPC_CORE_PROTOCOL_VERSION: usize = 1;

pub struct HandshakeConfig {
    pub magic_cookie_key: String,
    pub magic_cookie_value: String,
}

pub struct Server {
    handshake_config: HandshakeConfig,
    protocol_version: u32,
    outgoing_conninfo_sender_receiver: UnboundedReceiver<UnboundedSender<Result<ConnInfo, Status>>>,
    outgoing_conninfo_receiver_receiver:
        UnboundedReceiver<UnboundedReceiver<Result<ConnInfo, Status>>>,
    incoming_conninfo_stream_sender: UnboundedSender<Streaming<ConnInfo>>,
    incoming_conninfo_stream_receiver_receiver:
        UnboundedReceiver<UnboundedReceiver<Streaming<ConnInfo>>>,
    trigger: triggered::Trigger,
    listener: triggered::Listener,
}

impl Server {
    pub fn new(protocol_version: u32, handshake_config: HandshakeConfig) -> Result<Server, Error> {
        // This channel sends conninfo from the plugin/server side (the sender will be vended to the JsonRPCBroker who will send new
        // ConnInfo's as new services/handlers are launched) to the host/client side (through the gRPCBroker's start_stream call)
        // where the host/client will process them
        let (outgoing_conninfo_sender, outgoing_conninfo_receiver) =
            unbounded_channel::<Result<ConnInfo, Status>>();

        // Use this channel to send the channel transmitter above from this constructor
        // to where it will be consumed in the "jsonrpc_broker" method later...
        // using channels avoids having to do a complex sync dance using mutable globals.
        let (outgoing_conninfo_sender_transmitter, outgoing_conninfo_sender_receiver) =
            unbounded_channel();
        outgoing_conninfo_sender_transmitter.send(outgoing_conninfo_sender).context("Unable to send the outgoing_conninfo_sender to the transmitter. This is a tokio mpsc channel's transmitter being transmitted over another channel so it can be consumed exactly-one by someone later.")?;

        // Use this channel to send the channel receiver above from this constructor
        // to where it will be consumed in the "serve" method later...
        // using channels avoids having to do a complex sync dance using mutable globals.
        let (outgoing_conninfo_receiver_transmitter, outgoing_conninfo_receiver_receiver) =
            unbounded_channel();
        outgoing_conninfo_receiver_transmitter.send(outgoing_conninfo_receiver).context("Unable to send the outgoing_conninfo_receiver to the transmitter. This is a tokio mpsc channel's receiver being transmitted over another channel so it can be consumed exactly-one by someone later.")?;

        // Use this channel to send the channel from where we will receive ConnInfo's coming inbound
        // from the host, to the broker which will know what to do with them
        // This channel/stream of ConnInfo's will be received from the GRPCBroker in the start_stream call
        // and will be sent from there to the JsonRPCBroker who will broker the ConnInfo's towards the host.
        let (incoming_conninfo_stream_sender, incoming_conninfo_stream_receiver) =
            unbounded_channel();

        // Do the same dance of channel-of-channels to send the receiver since the underlying stream won't be available
        // for quite some time.
        let (
            incoming_conninfo_stream_receiver_transmitter,
            incoming_conninfo_stream_receiver_receiver,
        ) = unbounded_channel();

        incoming_conninfo_stream_receiver_transmitter.send(incoming_conninfo_stream_receiver)
            .context("Unable to send the incoming_conninfo_stream_receiver to the transmitter. This is a tokio mpsc channel's receiver's receiver being transmitted over another channel so it can be consumed exactly-one by someone later. They will eventually listen to this channel to then get the actual stream over which they'll receive incoming ConnInfo's.")?;

        let (trigger, listener) = triggered::trigger();

        Ok(Server {
            handshake_config,
            protocol_version,
            outgoing_conninfo_sender_receiver,
            outgoing_conninfo_receiver_receiver,
            incoming_conninfo_stream_sender,
            incoming_conninfo_stream_receiver_receiver,
            trigger,
            listener,
        })
    }

    pub async fn grpc_broker(&mut self) -> Result<GRpcBroker, Error> {
        let outgoing_conninfo_sender = match self.outgoing_conninfo_sender_receiver.recv().await {
            None => {
                let err = anyhow!("jsonrpc_server_broker's transmission channel was None, which, being initalized in the constructor, was vended off already. Was this method called twice? Did someone else .recv() it?");
                log::error!("{}", err);
                return Err(Error::Other(err));
            }
            Some(outgoing_conninfo_sender) => outgoing_conninfo_sender,
        };

        let incoming_conninfo_stream_receiver = match self
            .incoming_conninfo_stream_receiver_receiver
            .recv()
            .await
        {
            None => {
                let err = anyhow!("jsonrpc_server_broker's receiver for a future incoming stream of ConnInfo was None, which, being initalized in the constructor, was vended off already.");
                log::error!("{}", err);
                return Err(Error::Other(err));
            }
            Some(outgoing_conninfo_sender) => outgoing_conninfo_sender,
        };

        // create the JSON-RPC 2.0 server broker
        log::trace!("Creating the JSON RPC 2.0 Server Broker.",);
        let jsonrpc_broker = GRpcBroker::new(
            unique_port::UniquePort::new(),
            outgoing_conninfo_sender,
            incoming_conninfo_stream_receiver,
            self.listener.clone(),
        );

        log::info!("Created JSON RPC 2.0 Server Broker.");

        Ok(jsonrpc_broker)
    }

    // Copied from: https://github.com/hashicorp/go-plugin/blob/master/server.go#L247
    fn validate_magic_cookie(&self) -> Result<(), Error> {
        log::info!("Validating the magic environment cookies to conduct the handshake. Expecting environment variable {}={}.", self.handshake_config.magic_cookie_key, self.handshake_config.magic_cookie_value);
        match env::var(&self.handshake_config.magic_cookie_key) {
            Ok(value) => {
                if value == self.handshake_config.magic_cookie_value {
                    log::info!("Handshake succeeded!");
                    return Ok(());
                } else {
                    log::error!("Handshake failed due to environment variable {}'s value being {}, but expected to be {}.",self.handshake_config.magic_cookie_key, value, self.handshake_config.magic_cookie_value);
                }
            }
            Err(e) => log::error!(
                "Handshake failed due to error reading environment variable {}: {:?}",
                self.handshake_config.magic_cookie_key,
                e
            ),
        }

        Err(Error::GRPCHandshakeMagicCookieValueMismatch)
    }

    pub async fn serve<S>(&mut self, plugin: S) -> Result<(), Error>
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error=Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        <S as Service<http::Request<hyper::Body>>>::Future: Send + 'static,
        <S as Service<http::Request<hyper::Body>>>::Error:
            Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        log::trace!("serving over a Tcp Socket...");

        self.validate_magic_cookie().context("Failed to validate magic cookie handshake from plugin client (i.e. host, i.e. consumer) to this Plugin.")?;

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<S>().await;
        log::info!("gRPC Health Service created.");

        let temp_socket = TempSocket::new()
            .context("Failed to create a new TempSocket for opening the main gRPC listener to")?;
        let socket_path = temp_socket.socket_filename()
            .context("Failed to get a temporary socket filename from the temp socket for opening the main gRPC listener to")?;
        log::trace!("Created new temp socket: {}", socket_path);

        let handshakestr = format!(
            "{}|{}|unix|{}|grpc|",
            GRPC_CORE_PROTOCOL_VERSION, self.protocol_version, socket_path,
        );
        log::trace!("Created Handshake string: {}", handshakestr);

        // create incoming stream from unix socket above...
        let incoming_stream_from_socket = unix::incoming_from_path(socket_path.as_str()).await?;
        log::trace!("Created Incoming unix stream from the socket");

        let outgoing_conninfo_receiver = match self.outgoing_conninfo_receiver_receiver.recv().await {
            Some(outgoing_conninfo_receiver) => outgoing_conninfo_receiver,
            None => return Err(Error::Other(anyhow!("Outgoing ConnInfo receiver does not exist. Did someone else .recv() it before? It was created in the constructor, so should be available in the method."))),
        };

        log::info!("Creating a GRPC Broker Server.");
        // mspc Senders can be cloned. Receivers need all the attention and queueing.
        let broker_server = grpc_broker_service::new_server(
            outgoing_conninfo_receiver,
            self.incoming_conninfo_stream_sender.clone(),
        )
        .await?;

        log::info!("Creating a GRPC Controller Server.");
        let controller_server = grpc_controller::new_server(self.trigger.clone());
        log::info!("Creating a GRPC Stdio Server.");
        let stdio_server = grpc_stdio::new_server();

        let listener = self.listener.clone();
        log::info!("Starting service...");
        let grpc_service_future = tonic::transport::Server::builder()
            .add_service(health_service)
            .add_service(broker_server)
            .add_service(controller_server)
            .add_service(stdio_server)
            .add_service(plugin)
            .serve_with_incoming_shutdown(incoming_stream_from_socket, async { listener.await });

        log::info!("About to print handshake string: {}", handshakestr);
        println!("{}", handshakestr);

        // starting broker and plugin services now...
        //join!(broker_service_future, plugin_service_future);
        let result = grpc_service_future.await;

        log::info!("gRPC broker service ended with result: {:?}", result);

        Ok(())
    }
}
