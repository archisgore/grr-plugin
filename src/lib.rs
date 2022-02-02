// A go-plugin Server to write Rust-based plugins to Golang.

mod error;
mod grpc_broker;
mod grpc_controller;
mod grpc_stdio;
mod unique_port;

pub use error::Error;

use http::{Request, Response};
use hyper::Body;
use std::clone::Clone;
use std::env;
use std::marker::Send;
use tonic::body::BoxBody;
use tonic::transport::NamedService;
use tower::Service;

pub use grpc_broker::grpc_plugins::ConnInfo;
pub use tonic::Status;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

// The constants are for generating the go-plugin string
// https://github.com/hashicorp/go-plugin/blob/master/docs/guide-plugin-write-non-go.md
const GRPC_CORE_PROTOCOL_VERSION: usize = 1;

/// Golang/go-plugin don't support IPV6 yet. Yes, yes, I know...
// bind to ALL addresses on Localhost
const LOCALHOST_BIND_ADDR: &str = "0.0.0.0";

// How should other processes on the localhost address localhost?
const LOCALHOST_ADVERTISE_ADDR: &str = "127.0.0.1";

const LOG_PREFIX: &str = "GrrPlugin::Server: ";

pub struct HandshakeConfig {
    pub magic_cookie_key: String,
    pub magic_cookie_value: String,
}

pub struct Server {
    handshake_config: HandshakeConfig,
    protocol_version: u32,
    conn_info_sender: UnboundedSender<Result<ConnInfo, Status>>,
    // Option allows us to take out of the reference.
    conn_info_receiver: Option<UnboundedReceiver<Result<ConnInfo, Status>>>,
}

impl Server {
    pub fn new(protocol_version: u32, handshake_config: HandshakeConfig) -> Server {
        let (tx, rx) = unbounded_channel::<Result<ConnInfo, Status>>();
        Server {
            handshake_config,
            protocol_version,
            conn_info_sender: tx,
            conn_info_receiver: Some(rx),
        }
    }

    pub fn get_conn_info_sender(&self) -> UnboundedSender<Result<ConnInfo, Status>> {
        self.conn_info_sender.clone()
    }

    // Copied from: https://github.com/hashicorp/go-plugin/blob/master/server.go#L247
    fn validate_magic_cookie(&self) -> Result<(), Error> {
        log::info!("{}Validating the magic environment cookies to conduct the handshake. Expecting environment variable {}={}.",LOG_PREFIX, self.handshake_config.magic_cookie_key, self.handshake_config.magic_cookie_value);
        match env::var(&self.handshake_config.magic_cookie_key) {
            Ok(value) => {
                if value == self.handshake_config.magic_cookie_value {
                    log::info!("{}Handshake succeeded!", LOG_PREFIX);
                    return Ok(());
                } else {
                    log::error!("{}Handshake failed due to environment variable {}'s value being {}, but expected to be {}.", LOG_PREFIX,self.handshake_config.magic_cookie_key, value, self.handshake_config.magic_cookie_value);
                }
            }
            Err(e) => log::error!(
                "{}Handshake failed due to error reading environment variable {}: {:?}",
                LOG_PREFIX,
                self.handshake_config.magic_cookie_key,
                e
            ),
        }

        Err(Error::GRPCHandshakeMagicCookieValueMismatch)
    }

    pub async fn serve<S>(&mut self, plugin: S) -> Result<(), Error>
    where
        S: Service<Request<Body>, Response = Response<BoxBody>>
            + NamedService
            + Clone
            + Send
            + 'static,
        <S as Service<http::Request<hyper::Body>>>::Future: Send + 'static,
        <S as Service<http::Request<hyper::Body>>>::Error:
            Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        log::info!("{}serve - serving over a Tcp Socket...", LOG_PREFIX);

        log_and_escalate!(self.validate_magic_cookie());

        let mut unique_ports = unique_port::UniquePort::new();

        let (trigger, listener) = triggered::trigger();

        let service_port = match unique_ports.get_unused_port() {
            Some(p) => p,
            None => {
                return Err(Error::Generic(
                    "Unable to find a free unused TCP port to bind the gRPC server to".to_string(),
                ));
            }
        };

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<S>().await;
        log::info!("{}serve -  gRPC Health Service created.", LOG_PREFIX);

        log::info!("{}serve - picked broker port: {}", LOG_PREFIX, service_port);

        let addrstr = format!("{}:{}", LOCALHOST_BIND_ADDR, service_port);
        let addr = log_and_escalate!(addrstr.parse());

        let handshakestr = format!(
            "{}|{}|tcp|{}:{}|grpc|",
            GRPC_CORE_PROTOCOL_VERSION,
            self.protocol_version,
            LOCALHOST_ADVERTISE_ADDR,
            service_port
        );

        log::info!(
            "{}serve - Created Handshake string: {}",
            LOG_PREFIX,
            handshakestr
        );

        // take conn_info_receiver from plugin
        let cir = match self.conn_info_receiver.take() {
            Some(cir) => cir,
            None => return Err(Error::ConnInfoReceiverMissing),
        };

        log::info!("{} serve - Creating a GRPC Broker Server.", LOG_PREFIX);
        let broker_server = grpc_broker::new_server(cir).await;
        log::info!("{} serve - Creating a GRPC Controller Server.", LOG_PREFIX);
        let controller_server = grpc_controller::new_server(trigger);
        log::info!("{} serve - Creating a GRPC Stdio Server.", LOG_PREFIX);
        let stdio_server = grpc_stdio::new_server();

        log::info!(
            "{} serve - All servers created. Spawning off a new task to serve them.",
            LOG_PREFIX
        );
        log::info!("{}serve - Creating a broker service future.", LOG_PREFIX);
        let grpc_service_future = tonic::transport::Server::builder()
            .add_service(health_service)
            .add_service(broker_server)
            .add_service(controller_server)
            .add_service(stdio_server)
            .add_service(plugin)
            .serve_with_shutdown(addr, async { listener.await });

        log::info!(
            "{}About to print handshake string: {}",
            LOG_PREFIX,
            handshakestr
        );
        println!("{}", handshakestr);

        // starting broker and plugin services now...
        //join!(broker_service_future, plugin_service_future);
        let result = grpc_service_future.await;

        log::info!(
            "{}gRPC broker service ended with result: {:?}",
            LOG_PREFIX,
            result
        );

        Ok(())
    }
}
