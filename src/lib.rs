// A go-plugin Server to write Rust-based plugins to Golang.

mod body;
mod error;
mod grpc_broker;
mod grpc_controller;
mod grpc_stdio;
mod unique_port;

use error::Error;

use grpc_broker::ConnInfo;
use grpc_broker::ConnInfoSender;
use http::{Request, Response};
use hyper::Body;
use std::clone::Clone;
use std::env;
use std::marker::Send;
use tonic::body::BoxBody;
use tonic::transport::NamedService;
use tower::Service;

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
}

impl Server {
    pub fn new(protocol_version: u32, handshake_config: HandshakeConfig) -> Server {
        Server {
            handshake_config,
            protocol_version,
        }
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

    pub async fn serve<S>(&self, plugin: S) -> Result<(), Error>
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
        log::info!("{}Serving over a Tcp Socket...", LOG_PREFIX);

        log_and_escalate!(self.validate_magic_cookie());

        let mut unique_ports = unique_port::UniquePort::new();

        let (trigger, listener) = triggered::trigger();

        let broker_port = match unique_ports.get_unused_port() {
            Some(p) => p,
            None => {
                return Err(Error::Generic(
                    "Unable to find a free unused TCP port to bind the gRPC server to".to_string(),
                ));
            }
        };

        let plugin_port = match unique_ports.get_unused_port() {
            Some(p) => p,
            None => {
                return Err(Error::Generic(
                    "Unable to find a free unused TCP port to bind the gRPC server to".to_string(),
                ));
            }
        };

        let mut conn_info_sender = self
            .serve_broker::<S>(broker_port, trigger, listener.clone())
            .await?;

        self.serve_plugin::<S>(plugin, plugin_port, listener.clone(), &mut conn_info_sender)
            .await?;

        Ok(())
    }

    async fn serve_plugin<S>(
        &self,
        plugin: S,
        port: portpicker::Port,
        listener: triggered::Listener,
        conn_info_sender: &mut ConnInfoSender,
    ) -> Result<(), Error>
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
        log::info!("{}Picked plugin port: {}", LOG_PREFIX, port);

        let plugin_addrstr = format!("{}:{}", LOCALHOST_BIND_ADDR, port);
        let plugin_addr = log_and_escalate!(plugin_addrstr.parse());

        // Send connection info for the plugin to the client
        let conn_info = ConnInfo {
            network: "tcp".to_string(),
            address: plugin_addrstr,
            service_id: 50,
        };

        log::info!(
            "{} serve_plugin - About to send plugin's conn_info to client: {:?}",
            LOG_PREFIX,
            conn_info
        );

        log_and_escalate!(conn_info_sender.send(conn_info).await);

        let plugin_listener = listener.clone();
        log::info!("{}About to begin serving plugin....", LOG_PREFIX);
        let result = tonic::transport::Server::builder()
            .add_service(plugin)
            .serve_with_shutdown(plugin_addr, async { plugin_listener.await })
            .await;

        log::info!(
            "{}Plugin serving ended with result: {:?}",
            LOG_PREFIX,
            result,
        );

        Ok(())
    }

    async fn serve_broker<S>(
        &self,
        port: portpicker::Port,
        trigger: triggered::Trigger,
        listener: triggered::Listener,
    ) -> Result<ConnInfoSender, Error>
    where
        S: NamedService,
    {
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<S>().await;
        log::info!("{}gRPC Health Service created.", LOG_PREFIX);

        log::info!("{}Picked broker port: {}", LOG_PREFIX, port);

        let addrstr = format!("{}:{}", LOCALHOST_BIND_ADDR, port);
        let addr = log_and_escalate!(addrstr.parse());

        let handshakestr = format!(
            "{}|{}|tcp|{}:{}|grpc|",
            GRPC_CORE_PROTOCOL_VERSION, self.protocol_version, LOCALHOST_ADVERTISE_ADDR, port
        );

        log::info!(
            "{}About to print Handshake string: {}",
            LOG_PREFIX,
            handshakestr
        );
        println!("{}", handshakestr);

        log::info!(
            "{} serve_broker - Creating a GRPC Broker Server.",
            LOG_PREFIX
        );
        let (broker_server, conn_info_sender) = grpc_broker::new_server().await;
        log::info!(
            "{} serve_broker - Creating a GRPC Controller Server.",
            LOG_PREFIX
        );
        let controller_server = grpc_controller::new_server(trigger);
        log::info!(
            "{} serve_broker - Creating a GRPC Stdio Server.",
            LOG_PREFIX
        );
        let stdio_server = grpc_stdio::new_server();

        log::info!(
            "{} serve_broker - All servers created. Spawning off a new task to serve them.",
            LOG_PREFIX
        );
        tokio::spawn(async move {
            log::info!(
                "{}serve_broker - About to begin serving broker....",
                LOG_PREFIX
            );
            let result = tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(broker_server)
                .add_service(controller_server)
                .add_service(stdio_server)
                .serve_with_shutdown(addr, async { listener.await })
                .await;

            log::info!(
                "{} serve_broker - Exited with result: {:?}",
                LOG_PREFIX,
                result
            );
        });

        Ok(conn_info_sender)
    }
}
