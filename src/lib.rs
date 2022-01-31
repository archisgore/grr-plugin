// A go-plugin Server to write Rust-based plugins to Golang.

mod error;
mod plugin_service;

use error::Error;
use std::env;
use plugin_service::PluginService;

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

    pub async fn serve<S: PluginService>(&self, service: S) -> Result<(), Error>
    {
        log::info!("{}Serving over a Tcp Socket...", LOG_PREFIX);

        log_and_escalate!(self.validate_magic_cookie());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<S>().await;
        log::info!("{}gRPC Health Service created.", LOG_PREFIX);

        let port = match portpicker::pick_unused_port() {
            Some(p) => p,
            None => {
                return Err(Error::Generic(
                    "Unable to find a free unused TCP port to bind the gRPC server to".to_string(),
                ));
            }
        };

        log::info!("{}Picked port: {}", LOG_PREFIX, port);

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

        log::info!("About to begin serving....");
        log_and_escalate!(
            tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(service)
                .serve(addr)
                .await
        );

        log::info!("{}Serving ended! Plugin about to shut down.", LOG_PREFIX,);

        Ok(())
    }
}
