// Because of course something using Golang and gRPC has to be overtly complex in new and innovative ways.
// The secondary streams brokered by GRPC Broker are JSON-RPC 2.0, wouldn't you know?
use super::unique_port::UniquePort;
use super::Error;
use super::{ConnInfo, Status};
use crate::{function, log_and_escalate};
use jsonrpc_http_server::jsonrpc_core::IoHandler;
use jsonrpc_http_server::ServerBuilder;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

const LOG_PREFIX: &str = "JsonRpcServer:: ";

type ServerId = u32;

pub struct JsonRpcServerBroker {
    unique_port: UniquePort,
    next_id: Mutex<u32>,

    // The IP address to bind to
    bind_ip: String,

    // Send the client information on new services and their endpoints
    tx: UnboundedSender<Result<ConnInfo, Status>>,
}

impl JsonRpcServerBroker {
    pub fn new(
        unique_port: UniquePort,
        bind_ip: String,
        tx: UnboundedSender<Result<ConnInfo, Status>>,
    ) -> Self {
        Self {
            next_id: Mutex::new(1),
            unique_port,
            bind_ip,
            tx,
        }
    }

    pub async fn new_server(&mut self, handler: IoHandler) -> Result<ServerId, Error> {
        log::info!("{} - newServer called", LOG_PREFIX);
        // get next service_id, increment the underlying value, and release lock in the block
        let service_id = self.next_service_id().await;

        log::debug!(
            "{} - newServer - created next service_id: {}",
            LOG_PREFIX,
            service_id
        );
        // let's find an unusued port
        let service_port = match self.unique_port.get_unused_port() {
            Some(p) => p,
            None => return Err(Error::NoTCPPortAvailable),
        };

        let addrstr = format!("{}:{}", self.bind_ip, service_port);
        log::debug!(
            "{} - newServer({}) - created address string: {}",
            LOG_PREFIX,
            service_id,
            addrstr
        );

        let addr = &addrstr.parse()?;

        log::debug!(
            "{} - newServer({}) - about to create server...",
            LOG_PREFIX,
            service_id
        );
        let server = log_and_escalate!(ServerBuilder::new(handler).start_http(addr));

        tokio::spawn(async move {
            log::info!("{} - newServer({}) - spawned into separate task to wait for this server to complete...", LOG_PREFIX, service_id);
            server.wait();
            log::info!(
                "{} - newServer({}) - server.wait() exited. Server has stopped",
                LOG_PREFIX,
                service_id
            );
        });

        log::debug!("{} - newServer({}) - Creating ConnInfo for this service to send to the client-side broker.", LOG_PREFIX, service_id);
        let conn_info = ConnInfo {
            network: "tcp".to_string(),
            address: addrstr,
            service_id,
        };

        log::debug!(
            "{} - newServer({}) - Created ConnInfo for this service: {:?}",
            LOG_PREFIX,
            service_id,
            conn_info
        );

        log_and_escalate!(self.tx.send(Ok(conn_info)));
        log::debug!(
            "{} - newServer({}) - Send ConnInfo to client-side broker",
            LOG_PREFIX,
            service_id
        );

        log::debug!(
            "{} - newServer({}) - returning service_id.",
            LOG_PREFIX,
            service_id
        );
        Ok(service_id)
    }

    pub async fn next_service_id(&mut self) -> u32 {
        let mut next_id = self.next_id.lock().await;
        let service_id = *next_id;
        *next_id += 1;
        service_id
    }

    pub fn get_unused_port(&mut self) -> Option<u16> {
        self.unique_port.get_unused_port()
    }
}
