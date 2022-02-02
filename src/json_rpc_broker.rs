// Because of course something using Golang and gRPC has to be overtly complex in new and innovative ways.
// The secondary streams brokered by GRPC Broker are JSON-RPC 2.0, wouldn't you know?
use super::unique_port::UniquePort;
use super::Error;
use super::{ConnInfo, Status};
use crate::{function, log_and_escalate};
use jsonrpc_http_server::jsonrpc_core::IoHandler;
use jsonrpc_http_server::ServerBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

const LOG_PREFIX: &str = "JsonRpcServer:: ";

type ServerId = u32;

pub struct JsonRpcBroker {
    unique_port: UniquePort,
    next_id: Mutex<u32>,

    // The IP address to bind to (usually 0.0.0.0 or equivalent)
    bind_ip: String,

    // The IP address to advertise (usually 127.0.0.1 or equivalent)
    advertise_ip: String,

    // Send the client information on new services and their endpoints
    outgoing_conninfo_sender: UnboundedSender<Result<ConnInfo, Status>>,

    // Services on the host-side that we've been informed of
    host_services: Arc<RwLock<HashMap<ServerId, ConnInfo>>>,
}

impl JsonRpcBroker {
    pub fn new(
        unique_port: UniquePort,
        bind_ip: String,
        advertise_ip: String,
        outgoing_conninfo_sender: UnboundedSender<Result<ConnInfo, Status>>,
    ) -> Self {
        Self {
            next_id: Mutex::new(1),
            unique_port,
            bind_ip,
            advertise_ip,
            outgoing_conninfo_sender,
            host_services: Arc::new(RwLock::new(HashMap::new())),
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

        let bind_addrstr = format!("{}:{}", self.bind_ip, service_port);
        log::debug!(
            "{} - newServer({}) - created bind address string: {}",
            LOG_PREFIX,
            service_id,
            bind_addrstr
        );

        let bind_addr = &bind_addrstr.parse()?;

        log::debug!(
            "{} - newServer({}) - about to create server...",
            LOG_PREFIX,
            service_id
        );
        let server = log_and_escalate!(ServerBuilder::new(handler).start_http(bind_addr));

        tokio::spawn(async move {
            log::info!("{} - newServer({}) - spawned into separate task to wait for this server to complete...", LOG_PREFIX, service_id);
            server.wait();
            log::info!(
                "{} - newServer({}) - server.wait() exited. Server has stopped",
                LOG_PREFIX,
                service_id
            );
        });

        let advertise_addrstr = format!("{}:{}", self.advertise_ip, service_port);
        log::debug!(
            "{} - newServer({}) - created advertise address string: {}",
            LOG_PREFIX,
            service_id,
            advertise_addrstr
        );

        log::debug!("{} - newServer({}) - Creating ConnInfo for this service to send to the client-side broker.", LOG_PREFIX, service_id);
        let conn_info = ConnInfo {
            network: "tcp".to_string(),
            address: advertise_addrstr,
            service_id,
        };

        log::debug!(
            "{} - newServer({}) - Created ConnInfo for this service: {:?}",
            LOG_PREFIX,
            service_id,
            conn_info
        );

        log_and_escalate!(self.outgoing_conninfo_sender.send(Ok(conn_info)));
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
