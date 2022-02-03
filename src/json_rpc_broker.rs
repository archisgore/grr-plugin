// Because of course something using Golang and gRPC has to be overtly complex in new and innovative ways.
// The secondary streams brokered by GRPC Broker are JSON-RPC 2.0, wouldn't you know?
use super::unique_port::UniquePort;
use super::Error;
use super::{ConnInfo, Status};
use crate::{function, log_and_escalate};
use async_recursion::async_recursion;
use futures::stream::StreamExt;
use jsonrpc_http_server::jsonrpc_core::IoHandler;
use jsonrpc_http_server::ServerBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Streaming;
use tower::service_fn;

const LOG_PREFIX: &str = "JsonRpcBroker:: ";

type ServiceId = u32;

// Brokers connections by service_id
// Not necessarily threadsafe, so caller should Arc<RwLock<>> this,
// but I don't know how you'd get multiple mutable references without that anyway.
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
    // The optional ConnInfo entry allows us to park a None
    // against a ServiceId that was previously used, so it won't get
    // reused.
    host_services: Arc<Mutex<HashMap<ServiceId, Option<ConnInfo>>>>,
}

impl JsonRpcBroker {
    pub fn new(
        unique_port: UniquePort,
        bind_ip: String,
        advertise_ip: String,
        outgoing_conninfo_sender: UnboundedSender<Result<ConnInfo, Status>>,
        mut incoming_conninfo_stream_receiver_receiver: UnboundedReceiver<Streaming<ConnInfo>>,
    ) -> Self {
        log::trace!("{} - new - called", LOG_PREFIX);
        let host_services = Arc::new(Mutex::new(HashMap::new()));

        log::trace!("{} - new - spawning a process to receive the stream of incoming ConnInfo's, and then the ConnInfo's themselves from host side...", LOG_PREFIX);
        let host_services_for_closure = host_services.clone();
        tokio::spawn(async move {
            log::trace!("{} - new - Inside spawn'd process. Waiting for the stream of ConnInfo's to be available....", LOG_PREFIX);
            let incoming_conninfo_stream = match incoming_conninfo_stream_receiver_receiver
                .recv()
                .await
            {
                Some(incoming_conninfo_stream) => incoming_conninfo_stream,
                None => {
                    log::error!("{} - new - inside spawn'd process to wait for a Stream of ConnInfo's, the stream was None, which is unexpected, since it is expected instead to block indefinitely until such a stream is available.", LOG_PREFIX);
                    return;
                }
            };

            Self::blocking_incoming_conn(incoming_conninfo_stream, host_services_for_closure).await
        });

        Self {
            next_id: Mutex::new(1), // start next id at a number where it won't conflict with other services
            unique_port,
            bind_ip,
            advertise_ip,
            outgoing_conninfo_sender,
            host_services,
        }
    }

    pub async fn new_server(&mut self, handler: IoHandler) -> Result<ServiceId, Error> {
        log::trace!("{} - newServer called", LOG_PREFIX);
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
        log::trace!(
            "{} - newServer({}) - created bind address string: {}",
            LOG_PREFIX,
            service_id,
            bind_addrstr
        );

        let bind_addr = &bind_addrstr.parse()?;

        log::trace!(
            "{} - newServer({}) - about to create server...",
            LOG_PREFIX,
            service_id
        );
        let server = log_and_escalate!(ServerBuilder::new(handler).start_http(bind_addr));

        tokio::spawn(async move {
            log::trace!("{} - newServer({}) - spawned into separate task to wait for this server to complete...", LOG_PREFIX, service_id);
            server.wait();
            log::info!(
                "{} - newServer({}) - server.wait() exited. Server has stopped",
                LOG_PREFIX,
                service_id
            );
        });

        let advertise_addrstr = format!("{}:{}", self.advertise_ip, service_port);
        log::trace!(
            "{} - newServer({}) - created advertise address string: {}",
            LOG_PREFIX,
            service_id,
            advertise_addrstr
        );

        log::trace!("{} - newServer({}) - Creating ConnInfo for this service to send to the client-side broker.", LOG_PREFIX, service_id);
        let conn_info = ConnInfo {
            network: "tcp".to_string(),
            address: advertise_addrstr,
            service_id,
        };

        log::trace!(
            "{} - newServer({}) - Created ConnInfo for this service: {:?}",
            LOG_PREFIX,
            service_id,
            conn_info
        );

        log_and_escalate!(self.outgoing_conninfo_sender.send(Ok(conn_info)));
        log::trace!(
            "{} - newServer({}) - Send ConnInfo to client-side broker",
            LOG_PREFIX,
            service_id
        );

        log::trace!(
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

    pub async fn dial_to_host_service(&mut self, service_id: ServiceId) -> Result<Channel, Error> {
        let conn_info = self.get_incoming_conninfo_retry(service_id, 5).await?;

        let channel = match conn_info.network.as_str() {
            "tcp" => Endpoint::try_from(conn_info.address)?.connect().await?,
            "unix" => {
                // Copied from: https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
                Endpoint::try_from("http://[::]:50051")?
                    .connect_with_connector(service_fn(move |_: Uri| {
                        // Connect to a Uds socket
                        // The clone ensures this closure doesn't consume the environment.
                        UnixStream::connect(conn_info.address.clone())
                    }))
                    .await?
            }
            s => return Err(Error::NetworkTypeUnknown(s.to_string())),
        };

        Ok(channel)
    }

    #[async_recursion]
    async fn get_incoming_conninfo_retry(
        &mut self,
        service_id: ServiceId,
        retry_count: usize,
    ) -> Result<ConnInfo, Error> {
        match self.get_incoming_conninfo(service_id).await {
            None => match retry_count {
                0 => Err(Error::ServiceIdDoesNotExist(service_id)),
                _c => {
                    sleep(Duration::from_secs(1)).await;
                    self.get_incoming_conninfo_retry(service_id, retry_count - 1)
                        .await
                }
            },
            Some(conn_info) => Ok(conn_info),
        }
    }

    //https://github.com/hashicorp/go-plugin/blob/master/grpc_broker.go#L371
    async fn get_incoming_conninfo(&mut self, service_id: ServiceId) -> Option<ConnInfo> {
        // hold lock for duration of this function, so we can atomically park a None
        // in case we pulled a ConnInfo out.
        let mut hs = self.host_services.lock().await;

        match hs.remove(&service_id) {
            None | Some(None) => None,
            Some(Some(conn_info)) => {
                // if some conn_info existed, replace it with None before exiting
                hs.insert(service_id, None);
                Some(conn_info)
            }
        }
    }

    // This function will run forever. tokio::spawn this!
    async fn blocking_incoming_conn(
        mut stream: Streaming<ConnInfo>,
        host_services: Arc<Mutex<HashMap<ServiceId, Option<ConnInfo>>>>,
    ) {
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
                Ok(conn_info) => {
                    log::info!("{}Received conn_info: {:?}", LOG_PREFIX, conn_info);

                    let mut hs = host_services.lock().await;
                    log::trace!(
                        "{}Write-locked the host services to add the new ConnInfo",
                        LOG_PREFIX
                    );

                    log::trace!(
                        "{}Only creating a new entry if one doesn't exist for this ServiceId: {}",
                        LOG_PREFIX,
                        conn_info.service_id
                    );
                    hs.entry(conn_info.service_id)
                        .or_insert_with(|| Some(conn_info));
                }
            }
        }
        log::info!(
            "{}blocking_incoming_conn - exiting due to stream returning None or an error",
            LOG_PREFIX
        );
    }
}
