// Because of course something using Golang and gRPC has to be overtly complex in new and innovative ways.
// The secondary streams brokered by GRPC Broker are JSON-RPC 2.0, wouldn't you know?
use super::unique_port::UniquePort;
use super::unix::{incoming_from_path, TempSocket};
use super::Error;
use super::ServiceId;
use super::{ConnInfo, Status};
use anyhow::anyhow;
use anyhow::{Context, Result};
use async_recursion::async_recursion;
use futures::stream::StreamExt;
use hyper::{Body, Request, Response};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tonic::body::BoxBody;
use tonic::transport::NamedService;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Streaming;
use tower::service_fn as tower_service_fn;
use tower::Service;

// Brokers connections by service_id
// Not necessarily threadsafe, so caller should Arc<RwLock<>> this,
// but I don't know how you'd get multiple mutable references without that anyway.
pub struct GRpcBroker {
    unique_port: UniquePort,
    used_ids: HashSet<u32>,
    next_id: u32,

    listener: triggered::Listener,

    // Send the client information on new services and their endpoints
    outgoing_conninfo_sender: UnboundedSender<Result<ConnInfo, Status>>,

    // Services on the host-side that we've been informed of
    // The optional ConnInfo entry allows us to park a None
    // against a ServiceId that was previously used, so it won't get
    // reused.
    host_services: Arc<Mutex<HashMap<ServiceId, Option<ConnInfo>>>>,
}

impl GRpcBroker {
    pub fn new(
        unique_port: UniquePort,
        outgoing_conninfo_sender: UnboundedSender<Result<ConnInfo, Status>>,
        mut incoming_conninfo_stream_receiver_receiver: UnboundedReceiver<Streaming<ConnInfo>>,
        listener: triggered::Listener,
    ) -> Self {
        log::info!("Creating new GrpcBroker");
        let host_services = Arc::new(Mutex::new(HashMap::new()));

        log::trace!("spawning a process to receive the stream of incoming ConnInfo's, and then the ConnInfo's themselves from host side...");
        let host_services_for_closure = host_services.clone();
        tokio::spawn(async move {
            log::trace!(
                "Inside spawn'd process. Waiting for the stream of ConnInfo's to be available...."
            );
            let incoming_conninfo_stream = match incoming_conninfo_stream_receiver_receiver
                .recv()
                .await
            {
                Some(incoming_conninfo_stream) => incoming_conninfo_stream,
                None => {
                    log::error!("inside spawn'd process to wait for a Stream of ConnInfo's, the stream was None, which is unexpected, since it is expected instead to block indefinitely until such a stream is available.");
                    return;
                }
            };

            Self::blocking_incoming_conn(incoming_conninfo_stream, host_services_for_closure).await
        });

        Self {
            next_id: 1, // start next id at a number where it won't conflict with other services
            used_ids: HashSet::new(),
            unique_port,
            outgoing_conninfo_sender,
            host_services,
            listener,
        }
    }

    pub async fn new_grpc_server<S>(&mut self, plugin: S) -> Result<ServiceId, Error>
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        <S as Service<http::Request<hyper::Body>>>::Future: Send + 'static,
        <S as Service<http::Request<hyper::Body>>>::Error:
            Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        log::info!("called");

        // get next service_id, increment the underlying value, and release lock in the block
        let service_id = self.get_unused_service_id();
        log::info!("newServer - obtained an unused service_id: {}", service_id);

        self.new_grpc_server_with_service_id(service_id, plugin)
            .await
    }

    pub async fn new_grpc_server_with_service_id<S>(
        &mut self,
        service_id: ServiceId,
        plugin: S,
    ) -> Result<ServiceId, Error>
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        <S as Service<http::Request<hyper::Body>>>::Future: Send + 'static,
        <S as Service<http::Request<hyper::Body>>>::Error:
            Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        log::info!("called");

        if self.used_ids.contains(&service_id) {
            return Err(Error::Other(anyhow!("In GrpcBroker, the service_id {} was provided to open a new server with, but it was found to exist already in the used set.", service_id)));
        }

        // reserve current service_id
        self.used_ids.insert(service_id);

        let temp_socket = TempSocket::new()
        .with_context(|| format!("newServer({}) Failed to create a new TempSocket for opening a new JSON-RPC 2.0 server", service_id))?;
        let socket_path = temp_socket.socket_filename()
            .with_context(|| format!("newServer({}) Failed to get a temporary socket filename from the temp socket for opening a new JSON-RPC 2.0 server", service_id))?;
        log::info!(
            "newServer({}) Created a temp socket path: {}",
            service_id,
            socket_path
        );

        let listener = self.listener.clone();

        tokio::spawn(async move {
            log::debug!(
                "newServer({}) - spawned into separate task to wait for this server to complete...",
                service_id
            );

            let socket_path = temp_socket.socket_filename()
            .with_context(|| format!("newServer({}) Inside spawned grpc server, failed to get a temporary socket filename from the temp socket for opening a new JSON-RPC 2.0 server", service_id)).unwrap();

            // create incoming stream from unix socket above...
            let incoming_stream_from_socket = incoming_from_path(socket_path.as_str()).await
                .with_context(|| format!("newServer({}) Inside spawned grpc server, unable to open incoming UnixStream from socket {}", service_id, socket_path.as_str())).unwrap();
            log::trace!("newServer({}) Inside spawned grpc server, created Incoming unix stream from the socket", service_id);

            log::info!(
                "newServer({}) Inside spawned grpc server, starting a new grpc service...",
                service_id
            );
            let grpc_service_future = tonic::transport::Server::builder()
                .add_service(plugin)
                .serve_with_incoming_shutdown(incoming_stream_from_socket, listener);

            if let Err(err) = grpc_service_future.await.with_context(|| {
                format!(
                    "newServer({}) Inside spawned grpc server, service future failed",
                    service_id
                )
            }) {
                log::error!(
                    "newServer({}) Inside spawned grpc server, it errored: {}",
                    service_id,
                    err
                );
            }

            log::info!(
                "newServer({}) Inside spawned grpc server, exiting task. Service has ended.",
                service_id
            );
        });

        log::debug!(
            "newServer({}) - Creating ConnInfo for this service to send to the client-side broker.",
            service_id
        );
        let conn_info = ConnInfo {
            network: "unix".to_string(),
            address: socket_path,
            service_id,
        };

        log::debug!(
            "newServer({}) - Created ConnInfo for this service: {:?}",
            service_id,
            conn_info
        );

        self.outgoing_conninfo_sender
            .send(Ok(conn_info.clone()))
            .with_context(|| {
                format!(
                    "Failed to send ConnInfo {:?} to the client/host/consumer of this plugin.",
                    conn_info
                )
            })?;
        log::info!(
            "newServer({}) - Sent ConnInfo to client-side broker",
            service_id
        );

        Ok(service_id)
    }

    pub fn get_unused_service_id(&mut self) -> u32 {
        // keep incrementing next_id so long as it has already been used.
        while self.used_ids.contains(&self.next_id) {
            self.next_id += 1;
        }

        // return service_id that is not yet used.
        self.next_id
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
                    .connect_with_connector(tower_service_fn(move |_: Uri| {
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
        log::info!("blocking_incoming_conn - perpetually listening for incoming ConnInfo's",);
        while let Some(conn_info_result) = stream.next().await {
            match conn_info_result {
                Err(e) => {
                    log::error!(
                        "blocking_incoming_conn - an error occurred reading from the stream: {:?}",
                        e
                    );
                    break; //out of the while loop
                }
                Ok(conn_info) => {
                    log::info!("Received conn_info: {:?}", conn_info);

                    let mut hs = host_services.lock().await;
                    log::trace!("Write-locked the host services to add the new ConnInfo",);

                    log::trace!(
                        "Only creating a new entry if one doesn't exist for this ServiceId: {}",
                        conn_info.service_id
                    );
                    hs.entry(conn_info.service_id)
                        .or_insert_with(|| Some(conn_info));
                }
            }
        }
        log::info!("blocking_incoming_conn - exiting due to stream returning None or an error",);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::unique_port;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn test_service_id_increment() {
        let (_t, l) = triggered::trigger();
        let (t1, _r1) = unbounded_channel::<Result<ConnInfo, Status>>();
        let (_t2, r2) = unbounded_channel::<Streaming<ConnInfo>>();
        let mut g = GRpcBroker::new(unique_port::UniquePort::new(), t1, r2, l);

        g.used_ids.insert(5);

        assert_eq!(1, g.get_unused_service_id());
        // still unuused
        assert_eq!(1, g.get_unused_service_id());
        g.used_ids.insert(1);
        assert_eq!(2, g.get_unused_service_id());
        g.used_ids.insert(2);
        assert_eq!(3, g.get_unused_service_id());
        g.used_ids.insert(3);
        assert_eq!(4, g.get_unused_service_id());
        g.used_ids.insert(4);

        // skip 5 which was pre-inserted
        assert_eq!(6, g.get_unused_service_id());
    }
}
