// Because of course something using Golang and gRPC has to be overtly complex in new and innovative ways.
// The secondary streams brokered by GRPC Broker are JSON-RPC 2.0, wouldn't you know?
use super::Error;
use super::{ConnInfo, Status};
use crate::{function, log_and_escalate};
use std::collections::HashMap;
use tonic::Streaming;
use std::sync::Arc;
use tokio_stream::StreamExt;
use futures::stream::Stream;
use tokio::sync::mpsc::UnboundedReceiver;

const LOG_PREFIX: &str = "JsonRpcClient:: ";

type ServiceId = u32;

pub struct JsonRpcClientBroker {
    services: Arc<RwLock<HashMap<ServiceId, ConnInfo>>>,
}

impl JsonRpcClientBroker {
    pub fn new(
        mut stream: Streaming<ConnInfo>,
    ) -> Self {
        let services = Arc::new(RwLock::new(HashMap::new()));

        // start the listener thread...

        Self {
            services,
        }
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
                Ok(conn_info) => {
                    log::debug!("{}Received conn_info: {:?}", LOG_PREFIX, conn_info);

                    tokio::spawn(async move {                        
                        log::debug!("{}Spawn'd a new task to deal with it: {:?}", LOG_PREFIX, conn_info);
                        if let Err(e) = handle_reverse_conn(&conn_info).await {
                            log::error!("{}Error when handling a reverse connection to conn_info {:?} : {:?}", LOG_PREFIX, conn_info, e);
                        }
                    });
                }
            }
        }

        async fn handle_reverse_conn(conn_info: &ConnInfo) -> Result<(), Error> {
            log::info!("{} handle_reverse_conn - opening reverse connnection to {:?}", LOG_PREFIX, conn_info);
            let conn = match conn_info.network.as_str() {
                "tcp" => http::connect(conn_info.address).await,
                "unix" => ipc::connect(conn_info.address).await,
                _ => return Err(Error::Generic(format!("{} opening reverse connection to {:?} because of unknown network type. Only 'tcp' and 'unix' are supported for now.", LOG_PREFIX, conn_info))),
            };

            log::info!("{} handle_reverse_conn - opened connnection: {:?}", LOG_PREFIX, conn);

            Ok(())
        }

        log::info!(
            "{}blocking_incoming_conn - exiting due to stream returning None or an error",
            LOG_PREFIX
        );
    }
}

