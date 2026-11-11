[![Build Status](https://github.com/archisgore/grr-plugin/actions/workflows/build.yml/badge.svg)](https://github.com/archisgore/grr-plugin/actions/workflows/build.yml)

# grr-plugin-server
[Hashicorp's go-plugin](https://github.com/hashicorp/go-plugin), for now, only server side (Plugin side) implemented in Rust.

This will allow Rust-based gRPC plugins to be consumed by go programs.

This repo is still being built. The authoritative usage of this crate is in the [Landslide Custom VM](https://github.com/archisgore/landslide) for the [Avalanche blockchain](https://www.avax.network/).

I can imagine this being used for Rust-based plugins for other Hashicorp tools such as Terraform and so forth. I'm sadly not plugged into that ecosystem.

Basic usage looks like this. `service` is your tonic-generated server, e.g. `MyPluginServer::new(MyPlugin)`, built with tonic 0.14:

```rust,no_run
use grr_plugin::{HandshakeConfig, Server};

#[tokio::main]
async fn main() -> Result<(), grr_plugin::error::Error> {
#   // Stand-in for your own tonic-generated plugin service.
#   let (_reporter, service) = tonic_health::server::health_reporter();
    let mut plugin = Server::new(
        1,
        HandshakeConfig {
            magic_cookie_key: "foo".to_string(),
            magic_cookie_value: "bar".to_string(),
        },
    )?;

    // Optional: the GRPCBroker, for serving extra services to (or dialing services
    // on) the host. Must be obtained before `serve`, which blocks until shutdown.
    let _broker = plugin.grpc_broker().await?;

    plugin.serve(service).await?;
    Ok(())
}
```

The service must implement `tower::Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>, Error = Infallible>` + `tonic::server::NamedService` + `Clone + Send + Sync + 'static`. Every tonic 0.14 generated `*Server<T>` does.
