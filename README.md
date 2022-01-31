[![Build Status](https://github.com/archisgore/grr-plugin/actions/workflows/build.yml/badge.svg)](https://github.com/archisgore/grr-plugin/actions/workflows/build.yml)

# grr-plugin-server
[Hashicorp's go-plugin](https://github.com/hashicorp/go-plugin), for now, only server side (Plugin side) implemented in Rust.

This will allow Rust-based gRPC plugins to be consumed by go programs.

This repo is still being built. The authoritative usage of this crate is in the [Landslide Custom VM](https://github.com/archisgore/landslide) for the [Avalanche blockchain](https://www.avax.network/).

I can imagine this being used for Rust-based plugins for other Hashicorp tools such as Terraform and so forth. I'm sadly not plugged into that ecosystem.

Basic usage looks like:

```.rust
    let plugin = Server::new(1, HandshakeConfig{
        magic_cookie_key: "foo".to_string(),
        magic_cookie_value: "bar".to_string(),
    });

    plugin.serve(service).await?;
```

