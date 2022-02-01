#!/bin/sh
set -ex

type wget >/dev/null 2>&1 || { echo >&2 "This requires the wget command to download the protobuf file"; exit 1; }

function grpcbrokerproto() {
    proto_filename="grpc_broker.proto"
    remote_path="https://raw.githubusercontent.com/hashicorp/go-plugin/master/internal/plugin/"
    local_path="./proto/"
    echo "Updating the gRPC Broker protobuf definition from upstream Hashicorp go-plugin repo: $remote_path"
    wget ${remote_path}${proto_filename} -O ${local_path}${proto_filename} 
}


function grpccontrollerproto() {
    proto_filename="grpc_controller.proto"
    remote_path="https://raw.githubusercontent.com/hashicorp/go-plugin/master/internal/plugin/"
    local_path="./proto/"
    echo "Updating the gRPC Broker protobuf definition from upstream Hashicorp go-plugin repo: $remote_path"
    wget ${remote_path}${proto_filename} -O ${local_path}${proto_filename} 
}


function grpcstdioproto() {
    proto_filename="grpc_stdio.proto"
    remote_path="https://raw.githubusercontent.com/hashicorp/go-plugin/master/internal/plugin/"
    local_path="./proto/"
    echo "Updating the gRPC Broker protobuf definition from upstream Hashicorp go-plugin repo: $remote_path"
    wget ${remote_path}${proto_filename} -O ${local_path}${proto_filename} 
}
grpcbrokerproto
grpccontrollerproto
