#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose ps
require_service_running ecash-node

info "blockchain"
node_cli getblockchaininfo |
    jq '{chain, blocks, headers, verificationprogress, initialblockdownload}'

info "chainstates"
node_cli getchainstates | jq '{headers, chainstates}'

info "peers"
node_cli getpeerinfo |
    jq '[.[] | {addr, inbound, startingheight, synced_headers, synced_blocks}]'

if ! service_is_running enforcer; then
    info "enforcer is not running; start it with 'just enforcer-up' after the node is ready"
    exit 0
fi

info "enforcer network parameters"
if chain_info="$(enforcer_rpc GetChainInfo)"; then
    jq '{network, bip300Constants}' <<<"${chain_info}"
else
    info "enforcer RPC is not ready yet"
    exit 0
fi

info "enforcer tip"
if chain_tip="$(enforcer_rpc GetChainTip)"; then
    jq '{blockHeaderInfo}' <<<"${chain_tip}"
else
    info "enforcer validator has not established a chain tip yet"
fi
