#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose ps
if ! compose ps --status running --quiet ecash-node | grep -q .; then
    die "ecash-node is not running"
fi

info "blockchain"
node_cli getblockchaininfo |
    jq '{chain, blocks, headers, verificationprogress, initialblockdownload}'

info "chainstates"
node_cli getchainstates | jq '{headers, chainstates}'

info "peers"
node_cli getpeerinfo |
    jq '[.[] | {addr, inbound, startingheight, synced_headers, synced_blocks}]'
