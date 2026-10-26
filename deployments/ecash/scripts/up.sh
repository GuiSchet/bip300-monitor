#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

allow_node_bootstrap
load_versions
load_deployment_env
require_command docker

compose config --quiet
info "pulling the pinned ${NETWORK_ID} node image"
compose pull ecash-node
info "starting the ${NETWORK_ID} node"
compose up --detach ecash-node
compose ps
# Until the pinned snapshot is loaded the node validates from genesis, which
# takes weeks on a disk like mainnet's. Start the load now, not after someone
# notices the headers: it waits for them on its own.
if [[ "${SNAPSHOT_ON_UP}" == true ]]; then
    start_snapshot_loader
else
    info "SNAPSHOT_ON_UP=false: run 'just snapshot' to load the pinned snapshot"
fi
info "the node may take time to download headers; run 'just status'"
