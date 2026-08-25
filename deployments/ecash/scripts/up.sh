#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker

compose config --quiet
info "pulling the pinned ${NETWORK_ID} node image"
compose pull ecash-node
info "starting the ${NETWORK_ID} node"
compose up --detach ecash-node
compose ps
info "the node may take time to download headers; run 'just status'"
