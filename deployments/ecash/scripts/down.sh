#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

allow_node_bootstrap
load_versions
load_deployment_env
compose down
info "containers stopped; persistent data was not deleted"
