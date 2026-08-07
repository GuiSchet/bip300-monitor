#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose config --quiet
require_node_ready
require_node_history_ready

info "pulling the pinned enforcer image"
compose pull enforcer
info "starting the Drynet3 enforcer"
compose up --detach enforcer
compose ps enforcer
info "the initial validator sync may take time; run 'just status', then 'just monitor-up'"
