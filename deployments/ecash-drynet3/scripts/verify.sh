#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose config --quiet
compose ps --status running --quiet ecash-node | grep -q . ||
    die "ecash-node is not running; run 'just up' first"

blockchain_info="$(node_cli getblockchaininfo)"
jq -e '.chain == "main"' <<<"${blockchain_info}" >/dev/null ||
    die "ecash-node is not using the expected main-chain network magic"
jq -e --argjson height "${DRYNET_ACTIVATION_HEIGHT}" \
    '.blocks >= $height and .headers >= $height' \
    <<<"${blockchain_info}" >/dev/null ||
    die "ecash-node has not reached Drynet3 activation height ${DRYNET_ACTIVATION_HEIGHT}"
jq -e '.initialblockdownload == false' <<<"${blockchain_info}" >/dev/null ||
    die "ecash-node is still in initial block download"

require_drynet_activation_block

chainstates="$(node_cli getchainstates)"
jq -e '.chainstates | length >= 1' <<<"${chainstates}" >/dev/null ||
    die "ecash-node returned no usable chainstate"

config_json="$(compose config --format json)"
jq -e '[.services[]?.ports[]?] | length == 0' <<<"${config_json}" >/dev/null ||
    die "the deployment unexpectedly publishes a host port"

blocks="$(jq -r '.blocks' <<<"${blockchain_info}")"
headers="$(jq -r '.headers' <<<"${blockchain_info}")"
info "Drynet3 node verification passed (blocks=${blocks}, headers=${headers})"
