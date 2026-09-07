#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in docker jq stat; do
    require_command "${command_name}"
done

compose config --quiet
require_node_ready
require_node_monitoring_ready

blockchain_info="$(node_cli getblockchaininfo)"

config_json="$(compose config --format json)"
jq -e '[.services[]?.ports[]?] | length == 0' <<<"${config_json}" >/dev/null ||
    die "the deployment unexpectedly publishes a host port"

require_rpc_cookie_readable
require_service_running enforcer
chain_info="$(enforcer_rpc GetChainInfo)" || die "enforcer RPC is not ready"
jq -e --arg network "${ENFORCER_API_NETWORK}" '.network == $network' \
    <<<"${chain_info}" >/dev/null ||
    die "enforcer reported an unexpected network"
jq -e --argjson height "${ECASH_ACTIVATION_HEIGHT}" \
    '.bip300Constants.activationHeight == $height' \
    <<<"${chain_info}" >/dev/null ||
    die "enforcer is not using the ${NETWORK_ID} activation height"

wait_seconds="${ENFORCER_SYNC_WAIT_SECONDS:-300}"
deadline="$((SECONDS + wait_seconds))"
enforcer_height="unavailable"
enforcer_hash="unavailable"
info "waiting up to ${wait_seconds}s for the enforcer to reach the node tip"
while ((SECONDS < deadline)); do
    blockchain_info="$(node_cli getblockchaininfo)"
    node_height="$(jq -er '.blocks' <<<"${blockchain_info}")"
    node_hash="$(node_cli getblockhash "${node_height}")"

    if chain_tip="$(enforcer_rpc GetChainTip 2>/dev/null)" &&
        enforcer_height="$(jq -er '.blockHeaderInfo.height' <<<"${chain_tip}")" &&
        enforcer_hash="$(jq -er '.blockHeaderInfo.blockHash.hex' <<<"${chain_tip}")" &&
        [[ "${enforcer_height}" == "${node_height}" ]] &&
        [[ "${enforcer_hash}" == "${node_hash}" ]]; then
        headers="$(jq -r '.headers' <<<"${blockchain_info}")"
        info "${NETWORK_ID} node and enforcer verification passed (blocks=${node_height}, headers=${headers})"
        exit 0
    fi
    sleep 5
done

blocks="$(jq -r '.blocks' <<<"${blockchain_info}")"
die "enforcer did not reach node tip ${blocks} within ${wait_seconds}s (enforcer height=${enforcer_height}, hash=${enforcer_hash})"
