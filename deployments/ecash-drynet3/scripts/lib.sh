#!/usr/bin/env bash

set -euo pipefail

DEPLOYMENT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DEPLOYMENT_ROOT
readonly VERSIONS_FILE="${DEPLOYMENT_ROOT}/VERSIONS.lock"

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

info() {
    printf '==> %s\n' "$*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

load_versions() {
    [[ -f "${VERSIONS_FILE}" ]] || die "missing ${VERSIONS_FILE}"
    # This file is tracked in the repository and contains assignments only.
    # shellcheck disable=SC1090
    source "${VERSIONS_FILE}"
}

deployment_env_file() {
    printf '%s\n' "${COMPOSE_ENV_FILE:-${DEPLOYMENT_ROOT}/.env}"
}

load_deployment_env() {
    local env_file
    env_file="$(deployment_env_file)"
    [[ -f "${env_file}" ]] || die "missing ${env_file}; run 'just init' first"

    set -a
    # The local file is created from the repository's simple KEY=VALUE example.
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a

    : "${ECASH_DATA_ROOT:?ECASH_DATA_ROOT must be set}"
    : "${PUID:?PUID must be set}"
    : "${PGID:?PGID must be set}"
    [[ "${PUID}" =~ ^[0-9]+$ ]] || die "PUID must be numeric"
    [[ "${PGID}" =~ ^[0-9]+$ ]] || die "PGID must be numeric"
    [[ "${ENFORCER_SYNC_WAIT_SECONDS:-300}" =~ ^[0-9]+$ ]] ||
        die "ENFORCER_SYNC_WAIT_SECONDS must be numeric"
    (("${ENFORCER_SYNC_WAIT_SECONDS:-300}" > 0)) ||
        die "ENFORCER_SYNC_WAIT_SECONDS must be greater than zero"
}

data_root() {
    if [[ "${ECASH_DATA_ROOT}" == /* ]]; then
        realpath -m -- "${ECASH_DATA_ROOT}"
    else
        realpath -m -- "${DEPLOYMENT_ROOT}/${ECASH_DATA_ROOT}"
    fi
}

compose() {
    local env_file
    env_file="$(deployment_env_file)"
    docker compose \
        --env-file "${VERSIONS_FILE}" \
        --env-file "${env_file}" \
        --file "${DEPLOYMENT_ROOT}/compose.yaml" \
        "$@"
}

node_cli() {
    compose exec -T ecash-node \
        bitcoin-cli \
        -datadir=/data \
        -conf=/etc/ecash/drivechain-ecash.conf \
        "$@"
}

service_is_running() {
    compose ps --status running --quiet "$1" | grep -q .
}

require_service_running() {
    service_is_running "$1" || die "$1 is not running"
}

enforcer_rpc() {
    local method="$1"
    compose exec -T enforcer \
        curl --fail-with-body --silent --show-error --max-time 30 \
        --request POST \
        --header 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:50051/cusf.mainchain.v1.ValidatorService/${method}"
}

require_drynet_activation_block() {
    local activation_hash
    activation_hash="$(node_cli getblockhash "${DRYNET_ACTIVATION_HEIGHT}")" ||
        die "Drynet3 activation block is unavailable"
    [[ "${activation_hash}" == "${DRYNET_ACTIVATION_BLOCK_HASH}" ]] ||
        die "unexpected block at height ${DRYNET_ACTIVATION_HEIGHT}: ${activation_hash}"
}

require_node_ready() {
    local blockchain_info
    require_service_running ecash-node
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
}
