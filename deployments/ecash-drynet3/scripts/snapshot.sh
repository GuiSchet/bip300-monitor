#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in curl jq sha256sum stat; do
    require_command "${command_name}"
done

resolved_data_root="$(data_root)"
snapshot_dir="${resolved_data_root}/snapshots"
snapshot_path="${snapshot_dir}/${DRYNET_SNAPSHOT_FILE}"
partial_path="${snapshot_path}.part"
mkdir -p -- "${snapshot_dir}"

verify_snapshot() {
    local path="$1"
    local actual_size
    actual_size="$(stat --format='%s' "${path}")"
    [[ "${actual_size}" == "${DRYNET_SNAPSHOT_SIZE}" ]] ||
        die "snapshot size is ${actual_size}, expected ${DRYNET_SNAPSHOT_SIZE}"
    printf '%s  %s\n' "${DRYNET_SNAPSHOT_SHA256}" "${path}" | sha256sum --check
}

if [[ -f "${snapshot_path}" ]]; then
    info "verifying existing snapshot"
    verify_snapshot "${snapshot_path}"
else
    info "downloading the pinned Drynet3 snapshot (resume is enabled)"
    curl --fail --location --show-error \
        --continue-at - \
        --output "${partial_path}" \
        "${DRYNET_SNAPSHOT_URL}"
    verify_snapshot "${partial_path}"
    mv -- "${partial_path}" "${snapshot_path}"
fi

compose ps --status running --quiet ecash-node | grep -q . ||
    die "ecash-node is not running; run 'just up' first"

if node_cli getchainstates |
    jq -e '.chainstates | any(has("snapshot_blockhash"))' >/dev/null; then
    info "an AssumeUTXO snapshot chainstate is already active"
    exit 0
fi

wait_seconds="${SNAPSHOT_HEADER_WAIT_SECONDS:-1800}"
deadline="$((SECONDS + wait_seconds))"
info "waiting for header ${DRYNET_ACTIVATION_HEIGHT}"
until node_cli getblockhash "${DRYNET_ACTIVATION_HEIGHT}" >/dev/null 2>&1; do
    ((SECONDS < deadline)) ||
        die "header ${DRYNET_ACTIVATION_HEIGHT} was not available after ${wait_seconds}s"
    sleep 10
done
require_drynet_activation_block

info "loading the verified AssumeUTXO snapshot"
node_cli -rpcclienttimeout=0 loadtxoutset "/snapshots/${DRYNET_SNAPSHOT_FILE}"
node_cli getchainstates | jq '{headers, chainstates}'
