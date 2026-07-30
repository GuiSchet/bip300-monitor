#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in date docker jq; do
    require_command "${command_name}"
done

"${DEPLOYMENT_ROOT}/scripts/verify.sh"

block_wait_seconds="${LIVE_BLOCK_WAIT_SECONDS:-3600}"
event_wait_seconds="${LIVE_EVENT_WAIT_SECONDS:-60}"
started_at="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
blockchain_info="$(node_cli getblockchaininfo)"
baseline_height="$(jq -er '.blocks' <<<"${blockchain_info}")"
baseline_hash="$(node_cli getblockhash "${baseline_height}")"
block_deadline="$((SECONDS + block_wait_seconds))"

info "waiting up to ${block_wait_seconds}s for a new Drynet3 block after ${baseline_height}"
while :; do
    blockchain_info="$(node_cli getblockchaininfo)"
    live_height="$(jq -er '.blocks' <<<"${blockchain_info}")"
    live_hash="$(node_cli getblockhash "${live_height}")"
    if [[ "${live_hash}" != "${baseline_hash}" ]]; then
        break
    fi
    ((SECONDS < block_deadline)) ||
        die "no new Drynet3 block arrived within ${block_wait_seconds}s; live delivery was not exercised"
    sleep 10
done

configured_sidechains="${BIP300_MONITOR_SIDECHAINS:-9,98}"
IFS=',' read -r -a sidechains <<<"${configured_sidechains}"
((${#sidechains[@]} > 0)) || die "BIP300_MONITOR_SIDECHAINS must not be empty"
for sidechain in "${sidechains[@]}"; do
    [[ "${sidechain}" =~ ^[0-9]+$ ]] ||
        die "invalid configured sidechain slot: ${sidechain}"
    ((10#${sidechain} <= 255)) || die "sidechain slot must fit in a u8: ${sidechain}"
done

event_deadline="$((SECONDS + event_wait_seconds))"
while :; do
    extractor_logs="$(
        compose logs --no-color --since "${started_at}" enforcer-extractor 2>/dev/null || true
    )"
    logger_logs="$(
        compose logs --no-color --since "${started_at}" event-logger 2>/dev/null || true
    )"
    all_delivered=true
    for sidechain in "${sidechains[@]}"; do
        if ! logs_contain_live_event \
            "${extractor_logs}" "published live enforcer event" \
            "${sidechain}" "${live_hash}"; then
            all_delivered=false
            break
        fi
        if ! logs_contain_live_event \
            "${logger_logs}" "received enforcer event" \
            "${sidechain}" "${live_hash}"; then
            all_delivered=false
            break
        fi
    done

    if [[ "${all_delivered}" == true ]]; then
        info "live Drynet3 event delivery passed (height=${live_height}, hash=${live_hash}, slots=${configured_sidechains})"
        exit 0
    fi
    ((SECONDS < event_deadline)) ||
        die "block ${live_hash} reached the node but was not delivered for every configured slot within ${event_wait_seconds}s"
    sleep 2
done
