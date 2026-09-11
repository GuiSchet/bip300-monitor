#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in chmod date dirname docker jq mktemp mv rm; do
    require_command "${command_name}"
done
(($# <= 1)) || die "usage: verify-live.sh [result-file]"
result_file="${1:-}"
result_tmp=""
trap '[[ -z "${result_tmp}" ]] || rm -f -- "${result_tmp}"' EXIT

"${DEPLOYMENT_ROOT}/scripts/verify.sh"

declare -a sidechains=()
active_activations="$(active_sidechain_activations)"
while read -r sidechain _activation_height; do
    [[ -n "${sidechain}" ]] || continue
    [[ "${sidechain}" =~ ^[0-9]+$ ]] || die "enforcer returned an invalid sidechain slot"
    ((10#${sidechain} <= 255)) || die "sidechain slot must fit in a u8: ${sidechain}"
    sidechains+=("${sidechain}")
done <<<"${active_activations}"
observed_sidechains="$(
    IFS=,
    printf '%s' "${sidechains[*]}"
)"

block_wait_seconds="${LIVE_BLOCK_WAIT_SECONDS:-3600}"
event_wait_seconds="${LIVE_EVENT_WAIT_SECONDS:-60}"
started_at="$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
blockchain_info="$(node_cli getblockchaininfo)"
baseline_height="$(jq -er '.blocks' <<<"${blockchain_info}")"
baseline_hash="$(node_cli getblockhash "${baseline_height}")"
block_deadline="$((SECONDS + block_wait_seconds))"

info "waiting up to ${block_wait_seconds}s for a new ${NETWORK_ID} block after ${baseline_height}"
while :; do
    blockchain_info="$(node_cli getblockchaininfo)"
    live_height="$(jq -er '.blocks' <<<"${blockchain_info}")"
    live_hash="$(node_cli getblockhash "${live_height}")"
    if [[ "${live_hash}" != "${baseline_hash}" ]]; then
        break
    fi
    ((SECONDS < block_deadline)) ||
        die "no new ${NETWORK_ID} block arrived within ${block_wait_seconds}s; live delivery was not exercised"
    sleep 10
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
        # The record is what has to hold the block. The two log assertions
        # additionally prove the live fan-out path still reaches a consumer.
        if ! record_has_block block_connected "${sidechain}" "${live_hash}"; then
            all_delivered=false
            break
        fi
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
        final_active_activations="$(active_sidechain_activations)"
        [[ "${final_active_activations}" == "${active_activations}" ]] ||
            die "the active sidechain set changed during live verification; run 'just verify-live' again so the new slot is included"
        if [[ -n "${result_file}" ]]; then
            result_directory="$(dirname -- "${result_file}")"
            [[ -d "${result_directory}" ]] ||
                die "live verification result directory does not exist: ${result_directory}"
            result_tmp="$(mktemp "${result_directory}/.verified-live.XXXXXX")"
            slots_json="$(
                printf '%s\n' "${sidechains[@]}" |
                    jq -R -s 'split("\n") | map(select(length > 0) | tonumber)'
            )"
            jq -n \
                --argjson height "${live_height}" \
                --arg hash "${live_hash}" \
                --argjson slots "${slots_json}" \
                '{height: $height, hash: $hash, slots: $slots}' >"${result_tmp}"
            chmod 0600 "${result_tmp}"
            mv -- "${result_tmp}" "${result_file}"
            result_tmp=""
        fi
        info "live ${NETWORK_ID} event delivery passed (height=${live_height}, hash=${live_hash}, slots=${observed_sidechains:-none})"
        exit 0
    fi
    ((SECONDS < event_deadline)) ||
        die "block ${live_hash} reached the node but was not recorded and delivered for every configured slot within ${event_wait_seconds}s"
    sleep 2
done
