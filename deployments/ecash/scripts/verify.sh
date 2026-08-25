#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose config --quiet
"${DEPLOYMENT_ROOT}/scripts/verify-core.sh"

for service in nats event-logger enforcer-extractor; do
    require_service_running "${service}"
done

wait_for_nats_health
wait_for_event_logger_subscription
wait_for_nats_client bip300-monitor-enforcer-extractor

configured_sidechains="${BIP300_MONITOR_SIDECHAINS:-9,98}"
IFS=',' read -r -a sidechains <<<"${configured_sidechains}"
((${#sidechains[@]} > 0)) || die "BIP300_MONITOR_SIDECHAINS must not be empty"
for sidechain in "${sidechains[@]}"; do
    [[ "${sidechain}" =~ ^[0-9]+$ ]] ||
        die "invalid configured sidechain slot: ${sidechain}"
    ((10#${sidechain} <= 255)) ||
        die "sidechain slot must fit in a u8: ${sidechain}"
done

wait_seconds="${MONITOR_EVENT_WAIT_SECONDS:-60}"
deadline="$((SECONDS + wait_seconds))"
while :; do
    extractor_started_before="$(service_started_at enforcer-extractor)"
    logger_started_before="$(service_started_at event-logger)"
    verification_started_at="$(
        latest_timestamp "${extractor_started_before}" "${logger_started_before}"
    )"
    extractor_logs="$(
        compose logs --no-color --since "${verification_started_at}" \
            enforcer-extractor 2>/dev/null || true
    )"
    logger_logs="$(
        compose logs --no-color --since "${verification_started_at}" \
            event-logger 2>/dev/null || true
    )"
    extractor_started_after="$(service_started_at enforcer-extractor)"
    logger_started_after="$(service_started_at event-logger)"
    if [[ "${extractor_started_before}" != "${extractor_started_after}" ||
        "${logger_started_before}" != "${logger_started_after}" ]]; then
        info "monitor container restarted during snapshot verification; resetting the log window"
        sleep 2
        continue
    fi

    snapshot_complete=true
    if ! logs_contain_snapshot_completion \
        "${extractor_logs}" "${#sidechains[@]}"; then
        snapshot_complete=false
    fi
    for event_kind in chain_info chain_tip sidechain_proposals active_sidechains; do
        if [[ "$(count_snapshot_events "${logger_logs}" "${event_kind}")" != 1 ]]; then
            snapshot_complete=false
            break
        fi
    done
    if [[ "${snapshot_complete}" == true ]]; then
        for sidechain in "${sidechains[@]}"; do
            if [[ "$(count_snapshot_events "${logger_logs}" ctip "${sidechain}")" != 1 ]]; then
                snapshot_complete=false
                break
            fi
        done
    fi

    if [[ "${snapshot_complete}" == true ]]; then
        break
    fi
    ((SECONDS < deadline)) ||
        die "current monitor instances did not deliver one complete semantic snapshot after ${wait_seconds}s"
    sleep 2
done

info "${NETWORK_ID} observation pipeline verification passed (fresh semantic snapshot, slots=${configured_sidechains})"
