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

for service in postgres nats event-logger enforcer-extractor; do
    require_service_running "${service}"
done

wait_for_postgres_health
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
    # Rows outlive a container, so the record is only asked about this instance.
    record_since="${extractor_started_before}"
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

    # The record is the authoritative check. The BIP300 constants and the
    # startup tip are recorded exactly once per instance, so a second one would
    # mean an unnoticed republish; every other kind is re-recorded whenever a
    # block changes it, so more than one is expected.
    if [[ "${snapshot_complete}" == true ]]; then
        for event_kind in chain_info chain_tip; do
            if [[ "$(record_event_count "${event_kind}" "${record_since}")" != 1 ]]; then
                snapshot_complete=false
                break
            fi
        done
    fi
    if [[ "${snapshot_complete}" == true ]]; then
        for event_kind in sidechain_proposals active_sidechains; do
            if ! record_has_event "${event_kind}" "${record_since}"; then
                snapshot_complete=false
                break
            fi
        done
    fi
    if [[ "${snapshot_complete}" == true ]]; then
        for sidechain in "${sidechains[@]}"; do
            if ! record_has_event ctip "${record_since}" "${sidechain}" ||
                ! record_has_event \
                    withdrawal_bundle_proposals "${record_since}" "${sidechain}"; then
                snapshot_complete=false
                break
            fi
        done
    fi

    # Independently, the logger proves that live fan-out reached a consumer.
    # It is a separate path from the record, so it gets a separate assertion
    # rather than being folded into the one above.
    if [[ "${snapshot_complete}" == true ]]; then
        for event_kind in chain_info active_sidechains; do
            if ! has_snapshot_event "${logger_logs}" "${event_kind}"; then
                snapshot_complete=false
                break
            fi
        done
    fi

    if [[ "${snapshot_complete}" == true ]]; then
        break
    fi
    if ((SECONDS >= deadline)); then
        # Only the extractor publishes a snapshot, and only when it starts. If
        # the logger came up afterwards, that snapshot is outside this window
        # and Core NATS has no JetStream to replay it from.
        newest_monitor="$(
            latest_timestamp "${extractor_started_before}" "${logger_started_before}"
        )"
        if [[ "${newest_monitor}" == "${logger_started_before}" &&
            "${logger_started_before}" != "${extractor_started_before}" ]]; then
            die "event-logger started after enforcer-extractor, so it never received the initial snapshot and Core NATS cannot replay it; restart the enforcer-extractor container to republish, then run 'just verify' again"
        fi
        die "current monitor instances did not record one complete semantic snapshot after ${wait_seconds}s"
    fi
    sleep 2
done

info "${NETWORK_ID} observation pipeline verification passed (fresh semantic snapshot recorded and fanned out, slots=${configured_sidechains})"
