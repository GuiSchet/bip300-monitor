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

declare -a active_sidechains=()
declare -A activation_heights=()
active_activations="$(active_sidechain_activations)"
while read -r sidechain activation_height; do
    [[ -n "${sidechain}" ]] || continue
    [[ "${sidechain}" =~ ^[0-9]+$ && "${activation_height}" =~ ^[0-9]+$ ]] ||
        die "enforcer returned an invalid active sidechain"
    ((10#${sidechain} <= 255)) || die "sidechain slot must fit in a u8: ${sidechain}"
    active_sidechains+=("${sidechain}")
    activation_heights["${sidechain}"]="${activation_height}"
done <<<"${active_activations}"
observed_sidechains="$(
    IFS=,
    printf '%s' "${active_sidechains[*]}"
)"

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

    # The record is the authoritative check, asked about the block the snapshot
    # is anchored to rather than about a time window. Recording is idempotent, so
    # a restart at an unchanged tip inserts nothing, and a window would read that
    # healthy state as a missing snapshot. The block is the stricter scope
    # anyway, because a previous run's rows are at a previous block. Which
    # instance published is a separate question, already answered above by the
    # log window.
    snapshot_anchor=''
    if [[ "${snapshot_complete}" == true ]]; then
        snapshot_anchor="$(record_snapshot_anchor)" || snapshot_anchor=''
        if [[ ! "${snapshot_anchor}" =~ ^[[:xdigit:]]{64}$ ]]; then
            snapshot_complete=false
        fi
    fi
    # Exactly one, not at least one: a second row at the same block would mean
    # the identity constraint stopped collapsing a republished snapshot, which is
    # the shape of unbounded growth rather than of a missing event.
    if [[ "${snapshot_complete}" == true ]]; then
        for event_kind in chain_info chain_tip sidechain_proposals active_sidechains; do
            if [[ "$(record_event_count_at "${event_kind}" "${snapshot_anchor}")" != 1 ]]; then
                snapshot_complete=false
                break
            fi
        done
    fi
    declare -a snapshot_sidechains=()
    if [[ "${snapshot_complete}" == true ]]; then
        snapshot_activations="$(
            record_snapshot_sidechain_activations "${snapshot_anchor}"
        )" || snapshot_complete=false
        if [[ "${snapshot_complete}" == true ]]; then
            while read -r sidechain activation_height; do
                [[ -n "${sidechain}" ]] || continue
                if [[ ! "${sidechain}" =~ ^[0-9]+$ ||
                    ! "${activation_height}" =~ ^[0-9]+$ ]] ||
                    ((10#${sidechain} > 255)); then
                    snapshot_complete=false
                    break
                fi
                snapshot_sidechains+=("${sidechain}")
            done <<<"${snapshot_activations}"
        fi
    fi
    if [[ "${snapshot_complete}" == true ]] &&
        ! logs_contain_snapshot_completion \
            "${extractor_logs}" "${#snapshot_sidechains[@]}"; then
        snapshot_complete=false
    fi
    if [[ "${snapshot_complete}" == true ]]; then
        for sidechain in "${snapshot_sidechains[@]}"; do
            if [[ "$(record_event_count_at ctip "${snapshot_anchor}" "${sidechain}")" != 1 ]] ||
                [[ "$(record_event_count_at withdrawal_bundle_proposals \
                    "${snapshot_anchor}" "${sidechain}")" != 1 ]]; then
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

snapshot_height="$(record_snapshot_height)"
[[ "${snapshot_height}" =~ ^[0-9]+$ ]] ||
    die "the recorded semantic snapshot has no valid height"
history_wait_seconds="${HISTORY_WAIT_SECONDS:-43200}"
history_deadline="$((SECONDS + history_wait_seconds))"
while :; do
    history_complete=true
    for sidechain in "${active_sidechains[@]}"; do
        if ! record_block_history_is_complete \
            "${sidechain}" "${activation_heights[${sidechain}]}" "${snapshot_height}"; then
            history_complete=false
            break
        fi
    done
    if [[ "${history_complete}" == true ]]; then
        break
    fi
    if ((SECONDS >= history_deadline)); then
        coverage="$(history_coverage_json 2>/dev/null || printf 'unavailable')"
        die "complete block history was not recorded after ${history_wait_seconds}s; coverage=${coverage}"
    fi
    sleep 5
done

final_active_activations="$(active_sidechain_activations)"
[[ "${final_active_activations}" == "${active_activations}" ]] ||
    die "the active sidechain set changed during verification; run 'just verify' again so the new slot is included"

info "${NETWORK_ID} observation pipeline verification passed (snapshot live, block history complete, slots=${observed_sidechains:-none})"
