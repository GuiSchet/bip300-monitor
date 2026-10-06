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

    # Verify the current run observation group rather than event.height.
    snapshot_group=''
    if [[ "${snapshot_complete}" == true ]]; then
        snapshot_group="$(record_snapshot_group)" || snapshot_group=''
        if [[ ! "${snapshot_group}" =~ ^[[:xdigit:]-]{36}$ ]]; then
            snapshot_complete=false
        fi
    fi
    if [[ "${snapshot_complete}" == true ]]; then
        for event_kind in sidechain_proposals active_sidechains; do
            if ! record_current_state_is_usable "${event_kind}"; then
                snapshot_complete=false
                break
            fi
        done
    fi
    declare -a snapshot_sidechains=()
    if [[ "${snapshot_complete}" == true ]]; then
        snapshot_activations="$(
            record_snapshot_sidechain_activations "${snapshot_group}"
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
            if ! record_current_state_is_usable ctip "${sidechain}" ||
                ! record_current_state_is_usable withdrawal_bundle_proposals "${sidechain}"; then
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
        die "current monitor instances did not record one complete observation window after ${wait_seconds}s"
    fi
    sleep 2
done

bmm_wait_seconds="${BMM_REQUEST_WAIT_SECONDS:-30}"
bmm_deadline="$((SECONDS + bmm_wait_seconds))"
until record_current_run_has_bmm_observation; do
    ((SECONDS < bmm_deadline)) ||
        die "the current extractor run did not record a successful BMM request poll after ${bmm_wait_seconds}s"
    sleep 1
done
event_contract_version="$(record_current_event_contract_version)"
[[ "${event_contract_version}" == "${MONITOR_EVENT_CONTRACT_VERSION}" ]] ||
    die "the current extractor uses event contract v${event_contract_version:-unknown}; Betanet requires v${MONITOR_EVENT_CONTRACT_VERSION}"
run_capabilities="$(record_current_run_capabilities)"
jq -e '
    index("live_bmm_bid_snapshots") != null
    and index("bmm_readiness_unknown") != null
    and index("official_enforcer_api") != null
    and index("node_block_evidence") != null
    and index("per_worker_health") != null
' <<<"${run_capabilities}" >/dev/null ||
    die "the current extractor run does not declare observed BMM and per-worker health"
if ((MONITOR_EVENT_CONTRACT_VERSION >= 6)); then
    jq -e '
        index("bip300_description_hash_identity") != null
        and index("tip_matched_snapshots") != null
        and index("validated_chain_identity") != null
        and index("orphan_run_reconciliation") != null
    ' <<<"${run_capabilities}" >/dev/null ||
        die "the current extractor run does not declare the contract-v6 correctness capabilities"
    record_current_run_has_tip_matched_bmm_observation ||
        die "the current extractor run has no tip-matched BMM observation"
fi
record_has_single_running_enforcer ||
    die "the current dataset does not have exactly one running enforcer extractor"
worker_wait_seconds="${WORKER_HEALTH_WAIT_SECONDS:-60}"
worker_deadline="$((SECONDS + worker_wait_seconds))"
until record_current_workers_are_healthy && record_node_worker_is_healthy; do
    ((SECONDS < worker_deadline)) || {
        worker_status="$(record_current_worker_status_json 2>/dev/null || printf 'unavailable')"
        die "extractor workers were not healthy after ${worker_wait_seconds}s; status=${worker_status}"
    }
    sleep 1
done

snapshot_height="$(record_snapshot_height "${snapshot_group}")"
[[ "${snapshot_height}" =~ ^[0-9]+$ ]] ||
    die "the recorded observation window has no valid height"
history_wait_seconds="${HISTORY_WAIT_SECONDS:-43200}"
history_deadline="$((SECONDS + history_wait_seconds))"
while :; do
    history_complete=true
    if ! record_node_history_is_complete \
        "${ECASH_ACTIVATION_HEIGHT}" "${snapshot_height}"; then
        history_complete=false
    fi
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

info "${NETWORK_ID} observation pipeline verification passed (contract=v${event_contract_version}, observed BMM polling live, workers healthy, snapshot live, node block and official slot histories complete, slots=${observed_sidechains:-none})"
