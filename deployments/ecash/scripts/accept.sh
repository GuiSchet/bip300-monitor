#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in chmod date git jq mktemp mv rm; do
    require_command "${command_name}"
done

repository_root="$(git -C "${DEPLOYMENT_ROOT}" rev-parse --show-toplevel)"
[[ -z "$(git -C "${repository_root}" status --porcelain)" ]] ||
    die "refusing to accept a deployment from a dirty repository"
repository_commit="$(git -C "${repository_root}" rev-parse HEAD)"
[[ "${repository_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not resolve the deployed repository commit"

resolved_data_root="$(data_root)"
marker_path="${resolved_data_root}/.deployment-accepted"
live_result="$(mktemp "${resolved_data_root}/.verified-live.XXXXXX")"
marker_tmp="$(mktemp "${resolved_data_root}/.deployment-accepted.XXXXXX")"
trap 'rm -f -- "${live_result}" "${marker_tmp}"' EXIT

"${DEPLOYMENT_ROOT}/scripts/verify-live.sh" "${live_result}"

verified_live_height="$(jq -er '.height' "${live_result}")"
verified_live_hash="$(jq -er '.hash' "${live_result}")"
history_slots="$(jq -er '.slots | map(tostring) | join(",")' "${live_result}")"
[[ "${verified_live_height}" =~ ^[0-9]+$ ]] ||
    die "live verification returned an invalid height"
[[ "${verified_live_hash}" =~ ^[[:xdigit:]]{64}$ ]] ||
    die "live verification returned an invalid block hash"
record_current_run_has_bmm_observation ||
    die "the current extractor run has no successful BMM request observation"
enforcer_mempool_tracking_is_enabled ||
    die "the enforcer is not running the validator mempool synchronization task"
record_current_workers_are_healthy ||
    die "the current extractor run has an unhealthy or uninitialized worker"
event_contract_version="$(record_current_event_contract_version)"
[[ "${event_contract_version}" == "${MONITOR_EVENT_CONTRACT_VERSION}" ]] ||
    die "the current extractor uses event contract v${event_contract_version:-unknown}; Betanet requires v${MONITOR_EVENT_CONTRACT_VERSION}"
run_capabilities="$(record_current_run_capabilities)"
jq -e 'index("live_bmm_bid_snapshots") != null' <<<"${run_capabilities}" >/dev/null ||
    die "the active extractor run does not declare live_bmm_bid_snapshots"
jq -e 'index("mempool_backed_bmm_bid_snapshots") != null' <<<"${run_capabilities}" >/dev/null ||
    die "the active extractor run does not declare mempool_backed_bmm_bid_snapshots"
if ((MONITOR_EVENT_CONTRACT_VERSION >= 6)); then
    jq -e '
        index("bip300_description_hash_identity") != null
        and index("stable_parent_bmm_snapshots") != null
        and index("validated_chain_identity") != null
        and index("orphan_run_reconciliation") != null
    ' <<<"${run_capabilities}" >/dev/null ||
        die "the active extractor run does not declare the contract-v6 correctness capabilities"
    record_current_run_has_stable_bmm_observation ||
        die "the current extractor run has no stable-parent BMM observation"
fi
record_has_single_running_enforcer ||
    die "the current dataset does not have exactly one running enforcer extractor"
dataset_created_at="$(record_current_dataset_created_at)"
chainstates="$(node_cli getchainstates)" || die "could not read node chainstates"
history_fully_validated=false
if node_history_is_fully_validated "${chainstates}"; then
    history_fully_validated=true
fi
{
    printf 'accepted_at=%s\n' "$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    printf 'network_id=%s\n' "${NETWORK_ID}"
    printf 'repository_commit=%s\n' "${repository_commit}"
    printf 'node_commit=%s\n' "${ECASH_NODE_COMMIT}"
    printf 'node_image=%s\n' "${ECASH_NODE_IMAGE}"
    printf 'enforcer_base_commit=%s\n' "${ENFORCER_BASE_COMMIT}"
    printf 'enforcer_upstream_reviewed_commit=%s\n' "${ENFORCER_UPSTREAM_REVIEWED_COMMIT}"
    printf 'enforcer_observer_commit=%s\n' "${ENFORCER_OBSERVER_COMMIT}"
    printf 'enforcer_commit=%s\n' "${ENFORCER_COMMIT}"
    printf 'enforcer_image=%s\n' "${ENFORCER_IMAGE}"
    printf 'monitor_image_commit=%s\n' "${MONITOR_IMAGE_COMMIT}"
    printf 'event_contract_version=%s\n' "${event_contract_version}"
    printf 'dataset_created_at=%s\n' "${dataset_created_at}"
    printf 'extractor_run_capabilities=%s\n' "$(jq -c . <<<"${run_capabilities}")"
    printf 'bmm_request_polling_verified=true\n'
    printf 'bmm_mempool_tracking_enabled=true\n'
    if ((MONITOR_EVENT_CONTRACT_VERSION >= 6)); then
        printf 'bmm_stable_parent_verified=true\n'
    fi
    printf 'confirmed_bmm_fee_available=false\n'
    printf 'snapshot_trust_enabled=%s\n' "${TRUST_ASSUMEUTXO_SNAPSHOT}"
    printf 'snapshot_transform=%s\n' "${ECASH_SNAPSHOT_TRANSFORM}"
    printf 'snapshot_height=%s\n' "${ECASH_SNAPSHOT_HEIGHT}"
    printf 'snapshot_block_hash=%s\n' "${ECASH_SNAPSHOT_BLOCK_HASH}"
    printf 'snapshot_utxo_hash=%s\n' "${ECASH_SNAPSHOT_UTXO_HASH}"
    printf 'snapshot_file_sha256=%s\n' "${ECASH_SNAPSHOT_SHA256}"
    if [[ "${ECASH_SNAPSHOT_TRANSFORM}" == network_magic_v2 ]]; then
        printf 'snapshot_source_url=%s\n' "${ECASH_SNAPSHOT_URL}"
        printf 'snapshot_source_mirror_url=%s\n' "${ECASH_SNAPSHOT_MIRROR_URL}"
        printf 'snapshot_source_file=%s\n' "${ECASH_SNAPSHOT_SOURCE_FILE}"
        printf 'snapshot_source_size=%s\n' "${ECASH_SNAPSHOT_SOURCE_SIZE}"
        printf 'snapshot_source_sha256=%s\n' "${ECASH_SNAPSHOT_SOURCE_SHA256}"
        printf 'snapshot_source_network_magic=%s\n' "${ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC}"
    fi
    printf 'history_fully_validated=%s\n' "${history_fully_validated}"
    printf 'block_history_complete=true\n'
    # The current upstream enforcer cannot expose historical mutable-state
    # snapshots. Keep that distinction explicit until its Phase 2 API lands.
    printf 'state_history_complete=false\n'
    printf 'history_slots=%s\n' "${history_slots}"
    printf 'verified_live_height=%s\n' "${verified_live_height}"
    printf 'verified_live_hash=%s\n' "${verified_live_hash}"
} >"${marker_tmp}"
chmod 0644 "${marker_tmp}"
mv -- "${marker_tmp}" "${marker_path}"
rm -f -- "${live_result}"
trap - EXIT

info "deployment accepted at ${marker_path}"
