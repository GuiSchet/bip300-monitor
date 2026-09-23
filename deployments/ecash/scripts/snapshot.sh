#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in cmp cp curl dd jq mv od rm sha256sum stat tr; do
    require_command "${command_name}"
done

resolved_data_root="$(data_root)"
snapshot_dir="${resolved_data_root}/snapshots"
snapshot_path="${snapshot_dir}/${ECASH_SNAPSHOT_FILE}"
partial_path="${snapshot_path}.part"
mkdir -p -- "${snapshot_dir}"
source_path=''
source_partial_path=''
if [[ "${ECASH_SNAPSHOT_TRANSFORM}" == network_magic_v2 ]]; then
    source_path="${snapshot_dir}/${ECASH_SNAPSHOT_SOURCE_FILE}"
    source_partial_path="${source_path}.part"
fi

verify_file() {
    local path="$1"
    local expected_size="$2"
    local expected_sha256="$3"
    local description="$4"
    local actual_size

    actual_size="$(stat --format='%s' "${path}")"
    if [[ "${actual_size}" != "${expected_size}" ]]; then
        printf '%s size is %s, expected %s\n' \
            "${description}" "${actual_size}" "${expected_size}" >&2
        return 1
    fi
    printf '%s  %s\n' "${expected_sha256}" "${path}" | sha256sum --check
}

verify_snapshot() {
    verify_file \
        "$1" \
        "${ECASH_SNAPSHOT_SIZE}" \
        "${ECASH_SNAPSHOT_SHA256}" \
        snapshot
}

download_verified_file() {
    local destination="$1"
    local partial="$2"
    local expected_size="$3"
    local expected_sha256="$4"
    local description="$5"
    local primary_url="$6"
    local mirror_url="${7:-}"
    local partial_size

    if [[ -f "${destination}" ]]; then
        info "verifying existing ${description}"
        verify_file \
            "${destination}" "${expected_size}" "${expected_sha256}" "${description}" ||
            die "existing ${description} failed verification"
        return 0
    fi
    if [[ -f "${partial}" ]]; then
        partial_size="$(stat --format='%s' "${partial}")"
        if ((partial_size > expected_size)); then
            info "discarding oversized partial ${description} (${partial_size} bytes)"
            rm -f -- "${partial}"
        elif ((partial_size == expected_size)); then
            info "verifying complete partial ${description} before downloading"
            if verify_file \
                "${partial}" "${expected_size}" "${expected_sha256}" "${description}"; then
                mv -- "${partial}" "${destination}"
                return 0
            fi
            info "discarding corrupt complete partial ${description}"
            rm -f -- "${partial}"
        else
            info "resuming partial ${description} at ${partial_size} bytes"
        fi
    fi

    info "downloading the pinned ${description} (resume is enabled)"
    if ! curl --fail --location --show-error --retry 3 \
        --continue-at - \
        --output "${partial}" \
        "${primary_url}"; then
        [[ -n "${mirror_url}" ]] || die "could not download ${description}"
        info "primary download failed; resuming from the pinned mirror"
        curl --fail --location --show-error --retry 3 \
            --continue-at - \
            --output "${partial}" \
            "${mirror_url}" || die "could not download ${description} from either source"
    fi
    if ! verify_file \
        "${partial}" "${expected_size}" "${expected_sha256}" "${description}"; then
        partial_size="$(stat --format='%s' "${partial}")"
        if ((partial_size >= expected_size)); then
            info "discarding non-resumable invalid partial ${description}"
            rm -f -- "${partial}"
        else
            info "retaining the partial ${description} so the next run can resume it"
        fi
        die "downloaded ${description} failed verification"
    fi
    mv -- "${partial}" "${destination}"
}

if [[ -f "${snapshot_path}" ]]; then
    info "verifying existing snapshot"
    verify_snapshot "${snapshot_path}" || die "existing snapshot failed verification"
elif [[ "${ECASH_SNAPSHOT_TRANSFORM}" == network_magic_v2 ]]; then
    # A transformation is deterministic but not resumable. Keep a verified
    # source instead, and always rebuild an incomplete/invalid destination.
    if [[ -f "${partial_path}" ]]; then
        if verify_snapshot "${partial_path}"; then
            mv -- "${partial_path}" "${snapshot_path}"
        else
            info "discarding incomplete or invalid transformed snapshot"
            rm -f -- "${partial_path}"
        fi
    fi
    if [[ ! -f "${snapshot_path}" ]]; then
        download_verified_file \
            "${source_path}" \
            "${source_partial_path}" \
            "${ECASH_SNAPSHOT_SOURCE_SIZE}" \
            "${ECASH_SNAPSHOT_SOURCE_SHA256}" \
            "${NETWORK_ID} snapshot source" \
            "${ECASH_SNAPSHOT_URL}" \
            "${ECASH_SNAPSHOT_MIRROR_URL}"
        info "rewriting the version-2 snapshot network magic for ${NETWORK_ID}"
        rewrite_snapshot_network_magic_v2 \
            "${source_path}" \
            "${partial_path}" \
            "${ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC}" \
            "${ECASH_NETWORK_MAGIC}"
        verify_snapshot "${partial_path}" || {
            rm -f -- "${partial_path}"
            die "transformed snapshot failed verification"
        }
        mv -- "${partial_path}" "${snapshot_path}"
    fi
else
    download_verified_file \
        "${snapshot_path}" \
        "${partial_path}" \
        "${ECASH_SNAPSHOT_SIZE}" \
        "${ECASH_SNAPSHOT_SHA256}" \
        "${NETWORK_ID} snapshot" \
        "${ECASH_SNAPSHOT_URL}"
fi

if [[ "${ECASH_SNAPSHOT_TRANSFORM}" == network_magic_v2 ]]; then
    # This also covers recovery after an interruption between promoting the
    # transformed artifact and deleting its reproducible source copy.
    cleanup_transformed_snapshot_source \
        "${snapshot_path}" "${source_path}" "${source_partial_path}" ||
        die "refusing to clean transformed snapshot sources before promotion"
fi

service_is_running ecash-node || die "ecash-node is not running; run 'just up' first"

wait_seconds="${SNAPSHOT_RPC_WAIT_SECONDS:-43200}"
deadline="$((SECONDS + wait_seconds))"
info "waiting up to ${wait_seconds}s for ecash-node RPC after startup or recovery"
until chainstates="$(node_cli getchainstates 2>/dev/null)" &&
    jq -e '.chainstates | type == "array"' <<<"${chainstates}" >/dev/null; do
    ((SECONDS < deadline)) ||
        die "ecash-node RPC did not leave warmup after ${wait_seconds}s"
    sleep 10
done

if jq -e '.chainstates | any(has("snapshot_blockhash"))' \
    <<<"${chainstates}" >/dev/null; then
    active_snapshot_hash="$(
        jq -er \
            '.chainstates[] | select(has("snapshot_blockhash")) | .snapshot_blockhash' \
            <<<"${chainstates}"
    )"
    [[ "${active_snapshot_hash}" == "${ECASH_SNAPSHOT_BLOCK_HASH}" ]] ||
        die "active AssumeUTXO snapshot uses ${active_snapshot_hash}, expected ${ECASH_SNAPSHOT_BLOCK_HASH}"
    require_activation_header
    ensure_activation_block_data
    info "an AssumeUTXO snapshot chainstate is already active"
    exit 0
fi
if node_history_is_fully_validated "${chainstates}"; then
    require_activation_block
    info "the complete ${NETWORK_ID} chainstate is already validated; no snapshot is needed"
    exit 0
fi

wait_seconds="${SNAPSHOT_HEADER_WAIT_SECONDS:-1800}"
deadline="$((SECONDS + wait_seconds))"
info "waiting for the exact ${NETWORK_ID} snapshot header ${ECASH_SNAPSHOT_BLOCK_HASH}"
until header_json="$(node_cli getblockheader "${ECASH_SNAPSHOT_BLOCK_HASH}" 2>/dev/null)" &&
    jq -e --argjson height "${ECASH_SNAPSHOT_HEIGHT}" \
        '.height == $height' <<<"${header_json}" >/dev/null; do
    ((SECONDS < deadline)) ||
        die "snapshot header ${ECASH_SNAPSHOT_BLOCK_HASH} was not available after ${wait_seconds}s"
    sleep 10
done

info "loading the verified AssumeUTXO snapshot"
node_cli -rpcclienttimeout=0 loadtxoutset "/snapshots/${ECASH_SNAPSHOT_FILE}"
ensure_activation_block_data
node_cli getchainstates | jq '{headers, chainstates}'
