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
[[ "${verified_live_height}" =~ ^[0-9]+$ ]] ||
    die "live verification returned an invalid height"
[[ "${verified_live_hash}" =~ ^[[:xdigit:]]{64}$ ]] ||
    die "live verification returned an invalid block hash"
{
    printf 'accepted_at=%s\n' "$(date --utc +%Y-%m-%dT%H:%M:%SZ)"
    printf 'network_id=%s\n' "${NETWORK_ID}"
    printf 'repository_commit=%s\n' "${repository_commit}"
    printf 'node_commit=%s\n' "${ECASH_NODE_COMMIT}"
    printf 'node_image=%s\n' "${ECASH_NODE_IMAGE}"
    printf 'enforcer_commit=%s\n' "${ENFORCER_COMMIT}"
    printf 'enforcer_image=%s\n' "${ENFORCER_IMAGE}"
    printf 'monitor_image_commit=%s\n' "${MONITOR_IMAGE_COMMIT}"
    printf 'verified_live_height=%s\n' "${verified_live_height}"
    printf 'verified_live_hash=%s\n' "${verified_live_hash}"
} >"${marker_tmp}"
chmod 0644 "${marker_tmp}"
mv -- "${marker_tmp}" "${marker_path}"
rm -f -- "${live_result}"
trap - EXIT

info "deployment accepted at ${marker_path}"
