#!/usr/bin/env bash

# Verify that the vendored enforcer API stays consistent with both its recorded
# provenance and the enforcer commit the deployment pins. The offline mode needs
# no network, so deployment static checks can run it too.

set -euo pipefail

REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPOSITORY_ROOT
readonly PROTO_ROOT="${REPOSITORY_ROOT}/proto/upstream"
readonly PROTO_README="${PROTO_ROOT}/README.md"
readonly VERSIONS_FILE="${REPOSITORY_ROOT}/deployments/ecash/VERSIONS.lock"
readonly UPSTREAM_RAW_URL=https://raw.githubusercontent.com/LayerTwo-Labs/bip300301_enforcer

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

info() {
    printf '==> %s\n' "$*"
}

check_upstream=false
case "${1:-}" in
"" | --offline) ;;
--online) check_upstream=true ;;
*) die "usage: check-proto-vendor.sh [--offline|--online]" ;;
esac

for command_name in find grep sed sha256sum; do
    command -v "${command_name}" >/dev/null 2>&1 ||
        die "required command not found: ${command_name}"
done

[[ -f "${PROTO_README}" ]] ||
    die "missing vendored proto provenance: ${PROTO_README}"
[[ -f "${VERSIONS_FILE}" ]] || die "missing ${VERSIONS_FILE}"

vendored_commit="$(
    grep -oE '^- Commit: `[[:xdigit:]]{40}`$' "${PROTO_README}" |
        grep -oE '[[:xdigit:]]{40}' || true
)"
[[ "${vendored_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read the vendored proto commit from ${PROTO_README}"

enforcer_commit="$(
    grep -oE '^ENFORCER_COMMIT=[[:xdigit:]]{40}$' "${VERSIONS_FILE}" |
        sed 's/^ENFORCER_COMMIT=//' || true
)"
[[ "${enforcer_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read ENFORCER_COMMIT from ${VERSIONS_FILE}"

[[ "${vendored_commit}" == "${enforcer_commit}" ]] ||
    die "proto/upstream is vendored from ${vendored_commit} but ${VERSIONS_FILE} deploys enforcer ${enforcer_commit}; re-vendor proto/upstream in the same change that promotes the enforcer"

# The fenced block in the provenance file is the recorded checksum list.
recorded_checksums="$(
    sed -n '/^```text$/,/^```$/p' "${PROTO_README}" |
        grep -E '^[[:xdigit:]]{64}  [a-z0-9/._-]+$' || true
)"
[[ -n "${recorded_checksums}" ]] ||
    die "no vendored proto checksums are recorded in ${PROTO_README}"

recorded_count="$(grep -c '' <<<"${recorded_checksums}")"
actual_count="$(find "${PROTO_ROOT}" -type f -name '*.proto' | grep -c '' || true)"
[[ "${recorded_count}" == "${actual_count}" ]] ||
    die "${PROTO_README} records ${recorded_count} checksums but proto/upstream contains ${actual_count} .proto files"

(cd "${PROTO_ROOT}" && sha256sum --check --quiet) <<<"${recorded_checksums}" ||
    die "vendored proto files do not match the checksums recorded in ${PROTO_README}"

info "vendored proto matches its provenance and enforcer ${enforcer_commit}"

if [[ "${check_upstream}" == false ]]; then
    exit 0
fi

command -v curl >/dev/null 2>&1 || die "required command not found: curl"
upstream_dir="$(mktemp -d)"
trap 'rm -rf -- "${upstream_dir}"' EXIT

while read -r _ proto_path; do
    destination="${upstream_dir}/${proto_path}"
    mkdir -p -- "$(dirname -- "${destination}")"
    curl --fail --silent --show-error --location --max-time 30 \
        --output "${destination}" \
        "${UPSTREAM_RAW_URL}/${enforcer_commit}/proto/${proto_path}" ||
        die "could not download proto/${proto_path} at ${enforcer_commit}"
done <<<"${recorded_checksums}"

(cd "${upstream_dir}" && sha256sum --check --quiet) <<<"${recorded_checksums}" ||
    die "proto/upstream no longer matches upstream ${enforcer_commit}; re-vendor the files and update ${PROTO_README}"

info "vendored proto matches upstream ${enforcer_commit} byte for byte"
