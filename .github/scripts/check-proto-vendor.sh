#!/usr/bin/env bash

# Verify that the vendored enforcer API stays consistent with both its recorded
# provenance and the enforcer commit the deployment pins. The offline mode needs
# no network, so deployment static checks can run it too.

set -euo pipefail

REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPOSITORY_ROOT
readonly PROTO_ROOT="${REPOSITORY_ROOT}/proto/upstream"
readonly PROTO_README="${PROTO_ROOT}/README.md"
readonly OBSERVER_PATCH="${PROTO_ROOT}/validator-observer.patch"
VERSIONS_FILE="${BIP300_MONITOR_PROTO_VERSIONS_FILE:-${REPOSITORY_ROOT}/deployments/ecash/VERSIONS.lock}"
readonly VERSIONS_FILE
readonly UPSTREAM_RAW_URL=https://raw.githubusercontent.com/LayerTwo-Labs/bip300301_enforcer
readonly DEFAULT_OBSERVER_REPO="${REPOSITORY_ROOT}/../bip300301_enforcer"

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
[[ -f "${OBSERVER_PATCH}" ]] || die "missing ${OBSERVER_PATCH}"
[[ -f "${VERSIONS_FILE}" ]] || die "missing ${VERSIONS_FILE}"

base_commit="$(
    grep -oE '^- Base commit: `[[:xdigit:]]{40}`$' "${PROTO_README}" |
        grep -oE '[[:xdigit:]]{40}' || true
)"
[[ "${base_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read the observer base commit from ${PROTO_README}"

observer_commit="$(
    grep -oE '^- Observer commit: `[[:xdigit:]]{40}`$' "${PROTO_README}" |
        grep -oE '[[:xdigit:]]{40}' || true
)"
[[ "${observer_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read the observer commit from ${PROTO_README}"

locked_base_commit="$(
    grep -oE '^ENFORCER_BASE_COMMIT=[[:xdigit:]]{40}$' "${VERSIONS_FILE}" |
        sed 's/^ENFORCER_BASE_COMMIT=//' || true
)"
[[ "${locked_base_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read ENFORCER_BASE_COMMIT from ${VERSIONS_FILE}"

locked_observer_commit="$(
    grep -oE '^ENFORCER_OBSERVER_COMMIT=[[:xdigit:]]{40}$' "${VERSIONS_FILE}" |
        sed 's/^ENFORCER_OBSERVER_COMMIT=//' || true
)"
[[ "${locked_observer_commit}" =~ ^[[:xdigit:]]{40}$ ]] ||
    die "could not read ENFORCER_OBSERVER_COMMIT from ${VERSIONS_FILE}"

[[ "${base_commit}" == "${locked_base_commit}" ]] ||
    die "observer proto is based on ${base_commit} but ${VERSIONS_FILE} records ${locked_base_commit}"
[[ "${observer_commit}" == "${locked_observer_commit}" ]] ||
    die "proto/upstream is vendored from observer ${observer_commit} but ${VERSIONS_FILE} records ${locked_observer_commit}"

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

info "vendored proto matches base ${base_commit} and observer ${observer_commit}"

if [[ "${check_upstream}" == false ]]; then
    exit 0
fi

for command_name in curl git; do
    command -v "${command_name}" >/dev/null 2>&1 ||
        die "required command not found: ${command_name}"
done
upstream_dir="$(mktemp -d)"
trap 'rm -rf -- "${upstream_dir}"' EXIT

while read -r _ proto_path; do
    destination="${upstream_dir}/proto/${proto_path}"
    mkdir -p -- "$(dirname -- "${destination}")"
    curl --fail --silent --show-error --location --max-time 30 \
        --output "${destination}" \
        "${UPSTREAM_RAW_URL}/${base_commit}/proto/${proto_path}" ||
        die "could not download proto/${proto_path} at ${base_commit}"
done <<<"${recorded_checksums}"

(
    cd "${upstream_dir}"
    git apply --check "${OBSERVER_PATCH}"
    git apply "${OBSERVER_PATCH}"
) || die "observer patch no longer applies cleanly to official base ${base_commit}"

(cd "${upstream_dir}/proto" && sha256sum --check --quiet) <<<"${recorded_checksums}" ||
    die "official base plus observer patch does not reproduce proto/upstream; re-vendor the files, patch and provenance together"

observer_repo="${ENFORCER_OBSERVER_REPO_PATH:-${DEFAULT_OBSERVER_REPO}}"
if git -C "${observer_repo}" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "${observer_repo}" cat-file -e "${observer_commit}^{commit}" 2>/dev/null ||
        die "observer commit ${observer_commit} is absent from ${observer_repo}"
    observer_parent="$(git -C "${observer_repo}" rev-parse "${observer_commit}^")"
    [[ "${observer_parent}" == "${base_commit}" ]] ||
        die "observer ${observer_commit} has parent ${observer_parent}, expected official base ${base_commit}"
    while read -r _ proto_path; do
        cmp -s \
            <(git -C "${observer_repo}" show "${observer_commit}:proto/${proto_path}") \
            "${PROTO_ROOT}/${proto_path}" ||
            die "vendored ${proto_path} differs from observer commit ${observer_commit}"
    done <<<"${recorded_checksums}"
    info "local observer commit and direct parent verified in ${observer_repo}"
else
    info "observer checkout not present; reproducibility verified from the checked-in patch"
fi

info "official base plus observer patch reproduces the vendored API byte for byte"
