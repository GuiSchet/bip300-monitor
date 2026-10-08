#!/usr/bin/env bash
# Compare verbatim official protobuf against its immutable source, never a patch.
set -euo pipefail
REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPOSITORY_ROOT
PROTO_ROOT="${REPOSITORY_ROOT}/proto/upstream"
VERSIONS_FILE="${BIP300_MONITOR_PROTO_VERSIONS_FILE:-${REPOSITORY_ROOT}/deployments/ecash/VERSIONS.lock}"
commit="$(sed -n 's/^ENFORCER_COMMIT=//p' "${VERSIONS_FILE}")"
[[ "${commit}" =~ ^[[:xdigit:]]{40}$ ]] || { echo 'invalid official commit' >&2; exit 1; }
grep -Fxq -- "- Official commit: \`${commit}\`" "${PROTO_ROOT}/README.md"
[[ ! -e "${PROTO_ROOT}/validator-observer.patch" ]] || { echo 'observer patches are forbidden' >&2; exit 1; }
(cd "${PROTO_ROOT}" && sha256sum --check --quiet SHA256SUMS)
case "${1:---offline}" in
--offline) ;;
--online)
    scratch="$(mktemp -d)"
    trap 'rm -rf -- "${scratch}"' EXIT
    while read -r checksum path; do
        curl --fail --silent --show-error --location --max-time 30 \
            "https://raw.githubusercontent.com/LayerTwo-Labs/bip300301_enforcer/${commit}/proto/${path}" \
            --output "${scratch}/proto"
        [[ "$(sha256sum "${scratch}/proto" | cut -d ' ' -f1)" == "${checksum}" ]] || { echo "upstream mismatch: ${path}" >&2; exit 1; }
    done < "${PROTO_ROOT}/SHA256SUMS"
    ;;
*) echo 'usage: check-proto-vendor.sh [--offline|--online]' >&2; exit 1 ;;
esac
printf 'Official protobuf verified: %s\n' "${commit}"
