#!/usr/bin/env bash
# Reviews a network's lock reference without deploying it. A candidate
# (RELEASE_STATUS=candidate) may still leave the values that do not exist yet as
# REPLACE_WITH_ placeholders; every other value is checked like an active lock.

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

lock="$(realpath -- "${1:?usage: check-lock.sh VERSIONS.<network>.lock.example}")"
[[ "${lock}" =~ /VERSIONS\.([a-z0-9-]+)\.lock\.example$ ]] ||
    die "${lock} is not a lock reference"
named_network="${BASH_REMATCH[1]}"

# Values a candidate cannot pin before launch: the activation block, the
# transformed snapshot, the enforcer release that supports the network, and
# the monitor images built after it.
candidate_fields=(
    ECASH_ACTIVATION_BLOCK_HASH
    ECASH_SNAPSHOT_SHA256
    ENFORCER_UPSTREAM_REVIEWED_COMMIT
    ENFORCER_COMMIT
    ENFORCER_IMAGE
    MONITOR_IMAGE_COMMIT
    ENFORCER_EXTRACTOR_IMAGE
    EVENT_LOGGER_IMAGE
)

lock_keys() {
    grep -oE '^[A-Z][A-Z0-9_]*=' "$1" | sort
}
diff <(lock_keys "${VERSIONS_FILE}") <(lock_keys "${lock}") ||
    die "${lock} does not lock the same values as VERSIONS.lock"

# The reference is tracked in the repository and contains assignments only.
# shellcheck disable=SC1090
source "${lock}"
[[ "${NETWORK_ID}" == "${named_network}" ]] ||
    die "${lock} locks NETWORK_ID=${NETWORK_ID}"

placeholders="$(grep -oE '^[A-Z][A-Z0-9_]*=REPLACE_WITH_' "${lock}" | cut -d= -f1 || true)"
case "${RELEASE_STATUS:-}" in
ready)
    [[ -z "${placeholders}" ]] || die "a ready lock cannot contain placeholders: ${placeholders//$'\n'/ }"
    ;;
candidate)
    zero_hash="$(printf '0%.0s' {1..64})"
    while IFS= read -r field; do
        [[ -n "${field}" ]] || continue
        [[ " ${candidate_fields[*]} " == *" ${field} "* ]] ||
            die "${field} cannot be a placeholder, even in a candidate"
        # Stand-ins with the right shape let the remaining checks run.
        case "${field}" in
        *_COMMIT) printf -v "${field}" '%s' "${zero_hash:0:40}" ;;
        *_HASH | *_SHA256) printf -v "${field}" '%s' "${zero_hash}" ;;
        *) : ;;
        esac
    done <<<"${placeholders}"
    [[ "${ENFORCER_IMAGE}" != REPLACE_WITH_* ]] ||
        ENFORCER_IMAGE="candidate.invalid/enforcer:sha-${ENFORCER_COMMIT:0:12}@sha256:${zero_hash}"
    [[ "${ENFORCER_EXTRACTOR_IMAGE}" != REPLACE_WITH_* ]] ||
        ENFORCER_EXTRACTOR_IMAGE="candidate.invalid/extractor:sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:${zero_hash}"
    [[ "${EVENT_LOGGER_IMAGE}" != REPLACE_WITH_* ]] ||
        EVENT_LOGGER_IMAGE="candidate.invalid/logger:sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:${zero_hash}"
    ;;
*)
    die "RELEASE_STATUS must be ready or candidate"
    ;;
esac

validate_versions
validate_release_pins

if [[ " ${placeholders//$'\n'/ } " != *" ENFORCER_COMMIT "* ]]; then
    BIP300_MONITOR_PROTO_VERSIONS_FILE="${lock}" \
        "$(git -C "${DEPLOYMENT_ROOT}" rev-parse --show-toplevel)/.github/scripts/check-proto-vendor.sh" --offline
fi

if [[ -n "${placeholders}" ]]; then
    info "${NETWORK_ID} lock is a valid candidate; still to pin: ${placeholders//$'\n'/ }"
else
    info "${NETWORK_ID} lock is valid and ready"
fi
