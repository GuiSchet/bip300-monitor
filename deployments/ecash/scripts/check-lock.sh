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

placeholders="$(lock_placeholders "${lock}")"
case "${RELEASE_STATUS:-}" in
ready)
    [[ -z "${placeholders}" ]] || die "a ready lock cannot contain placeholders: ${placeholders//$'\n'/ }"
    ;;
candidate)
    use_candidate_stand_ins "${placeholders}" "${CANDIDATE_FIELDS[@]}"
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
