#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in awk bash cmp curl df docker grep jq mktemp realpath timeout; do
    require_command "${command_name}"
done

case "$(uname -m)" in
x86_64 | amd64) ;;
*) die "the pinned ${NETWORK_ID} images require an x86_64 host" ;;
esac

docker compose version >/dev/null
docker buildx version >/dev/null
docker info >/dev/null 2>&1 || die "Docker Engine is not available to the current user"
compose config --quiet
config_json="$(compose config --format json)"
jq -e '[.services[]?.ports[]?] | length == 0' <<<"${config_json}" >/dev/null ||
    die "the deployment unexpectedly publishes a host port"

resolved_data_root="$(data_root)"
[[ "${resolved_data_root}" != / ]] || die "ECASH_DATA_ROOT must not be /"
[[ -d "${resolved_data_root}" ]] ||
    die "data root does not exist; run 'just init' first: ${resolved_data_root}"

node_config="${resolved_data_root}/config/ecash.conf"
[[ -f "${node_config}" ]] ||
    die "generated node configuration is missing; run 'just init' again"
expected_node_config="$(mktemp)"
trap 'rm -f -- "${expected_node_config}"' EXIT
render_node_config "${expected_node_config}"
cmp --silent "${expected_node_config}" "${node_config}" ||
    die "generated node configuration does not match NETWORK_ID=${NETWORK_ID}; run 'just init' again"

filesystem_total="$(df --block-size=1 --output=size "${resolved_data_root}" | awk 'NR == 2 { print $1 }')"
filesystem_available="$(df --block-size=1 --output=avail "${resolved_data_root}" | awk 'NR == 2 { print $1 }')"
for value in "${filesystem_total}" "${filesystem_available}"; do
    [[ "${value}" =~ ^[0-9]+$ ]] || die "could not calculate deployment disk requirements"
done

node_data_present=false
for node_data_indicator in \
    "${resolved_data_root}/node/blocks/blk00000.dat" \
    "${resolved_data_root}/node/chainstate" \
    "${resolved_data_root}/node/indexes" \
    "${resolved_data_root}/node/debug.log"; do
    if [[ -e "${node_data_indicator}" ]]; then
        node_data_present=true
        break
    fi
done
required_reserve="$((filesystem_total * ECASH_MIN_FREE_PERCENT / 100))"
if [[ "${node_data_present}" == false ]]; then
    required_available="$((ECASH_EXPECTED_DATA_BYTES + required_reserve))"
    ((filesystem_available >= required_available)) ||
        die "insufficient disk for a new ${NETWORK_ID} data root: available=${filesystem_available}, required=${required_available}"
    info "new-data disk preflight passed (available=${filesystem_available}, expected_data=${ECASH_EXPECTED_DATA_BYTES}, reserve=${required_reserve})"
else
    free_percent="$((filesystem_available * 100 / filesystem_total))"
    ((free_percent >= ECASH_MIN_FREE_PERCENT)) ||
        die "existing ${NETWORK_ID} data root has only ${free_percent}% disk free; minimum is ${ECASH_MIN_FREE_PERCENT}%"
    info "existing-data disk preflight passed (available=${filesystem_available}, free=${free_percent}%)"
fi

if tip_height="$(
    curl --fail --silent --show-error --max-time 15 "${ECASH_TIP_HEIGHT_URL}" 2>/dev/null
)" && [[ "${tip_height}" =~ ^[0-9]+$ ]]; then
    info "public ${NETWORK_ID} tip is ${tip_height} (advisory only)"
else
    info "warning: public ${NETWORK_ID} tip endpoint is unavailable; continuing because it is advisory"
fi

while IFS= read -r peer; do
    peer_host="${peer%:*}"
    peer_port="${peer##*:}"
    # Positional parameters are intentionally expanded by the inner Bash.
    # shellcheck disable=SC2016
    timeout 5 bash -c 'exec 3<>"/dev/tcp/$1/$2"' _ \
        "${peer_host}" "${peer_port}" ||
        die "could not connect to ${peer}"
    info "peer endpoint reachable: ${peer}"
done < <(network_peers)

snapshot_headers="$(
    curl --fail --silent --show-error --head --max-time 30 \
        "${ECASH_SNAPSHOT_URL}"
)" ||
    die "could not reach the pinned ${NETWORK_ID} snapshot"
snapshot_content_length="$(
    awk '
        tolower($1) == "content-length:" {
            gsub("\\r", "", $2)
            content_length = $2
        }
        END { print content_length }
    ' <<<"${snapshot_headers}"
)"
[[ "${snapshot_content_length}" == "${ECASH_SNAPSHOT_SIZE}" ]] ||
    die "snapshot HTTP size is ${snapshot_content_length:-unavailable}, expected ${ECASH_SNAPSHOT_SIZE}"

snapshot_checksums="$(
    curl --fail --silent --show-error --max-time 15 \
        "${ECASH_SNAPSHOT_CHECKSUMS_URL}"
)" || die "could not read the upstream ${NETWORK_ID} snapshot checksums"
grep -Fxq "${ECASH_SNAPSHOT_SHA256}  ${ECASH_SNAPSHOT_FILE}" \
    <<<"${snapshot_checksums}" ||
    die "upstream checksums do not match the locked ${NETWORK_ID} snapshot"

for image in \
    "${ECASH_NODE_IMAGE}" \
    "${ENFORCER_IMAGE}" \
    "${NATS_IMAGE}" \
    "${ENFORCER_EXTRACTOR_IMAGE}" \
    "${EVENT_LOGGER_IMAGE}"; do
    docker buildx imagetools inspect "${image}" >/dev/null ||
        die "could not resolve pinned image: ${image}"
done

info "${NETWORK_ID} deployment preflight passed"
