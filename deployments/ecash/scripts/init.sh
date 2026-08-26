#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
case "$(uname -m)" in
x86_64 | amd64) ;;
*) die "the pinned ${NETWORK_ID} node image requires an x86_64 host" ;;
esac

for command_name in cmp curl docker jq mktemp realpath sha256sum tar; do
    require_command "${command_name}"
done
docker compose version >/dev/null
docker info >/dev/null 2>&1 || die "Docker Engine is not available to the current user"

env_file="${DEPLOYMENT_ROOT}/.env"
if [[ ! -f "${env_file}" ]]; then
    cp "${DEPLOYMENT_ROOT}/.env.example" "${env_file}"
    info "created ${env_file}; review it before deploying to a VM"
fi

load_deployment_env
resolved_data_root="$(data_root)"
[[ "${resolved_data_root}" != "/" ]] || die "ECASH_DATA_ROOT must not be /"

for directory in \
    "${resolved_data_root}/node" \
    "${resolved_data_root}/node/blocks" \
    "${resolved_data_root}/config" \
    "${resolved_data_root}/snapshots" \
    "${resolved_data_root}/rpc-cookie" \
    "${resolved_data_root}/enforcer"; do
    if ! mkdir -p -- "${directory}" 2>/dev/null; then
        require_command sudo
        sudo install -d -o "${PUID}" -g "${PGID}" "${directory}"
    fi
    [[ -w "${directory}" ]] ||
        die "${directory} is not writable by the current user"
done

chmod 0750 "${resolved_data_root}/rpc-cookie"

# bitcoind reads its configuration once, at startup. Rendering a changed file
# under a running node would leave the container on the previous settings while
# preflight compares the new file and reports success.
node_config="${resolved_data_root}/config/ecash.conf"
node_config_changed=false
if [[ -f "${node_config}" ]]; then
    previous_node_config="$(mktemp)"
    render_node_config "${previous_node_config}"
    cmp --silent "${previous_node_config}" "${node_config}" ||
        node_config_changed=true
    rm -f -- "${previous_node_config}"
fi
render_node_config "${node_config}"
if [[ "${node_config_changed}" == true ]] && service_is_running ecash-node; then
    die "the node configuration changed while ecash-node is running; recreate the container so bitcoind reads it ('just down' then 'just up'), then run 'just preflight'"
fi

compose config --quiet
info "${NETWORK_ID} deployment initialized at ${resolved_data_root}"
