#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

case "$(uname -m)" in
x86_64 | amd64) ;;
*) die "the pinned Drynet3 node image requires an x86_64 host" ;;
esac

for command_name in curl docker jq realpath sha256sum tar; do
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

for directory in "${resolved_data_root}/node" "${resolved_data_root}/snapshots"; do
    if ! mkdir -p -- "${directory}" 2>/dev/null; then
        require_command sudo
        sudo install -d -o "${PUID}" -g "${PGID}" "${directory}"
    fi
    [[ -w "${directory}" ]] ||
        die "${directory} is not writable by the current user"
done

compose config --quiet
info "Drynet3 deployment initialized at ${resolved_data_root}"
