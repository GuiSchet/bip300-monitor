#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
export COMPOSE_ENV_FILE="${DEPLOYMENT_ROOT}/.env.example"

for command_name in docker jq just shellcheck shfmt yamllint; do
    require_command "${command_name}"
done

shellcheck --external-sources \
    --source-path="${DEPLOYMENT_ROOT}/scripts" \
    "${DEPLOYMENT_ROOT}"/scripts/*.sh
shfmt --diff --indent 4 "${DEPLOYMENT_ROOT}"/scripts/*.sh
yamllint --config-file "${DEPLOYMENT_ROOT}/.yamllint.yml" \
    "${DEPLOYMENT_ROOT}/compose.yaml"
just --justfile "${DEPLOYMENT_ROOT}/justfile" --fmt --check

config_json="$(compose config --format json)"
jq -e '.services | keys == ["ecash-node"]' <<<"${config_json}" >/dev/null
jq -e --arg image "${ECASH_NODE_IMAGE}" \
    '.services["ecash-node"].image == $image' <<<"${config_json}" >/dev/null
jq -e '[.services[]?.ports[]?] | length == 0' <<<"${config_json}" >/dev/null

grep -Fxq 'connect=drynet3.drivechain.dev:8337' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
grep -Fxq 'listen=0' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
grep -Fxq 'rpcallowip=172.30.0.0/24' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
[[ "${DRYNET_ACTIVATION_BLOCK_HASH}" =~ ^[[:xdigit:]]{64}$ ]]

info "Drynet3 deployment checks passed"
