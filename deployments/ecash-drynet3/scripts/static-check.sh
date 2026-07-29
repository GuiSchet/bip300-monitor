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
jq -e '.services | keys == ["ecash-node", "enforcer"]' <<<"${config_json}" >/dev/null
jq -e --arg image "${ECASH_NODE_IMAGE}" \
    '.services["ecash-node"].image == $image' <<<"${config_json}" >/dev/null
jq -e --arg image "${ENFORCER_IMAGE}" \
    '.services.enforcer.image == $image' <<<"${config_json}" >/dev/null
jq -e '[.services[]?.ports[]?] | length == 0' <<<"${config_json}" >/dev/null
jq -e '.services.enforcer.depends_on["ecash-node"].condition == "service_healthy"' \
    <<<"${config_json}" >/dev/null
jq -e '[.services.enforcer.volumes[] | select(.target == "/rpc-cookie" and .read_only == true)] | length == 1' \
    <<<"${config_json}" >/dev/null
jq -e '[.services.enforcer.volumes[] | select(.target == "/node-blocks" and .read_only == true)] | length == 1' \
    <<<"${config_json}" >/dev/null
jq -e '.services.enforcer.user == "1000:1000"' <<<"${config_json}" >/dev/null
jq -e '.services.enforcer.command | all(. != "--enable-wallet" and . != "--enable-mempool")' \
    <<<"${config_json}" >/dev/null
jq -e '.services.enforcer.healthcheck.test | any(contains("GetChainTip"))' \
    <<<"${config_json}" >/dev/null

for expected_arg in \
    '--network-preset=drynet3' \
    '--node-rpc-addr=ecash-node:8332' \
    '--node-rpc-cookie-path=/rpc-cookie/.cookie' \
    '--node-zmq-addr-sequence=tcp://ecash-node:29000' \
    '--node-blocks-dir=/node-blocks' \
    '--serve-grpc-addr=0.0.0.0:50051' \
    '--bitcoin-core-expected-version=31'; do
    jq -e --arg expected_arg "${expected_arg}" \
        '.services.enforcer.command | index($expected_arg) != null' \
        <<<"${config_json}" >/dev/null
done

grep -Fxq 'connect=drynet3.drivechain.dev:8337' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
grep -Fxq 'listen=0' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
grep -Fxq 'rpcallowip=172.30.0.0/24' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
grep -Fxq 'rpccookiefile=/rpc-cookie/.cookie' \
    "${DEPLOYMENT_ROOT}/config/drynet3/drivechain-ecash.conf"
[[ "${DRYNET_ACTIVATION_BLOCK_HASH}" =~ ^[[:xdigit:]]{64}$ ]]
[[ "${ENFORCER_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]]

info "Drynet3 deployment checks passed"
