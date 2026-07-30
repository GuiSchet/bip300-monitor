#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
export COMPOSE_ENV_FILE="${DEPLOYMENT_ROOT}/.env.example"

for command_name in docker jq just mktemp shellcheck shfmt yamllint; do
    require_command "${command_name}"
done

shellcheck --external-sources \
    --source-path="${DEPLOYMENT_ROOT}/scripts" \
    "${DEPLOYMENT_ROOT}"/scripts/*.sh
shfmt --diff --indent 4 "${DEPLOYMENT_ROOT}"/scripts/*.sh
yamllint --config-file "${DEPLOYMENT_ROOT}/.yamllint.yml" \
    "${DEPLOYMENT_ROOT}/compose.yaml"
just --justfile "${DEPLOYMENT_ROOT}/justfile" --fmt --check

override_env="$(mktemp)"
trap 'rm -f -- "${override_env}"' EXIT
for override in \
    'ENFORCER_IMAGE=example.invalid/unpinned:latest' \
    '   ENFORCER_IMAGE=example.invalid/unpinned:latest'; do
    cp "${DEPLOYMENT_ROOT}/.env.example" "${override_env}"
    printf '\n%s\n' "${override}" >>"${override_env}"
    if (
        COMPOSE_ENV_FILE="${override_env}"
        load_deployment_env
    ) >/dev/null 2>&1; then
        die "deployment configuration accepted an override of ENFORCER_IMAGE"
    fi
done

for invalid_live_event_wait in 0 invalid; do
    cp "${DEPLOYMENT_ROOT}/.env.example" "${override_env}"
    printf '\nLIVE_EVENT_WAIT_SECONDS=%s\n' "${invalid_live_event_wait}" >>"${override_env}"
    if (
        COMPOSE_ENV_FILE="${override_env}"
        load_deployment_env
    ) >/dev/null 2>&1; then
        die "deployment configuration accepted invalid LIVE_EVENT_WAIT_SECONDS=${invalid_live_event_wait}"
    fi
done

live_hash='0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
extractor_slot_9="INFO published live enforcer event sidechain=9 height=123 hash=${live_hash}"
extractor_slot_98="INFO published live enforcer event sidechain=98 height=123 hash=${live_hash}"
logger_slot_9="INFO received enforcer event summary=\"sidechain=9 height=123 hash=${live_hash}\""
logger_slot_98="INFO received enforcer event summary=\"sidechain=98 height=123 hash=${live_hash}\""

logs_contain_live_event \
    "${extractor_slot_9}" "published live enforcer event" 9 "${live_hash}" ||
    die "live event matcher rejected extractor slot 9"
logs_contain_live_event \
    "${extractor_slot_98}" "published live enforcer event" 98 "${live_hash}" ||
    die "live event matcher rejected extractor slot 98"
logs_contain_live_event \
    "${logger_slot_9}" "received enforcer event" 9 "${live_hash}" ||
    die "live event matcher rejected quoted logger slot 9"
logs_contain_live_event \
    "${logger_slot_98}" "received enforcer event" 98 "${live_hash}" ||
    die "live event matcher rejected quoted logger slot 98"

if logs_contain_live_event \
    "${extractor_slot_98}" "published live enforcer event" 9 "${live_hash}"; then
    die "live event matcher confused slot 98 with slot 9"
fi
if logs_contain_live_event \
    "${logger_slot_9}" "received enforcer event" 98 "${live_hash}"; then
    die "live event matcher confused slot 9 with slot 98"
fi
if logs_contain_live_event \
    "${extractor_slot_9}" "received enforcer event" 9 "${live_hash}"; then
    die "live event matcher accepted the wrong log message"
fi
if logs_contain_live_event \
    "${logger_slot_9}" "received enforcer event" 9 "${live_hash%?}0"; then
    die "live event matcher accepted the wrong block hash"
fi

config_json="$(compose config --format json)"
jq -e '.services | keys == ["ecash-node", "enforcer", "enforcer-extractor", "event-logger", "nats"]' \
    <<<"${config_json}" >/dev/null
jq -e --arg image "${ECASH_NODE_IMAGE}" \
    '.services["ecash-node"].image == $image' <<<"${config_json}" >/dev/null
jq -e --arg image "${ENFORCER_IMAGE}" \
    '.services.enforcer.image == $image' <<<"${config_json}" >/dev/null
jq -e --arg image "${NATS_IMAGE}" \
    '.services.nats.image == $image' <<<"${config_json}" >/dev/null
jq -e --arg image "${ENFORCER_EXTRACTOR_IMAGE}" \
    '.services["enforcer-extractor"].image == $image' \
    <<<"${config_json}" >/dev/null
jq -e --arg image "${EVENT_LOGGER_IMAGE}" \
    '.services["event-logger"].image == $image' \
    <<<"${config_json}" >/dev/null
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

jq -e '.services.nats.user == "10002:10002" and .services.nats.read_only == true' \
    <<<"${config_json}" >/dev/null
jq -e '.services.nats.command
    | index("nats-server") != null
      and index("--http_port=8222") != null
      and all(. != "--jetstream" and . != "-js")' \
    <<<"${config_json}" >/dev/null
jq -e '.services.nats.healthcheck.test | any(contains("/healthz"))' \
    <<<"${config_json}" >/dev/null

for monitor_service in enforcer-extractor event-logger; do
    jq -e --arg service "${monitor_service}" \
        '.services[$service].user == "10001:10001"
         and .services[$service].read_only == true' \
        <<<"${config_json}" >/dev/null
done

jq -e '.services["event-logger"].depends_on.nats.condition == "service_healthy"' \
    <<<"${config_json}" >/dev/null
jq -e '.services["enforcer-extractor"].depends_on.enforcer.condition == "service_healthy"
    and .services["enforcer-extractor"].depends_on.nats.condition == "service_healthy"
    and .services["enforcer-extractor"].depends_on["event-logger"].condition == "service_started"' \
    <<<"${config_json}" >/dev/null
jq -e '.services["event-logger"].environment.BIP300_MONITOR_NATS_URL == "nats://nats:4222"
    and .services["event-logger"].environment.BIP300_MONITOR_FULL_EVENTS == "true"' \
    <<<"${config_json}" >/dev/null
jq -e '.services["enforcer-extractor"].environment.BIP300_MONITOR_NATS_URL == "nats://nats:4222"
    and .services["enforcer-extractor"].environment.BIP300_MONITOR_ENFORCER_ENDPOINT == "http://enforcer:50051"
    and .services["enforcer-extractor"].environment.BIP300_MONITOR_SIDECHAINS == "9,98"' \
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
[[ "${MONITOR_IMAGE_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]]
[[ "${ENFORCER_EXTRACTOR_IMAGE}" == *":sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:"* ]]
[[ "${EVENT_LOGGER_IMAGE}" == *":sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:"* ]]

info "Drynet3 deployment checks passed"
