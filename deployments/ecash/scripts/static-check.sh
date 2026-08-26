#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
export COMPOSE_ENV_FILE="${DEPLOYMENT_ROOT}/.env.example"
load_deployment_env

for command_name in cmp docker git jq just mktemp shellcheck shfmt stat yamllint; do
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
rendered_node_config="$(mktemp)"
cookie_root="$(mktemp -d)"
trap 'rm -f -- "${override_env}" "${rendered_node_config}"
    rm -rf -- "${cookie_root}"' EXIT
for override in \
    'ENFORCER_IMAGE=example.invalid/unpinned:latest' \
    'NETWORK_ID=unreviewed-network' \
    'COMPOSE_PROJECT_NAME=stale-project' \
    'ECASH_DATA_ROOT=/srv/bip300-monitor/stale-network' \
    '   ECASH_ACTIVATION_HEIGHT=1'; do
    cp "${DEPLOYMENT_ROOT}/.env.example" "${override_env}"
    printf '\n%s\n' "${override}" >>"${override_env}"
    if (
        COMPOSE_ENV_FILE="${override_env}"
        load_deployment_env
    ) >/dev/null 2>&1; then
        die "deployment configuration accepted a locked override: ${override}"
    fi
done

[[ "${COMPOSE_PROJECT_NAME}" == "bip300-ecash-${NETWORK_ID}" ]] ||
    die "Compose project name is not derived from NETWORK_ID"
[[ "${ECASH_DATA_ROOT}" == "${DEPLOYMENT_ROOT}/data/${NETWORK_ID}" ]] ||
    die "data root is not derived from ECASH_DATA_BASE and NETWORK_ID"

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

ready_chainstates='{"headers":996259,"chainstates":[{"blocks":996259,"validated":true}]}'
syncing_chainstates='{"headers":996259,"chainstates":[{"blocks":763703,"validated":true},{"blocks":996259,"snapshot_blockhash":"snapshot","validated":false}]}'
unvalidated_chainstate='{"headers":996259,"chainstates":[{"blocks":996259,"snapshot_blockhash":"snapshot","validated":false}]}'
empty_chainstates='{"headers":996259,"chainstates":[]}'

node_history_is_ready "${ready_chainstates}" ||
    die "node history readiness rejected one validated chainstate"
for incomplete_chainstates in \
    "${syncing_chainstates}" \
    "${unvalidated_chainstate}" \
    "${empty_chainstates}" \
    'not-json'; do
    if node_history_is_ready "${incomplete_chainstates}"; then
        die "node history readiness accepted incomplete or invalid chainstates"
    fi
done

# The enforcer reads a cookie the node image creates, so the check has to accept
# owner, group, and world readability and reject the root:root 0600 case.
mkdir -p "${cookie_root}/rpc-cookie"
cookie_path="${cookie_root}/rpc-cookie/.cookie"
touch "${cookie_path}"
cookie_uid="$(id -u)"
cookie_gid="$(id -g)"
foreign_uid="$((cookie_uid + 1))"
foreign_gid="$((cookie_gid + 1))"

cookie_is_readable() {
    local mode="$1"
    local uid="$2"
    local gid="$3"

    chmod "${mode}" "${cookie_path}"
    (
        ECASH_DATA_ROOT="${cookie_root}"
        PUID="${uid}"
        PGID="${gid}"
        require_rpc_cookie_readable
    ) >/dev/null 2>&1
}

cookie_is_readable 600 "${cookie_uid}" "${cookie_gid}" ||
    die "RPC cookie check rejected a cookie owned by the enforcer user"
cookie_is_readable 640 "${foreign_uid}" "${cookie_gid}" ||
    die "RPC cookie check rejected a cookie readable through its group"
cookie_is_readable 644 "${foreign_uid}" "${foreign_gid}" ||
    die "RPC cookie check rejected a world-readable cookie"
if cookie_is_readable 600 "${foreign_uid}" "${foreign_gid}"; then
    die "RPC cookie check accepted a cookie the enforcer cannot read"
fi
if cookie_is_readable 000 "${cookie_uid}" "${cookie_gid}"; then
    die "RPC cookie check accepted an unreadable cookie"
fi
rm -f -- "${cookie_path}"
if (
    ECASH_DATA_ROOT="${cookie_root}"
    require_rpc_cookie_readable
) >/dev/null 2>&1; then
    die "RPC cookie check accepted a missing cookie"
fi

live_hash='0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
extractor_slot_9="INFO enforcer_extractor: published live enforcer event event=\"block_connected\" sidechain=9 height=123 block_hash=${live_hash}"
extractor_slot_98="INFO enforcer_extractor: published live enforcer event event=\"block_connected\" sidechain=98 height=123 block_hash=${live_hash}"
logger_slot_9="INFO event_logger: received enforcer event subject=bip300.enforcer timestamp_ms=1 event=\"block_connected\" summary=sidechain=9 height=123 block_hash=${live_hash}"
logger_slot_98="INFO event_logger: received enforcer event subject=bip300.enforcer timestamp_ms=1 event=\"block_connected\" summary=sidechain=98 height=123 block_hash=${live_hash}"
quoted_logger_slot_9="INFO event_logger: received enforcer event event=\"block_connected\" summary=\"sidechain=9 height=123 block_hash=${live_hash}\""
payload_only_slot_9="INFO event_logger: received enforcer event event=\"block_connected\" payload=sidechain=9 summary=height=123 block_hash=${live_hash}"

logs_contain_live_event \
    "${extractor_slot_9}" "published live enforcer event" 9 "${live_hash}" ||
    die "live event matcher rejected extractor slot 9"
logs_contain_live_event \
    "${extractor_slot_98}" "published live enforcer event" 98 "${live_hash}" ||
    die "live event matcher rejected extractor slot 98"
logs_contain_live_event \
    "${logger_slot_9}" "received enforcer event" 9 "${live_hash}" ||
    die "live event matcher rejected real logger slot 9"
logs_contain_live_event \
    "${logger_slot_98}" "received enforcer event" 98 "${live_hash}" ||
    die "live event matcher rejected real logger slot 98"
logs_contain_live_event \
    "${quoted_logger_slot_9}" "received enforcer event" 9 "${live_hash}" ||
    die "live event matcher rejected quoted logger compatibility"

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
if logs_contain_live_event \
    "${payload_only_slot_9}" "received enforcer event" 9 "${live_hash}"; then
    die "live event matcher accepted sidechain data from an unrelated field"
fi

snapshot_logs="$(printf '%s\n' \
    'INFO received enforcer event event="chain_info" summary=network_mainnet' \
    'INFO received enforcer event event="chain_tip" summary=height=996259' \
    'INFO received enforcer event event="sidechain_proposals" summary=proposal_count=0' \
    'INFO received enforcer event event="active_sidechains" summary=sidechain_count=0' \
    'INFO received enforcer event event="ctip" summary=sidechain=9 present=false' \
    'INFO received enforcer event event="ctip" summary=sidechain=98 present=false' \
    'INFO received enforcer event event="block_connected" summary=sidechain=9 height=996260')"
for snapshot_kind in chain_info chain_tip sidechain_proposals active_sidechains; do
    [[ "$(count_snapshot_events "${snapshot_logs}" "${snapshot_kind}")" == 1 ]] ||
        die "semantic snapshot matcher rejected ${snapshot_kind}"
done
[[ "$(count_snapshot_events "${snapshot_logs}" ctip 9)" == 1 ]] ||
    die "semantic snapshot matcher rejected CTIP slot 9"
[[ "$(count_snapshot_events "${snapshot_logs}" ctip 98)" == 1 ]] ||
    die "semantic snapshot matcher rejected CTIP slot 98"
[[ "$(count_snapshot_events "${snapshot_logs}" ctip 8)" == 0 ]] ||
    die "semantic snapshot matcher accepted the wrong CTIP slot"
[[ "$(count_snapshot_events "${snapshot_logs}" block_connected 9)" == 1 ]] ||
    die "semantic snapshot matcher did not isolate a live event kind"

duplicate_chain_info="${snapshot_logs}"$'\nINFO received enforcer event event=chain_info summary=duplicate'
[[ "$(count_snapshot_events "${duplicate_chain_info}" chain_info)" == 2 ]] ||
    die "semantic snapshot matcher did not expose duplicate snapshot events"

logs_contain_snapshot_completion \
    'INFO sidechain_count=2 published initial enforcer snapshot' 2 ||
    die "snapshot completion matcher rejected a valid message"
if logs_contain_snapshot_completion \
    'INFO sidechain_count=1 published initial enforcer snapshot' 2; then
    die "snapshot completion matcher accepted the wrong sidechain count"
fi
if logs_contain_snapshot_completion \
    'INFO sidechain_count=2 published live enforcer event' 2; then
    die "snapshot completion matcher accepted the wrong message"
fi

[[ "$(latest_timestamp '2026-08-25T10:00:00.000000000Z' '2026-08-25T10:00:01.000000000Z')" == '2026-08-25T10:00:01.000000000Z' ]] ||
    die "latest timestamp helper selected a stale container instance"

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
jq -e '[.services[]? | select(.network_mode == "host")] | length == 0' \
    <<<"${config_json}" >/dev/null
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
jq -e --arg source "${ECASH_DATA_ROOT}/config/ecash.conf" \
    '[.services["ecash-node"].volumes[]
      | select(.source == $source and .target == "/etc/ecash/ecash.conf" and .read_only == true)]
     | length == 1' <<<"${config_json}" >/dev/null

# ecash-node is the one service that intentionally omits `user:`, because its
# image entrypoint drops to UID/GID itself. Pin that exception so it stays
# deliberate, and require the hardening the other services already carry.
jq -e '.services["ecash-node"] | has("user") | not' <<<"${config_json}" >/dev/null
jq -e --arg uid "${PUID}" --arg gid "${PGID}" \
    '.services["ecash-node"].environment | .UID == $uid and .GID == $gid' \
    <<<"${config_json}" >/dev/null
jq -e '.services["ecash-node"].security_opt
    | index("no-new-privileges:true") != null' <<<"${config_json}" >/dev/null

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
    "--network-preset=${ENFORCER_NETWORK_PRESET}" \
    "--node-rpc-addr=ecash-node:${ECASH_NODE_RPC_PORT}" \
    '--node-rpc-cookie-path=/rpc-cookie/.cookie' \
    "--node-zmq-addr-sequence=tcp://ecash-node:${ECASH_NODE_ZMQ_PORT}" \
    '--node-blocks-dir=/node-blocks' \
    '--serve-grpc-addr=0.0.0.0:50051' \
    "--bitcoin-core-expected-version=${ECASH_NODE_EXPECTED_VERSION}"; do
    jq -e --arg expected_arg "${expected_arg}" \
        '.services.enforcer.command | index($expected_arg) != null' \
        <<<"${config_json}" >/dev/null
done

for expected_arg in \
    '-conf=/etc/ecash/ecash.conf' \
    "-port=${ECASH_NODE_P2P_PORT}" \
    "-rpcport=${ECASH_NODE_RPC_PORT}"; do
    jq -e --arg expected_arg "${expected_arg}" \
        '.services["ecash-node"].command | index($expected_arg) != null' \
        <<<"${config_json}" >/dev/null
done

render_node_config "${rendered_node_config}"
grep -Fxq '# Locked network values. Generated by scripts/init.sh.' \
    "${rendered_node_config}"
grep -Fxq "# network_id=${NETWORK_ID} magic=${ECASH_NETWORK_MAGIC}" \
    "${rendered_node_config}"
grep -Fxq 'listen=0' "${rendered_node_config}"
grep -Fxq "port=${ECASH_NODE_P2P_PORT}" "${rendered_node_config}"
grep -Fxq "rpcport=${ECASH_NODE_RPC_PORT}" "${rendered_node_config}"
grep -Fxq 'rpcallowip=172.30.0.0/24' "${rendered_node_config}"
grep -Fxq 'rpccookiefile=/rpc-cookie/.cookie' "${rendered_node_config}"
grep -Fxq "zmqpubsequence=tcp://0.0.0.0:${ECASH_NODE_ZMQ_PORT}" \
    "${rendered_node_config}"
expected_peer_count=0
while IFS= read -r peer; do
    grep -Fxq "addnode=${peer}" "${rendered_node_config}"
    ((expected_peer_count += 1))
done < <(network_peers)
actual_peer_count="$(grep -c '^addnode=' "${rendered_node_config}")"
[[ "${actual_peer_count}" == "${expected_peer_count}" ]] ||
    die "rendered node configuration contains an unexpected peer"

[[ "${LOCK_FORMAT}" == 2 ]]
[[ "${ECASH_NETWORK_MAGIC}" =~ ^[[:xdigit:]]{8}$ ]]
[[ "${ECASH_ACTIVATION_BLOCK_HASH}" =~ ^[[:xdigit:]]{64}$ ]]
[[ "${ECASH_SNAPSHOT_SHA256}" =~ ^[[:xdigit:]]{64}$ ]]
[[ "${ECASH_NODE_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]]
[[ "${ENFORCER_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]]
[[ "${MONITOR_IMAGE_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]]
[[ "${ECASH_NODE_IMAGE}" == *":${ECASH_NODE_BRANCH}@sha256:"* ]]
[[ "${ENFORCER_IMAGE}" == *":sha-${ENFORCER_COMMIT:0:7}@sha256:"* ]]
[[ "${ENFORCER_EXTRACTOR_IMAGE}" == *":sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:"* ]]
[[ "${EVENT_LOGGER_IMAGE}" == *":sha-${MONITOR_IMAGE_COMMIT:0:12}@sha256:"* ]]

legacy_prefix=DRYNET
if grep -R -n --exclude-dir=data "${legacy_prefix}_" "${DEPLOYMENT_ROOT}"; then
    die "deployment still contains a legacy drynet variable"
fi

repository_root="$(git -C "${DEPLOYMENT_ROOT}" rev-parse --show-toplevel)"
if grep -Fq "hashFiles('deployments/ecash" \
    "${repository_root}/.github/workflows/ci.yml"; then
    die "deployment CI gate is guarded by hashFiles and can be skipped"
fi

# Promoting ENFORCER_IMAGE without re-vendoring proto/upstream would pair a new
# enforcer with stale client stubs. This is the offline half of that gate; CI
# also re-downloads the upstream files.
"${repository_root}/.github/scripts/check-proto-vendor.sh" --offline

info "${NETWORK_ID} eCash deployment checks passed"
