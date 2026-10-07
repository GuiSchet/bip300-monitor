#!/usr/bin/env bash

set -euo pipefail

DEPLOYMENT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DEPLOYMENT_ROOT
readonly VERSIONS_FILE="${DEPLOYMENT_ROOT}/VERSIONS.lock"

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

info() {
    printf '==> %s\n' "$*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    [[ "${value}" =~ ^[0-9]+$ ]] || die "${name} must be numeric"
    ((value > 0)) || die "${name} must be greater than zero"
}

require_boolean() {
    local name="$1"
    local value="$2"
    [[ "${value}" == true || "${value}" == false ]] ||
        die "${name} must be true or false"
}

require_ready_release() {
    [[ "${RELEASE_STATUS:-}" == ready ]] || die "release is not marked ready; this candidate cannot be started"
    [[ "${ENFORCER_REPO}" == https://github.com/LayerTwo-Labs/bip300301_enforcer.git ]] || die "enforcer must use official upstream"
    [[ "${ENFORCER_COMMIT}" == "${ENFORCER_UPSTREAM_REVIEWED_COMMIT}" ]] || die "official enforcer commit mismatch"
}

load_versions() {
    [[ -f "${VERSIONS_FILE}" ]] || die "missing ${VERSIONS_FILE}"
    if grep -qE '^[A-Z][A-Z0-9_]*=REPLACE_WITH_' "${VERSIONS_FILE}"; then
        die "${VERSIONS_FILE} contains unresolved promotion placeholders"
    fi
    # This file is tracked in the repository and contains assignments only.
    # shellcheck disable=SC1090
    source "${VERSIONS_FILE}"

    : "${LOCK_FORMAT:?LOCK_FORMAT must be locked}"
    : "${NETWORK_ID:?NETWORK_ID must be locked}"
    : "${ECASH_NODE_CHAIN:?ECASH_NODE_CHAIN must be locked}"
    : "${ECASH_NETWORK_MAGIC:?ECASH_NETWORK_MAGIC must be locked}"
    : "${ECASH_PUBLIC_PEERS:?ECASH_PUBLIC_PEERS must be locked}"
    : "${ECASH_NODE_P2P_PORT:?ECASH_NODE_P2P_PORT must be locked}"
    : "${ECASH_NODE_RPC_PORT:?ECASH_NODE_RPC_PORT must be locked}"
    : "${ECASH_NODE_ZMQ_PORT:?ECASH_NODE_ZMQ_PORT must be locked}"
    : "${ECASH_NODE_EXPECTED_VERSION:?ECASH_NODE_EXPECTED_VERSION must be locked}"
    : "${ECASH_TIP_HEIGHT_URL:?ECASH_TIP_HEIGHT_URL must be locked}"
    : "${ECASH_EXPECTED_DATA_BYTES:?ECASH_EXPECTED_DATA_BYTES must be locked}"
    : "${ECASH_MIN_FREE_PERCENT:?ECASH_MIN_FREE_PERCENT must be locked}"
    : "${ECASH_ACTIVATION_HEIGHT:?ECASH_ACTIVATION_HEIGHT must be locked}"
    : "${ECASH_ACTIVATION_BLOCK_HASH:?ECASH_ACTIVATION_BLOCK_HASH must be locked}"
    : "${ECASH_SNAPSHOT_FILE:?ECASH_SNAPSHOT_FILE must be locked}"
    : "${ECASH_SNAPSHOT_HEIGHT:?ECASH_SNAPSHOT_HEIGHT must be locked}"
    : "${ECASH_SNAPSHOT_BLOCK_HASH:?ECASH_SNAPSHOT_BLOCK_HASH must be locked}"
    : "${ECASH_SNAPSHOT_UTXO_HASH:?ECASH_SNAPSHOT_UTXO_HASH must be locked}"
    : "${ECASH_SNAPSHOT_CHAIN_TX_COUNT:?ECASH_SNAPSHOT_CHAIN_TX_COUNT must be locked}"
    : "${ECASH_SNAPSHOT_TRANSFORM:?ECASH_SNAPSHOT_TRANSFORM must be locked}"
    : "${ECASH_SNAPSHOT_URL:?ECASH_SNAPSHOT_URL must be locked}"
    : "${ECASH_SNAPSHOT_SIZE:?ECASH_SNAPSHOT_SIZE must be locked}"
    : "${ECASH_SNAPSHOT_SHA256:?ECASH_SNAPSHOT_SHA256 must be locked}"
    : "${ENFORCER_NETWORK_PRESET:?ENFORCER_NETWORK_PRESET must be locked}"
    : "${ENFORCER_API_NETWORK:?ENFORCER_API_NETWORK must be locked}"
    : "${ENFORCER_UPSTREAM_REVIEWED_COMMIT:?ENFORCER_UPSTREAM_REVIEWED_COMMIT must be locked}"
    : "${MONITOR_EVENT_CONTRACT_VERSION:?MONITOR_EVENT_CONTRACT_VERSION must be locked}"
    : "${POSTGRES_IMAGE:?POSTGRES_IMAGE must be locked}"
    : "${POSTGRES_DB:?POSTGRES_DB must be locked}"
    : "${POSTGRES_USER:?POSTGRES_USER must be locked}"

    [[ "${LOCK_FORMAT}" == 5 ]] || die "unsupported LOCK_FORMAT=${LOCK_FORMAT}"
    [[ "${NETWORK_ID}" =~ ^[a-z0-9][a-z0-9-]*$ ]] ||
        die "NETWORK_ID has an invalid format"
    # Both end up unquoted inside psql invocations and the healthcheck.
    [[ "${POSTGRES_DB}" =~ ^[a-z_][a-z0-9_]*$ ]] ||
        die "POSTGRES_DB has an invalid format"
    [[ "${POSTGRES_USER}" =~ ^[a-z_][a-z0-9_]*$ ]] ||
        die "POSTGRES_USER has an invalid format"
    [[ "${ECASH_ACTIVATION_BLOCK_HASH}" =~ ^[[:xdigit:]]{64}$ ]] ||
        die "ECASH_ACTIVATION_BLOCK_HASH must contain 64 hexadecimal characters"
    [[ "${ECASH_SNAPSHOT_BLOCK_HASH}" =~ ^[[:xdigit:]]{64}$ ]] ||
        die "ECASH_SNAPSHOT_BLOCK_HASH must contain 64 hexadecimal characters"
    [[ "${ECASH_SNAPSHOT_UTXO_HASH}" =~ ^[[:xdigit:]]{64}$ ]] ||
        die "ECASH_SNAPSHOT_UTXO_HASH must contain 64 hexadecimal characters"
    [[ "${ECASH_SNAPSHOT_SHA256}" =~ ^[[:xdigit:]]{64}$ ]] ||
        die "ECASH_SNAPSHOT_SHA256 must contain 64 hexadecimal characters"
    [[ "${ENFORCER_UPSTREAM_REVIEWED_COMMIT}" =~ ^[[:xdigit:]]{40}$ ]] ||
        die "ENFORCER_UPSTREAM_REVIEWED_COMMIT must contain 40 hexadecimal characters"
    case "${ECASH_SNAPSHOT_TRANSFORM}" in
    none)
        : "${ECASH_SNAPSHOT_CHECKSUMS_URL:?ECASH_SNAPSHOT_CHECKSUMS_URL must be locked for a direct snapshot}"
        ;;
    network_magic_v2)
        : "${ECASH_SNAPSHOT_MIRROR_URL:?ECASH_SNAPSHOT_MIRROR_URL must be locked for a transformed snapshot}"
        : "${ECASH_SNAPSHOT_SOURCE_FILE:?ECASH_SNAPSHOT_SOURCE_FILE must be locked for a transformed snapshot}"
        : "${ECASH_SNAPSHOT_SOURCE_SIZE:?ECASH_SNAPSHOT_SOURCE_SIZE must be locked for a transformed snapshot}"
        : "${ECASH_SNAPSHOT_SOURCE_SHA256:?ECASH_SNAPSHOT_SOURCE_SHA256 must be locked for a transformed snapshot}"
        : "${ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC:?ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC must be locked for a transformed snapshot}"
        [[ "${ECASH_SNAPSHOT_SOURCE_FILE}" != "${ECASH_SNAPSHOT_FILE}" ]] ||
            die "source and transformed snapshot filenames must differ"
        [[ "${ECASH_SNAPSHOT_SOURCE_SHA256}" =~ ^[[:xdigit:]]{64}$ ]] ||
            die "ECASH_SNAPSHOT_SOURCE_SHA256 must contain 64 hexadecimal characters"
        [[ "${ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC}" =~ ^[[:xdigit:]]{8}$ ]] ||
            die "ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC must contain 8 hexadecimal characters"
        require_positive_integer ECASH_SNAPSHOT_SOURCE_SIZE "${ECASH_SNAPSHOT_SOURCE_SIZE}"
        [[ "${ECASH_SNAPSHOT_SOURCE_SIZE}" == "${ECASH_SNAPSHOT_SIZE}" ]] ||
            die "a network-magic rewrite must preserve the snapshot size"
        ;;
    *)
        die "unsupported ECASH_SNAPSHOT_TRANSFORM=${ECASH_SNAPSHOT_TRANSFORM}"
        ;;
    esac
    require_positive_integer ECASH_NODE_P2P_PORT "${ECASH_NODE_P2P_PORT}"
    require_positive_integer ECASH_NODE_RPC_PORT "${ECASH_NODE_RPC_PORT}"
    require_positive_integer ECASH_NODE_ZMQ_PORT "${ECASH_NODE_ZMQ_PORT}"
    require_positive_integer ECASH_NODE_EXPECTED_VERSION "${ECASH_NODE_EXPECTED_VERSION}"
    require_positive_integer ECASH_EXPECTED_DATA_BYTES "${ECASH_EXPECTED_DATA_BYTES}"
    require_positive_integer ECASH_MIN_FREE_PERCENT "${ECASH_MIN_FREE_PERCENT}"
    require_positive_integer ECASH_ACTIVATION_HEIGHT "${ECASH_ACTIVATION_HEIGHT}"
    require_positive_integer ECASH_SNAPSHOT_HEIGHT "${ECASH_SNAPSHOT_HEIGHT}"
    require_positive_integer ECASH_SNAPSHOT_CHAIN_TX_COUNT "${ECASH_SNAPSHOT_CHAIN_TX_COUNT}"
    require_positive_integer ECASH_SNAPSHOT_SIZE "${ECASH_SNAPSHOT_SIZE}"
    require_positive_integer MONITOR_EVENT_CONTRACT_VERSION "${MONITOR_EVENT_CONTRACT_VERSION}"
    ((ECASH_MIN_FREE_PERCENT < 100)) ||
        die "ECASH_MIN_FREE_PERCENT must be less than 100"
    ((ECASH_SNAPSHOT_HEIGHT <= ECASH_ACTIVATION_HEIGHT)) ||
        die "ECASH_SNAPSHOT_HEIGHT must not be after ECASH_ACTIVATION_HEIGHT; otherwise activation-era block bodies can be skipped"
    network_peers >/dev/null
}

network_peers() {
    local host
    local peer
    local port
    local -a peers

    IFS=',' read -r -a peers <<<"${ECASH_PUBLIC_PEERS}"
    ((${#peers[@]} > 0)) || die "ECASH_PUBLIC_PEERS must not be empty"
    for peer in "${peers[@]}"; do
        [[ "${peer}" != *[[:space:]]* && "${peer}" == *:* ]] ||
            die "invalid locked peer: ${peer}"
        host="${peer%:*}"
        port="${peer##*:}"
        [[ "${host}" =~ ^[a-zA-Z0-9.-]+$ ]] ||
            die "invalid locked peer hostname: ${host}"
        [[ "${port}" == "${ECASH_NODE_P2P_PORT}" ]] ||
            die "locked peer ${peer} does not use ECASH_NODE_P2P_PORT=${ECASH_NODE_P2P_PORT}"
        printf '%s\n' "${peer}"
    done
}

render_node_config() {
    local destination="$1"
    local peer
    local temporary_config
    local auth_config="${BIP300_NODE_OBSERVER_AUTH_CONFIG_FILE:-$(data_root)/secrets/node-observer-rpcauth}"
    local template="${DEPLOYMENT_ROOT}/config/ecash.conf.template"

    [[ -f "${template}" ]] || die "missing node configuration template: ${template}"
    temporary_config="$(mktemp "${destination}.XXXXXX")"
    if ! cp "${template}" "${temporary_config}"; then
        rm -f -- "${temporary_config}"
        die "could not copy the node configuration template"
    fi
    {
        printf '\n# Locked network values. Generated by scripts/init.sh.\n'
        printf '# network_id=%s magic=%s\n' "${NETWORK_ID}" "${ECASH_NETWORK_MAGIC}"
        printf 'port=%s\n' "${ECASH_NODE_P2P_PORT}"
        printf 'rpcport=%s\n' "${ECASH_NODE_RPC_PORT}"
        # The enforcer requires historical block data from its last persisted
        # cursor. Make the non-pruned requirement explicit instead of relying
        # on the node default, which could change or be overridden unnoticed.
        printf 'prune=0\n'
        [[ -f "${auth_config}" ]] || die "missing dedicated observer rpcauth; run just init"
        cat -- "${auth_config}"
        printf 'zmqpubsequence=tcp://0.0.0.0:%s\n' "${ECASH_NODE_ZMQ_PORT}"
        while IFS= read -r peer; do
            printf 'addnode=%s\n' "${peer}"
        done < <(network_peers)
    } >>"${temporary_config}"
    # The node bind-mounts this exact file, so the destination inode has to
    # survive. A replacing rename would leave the running container reading the
    # previous file while the host shows the new one.
    if ! cat -- "${temporary_config}" >"${destination}"; then
        rm -f -- "${temporary_config}"
        die "could not write the node configuration: ${destination}"
    fi
    rm -f -- "${temporary_config}"
    chmod 0644 "${destination}"
}

normalize_assignment_key() {
    local key="$1"

    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    printf '%s\n' "${key}"
}

reject_version_overrides() {
    local env_file="$1"
    local -A locked_keys=()
    local key

    while IFS='=' read -r key _; do
        key="$(normalize_assignment_key "${key}")"
        [[ "${key}" =~ ^[A-Z][A-Z0-9_]*$ ]] || continue
        locked_keys["${key}"]=1
    done <"${VERSIONS_FILE}"

    while IFS='=' read -r key _; do
        key="$(normalize_assignment_key "${key}")"
        [[ "${key}" =~ ^[A-Z][A-Z0-9_]*$ ]] || continue
        [[ -z "${locked_keys[${key}]+present}" ]] ||
            die "${env_file} must not override locked variable ${key}"
        case "${key}" in
        COMPOSE_PROJECT_NAME | ECASH_DATA_ROOT)
            die "${env_file} must not set derived variable ${key}"
            ;;
        esac
    done <"${env_file}"
}

deployment_env_file() {
    printf '%s\n' "${COMPOSE_ENV_FILE:-${DEPLOYMENT_ROOT}/.env}"
}

load_deployment_env() {
    local bmm_poll_interval_seconds
    local env_file
    local resolved_data_base
    local stream_stall_timeout_seconds
    local tip_poll_interval_seconds
    env_file="$(deployment_env_file)"
    [[ -f "${env_file}" ]] || die "missing ${env_file}; run 'just init' first"
    reject_version_overrides "${env_file}"

    set -a
    # The local file is created from the repository's simple KEY=VALUE example.
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a

    : "${ECASH_DATA_BASE:?ECASH_DATA_BASE must be set}"
    : "${PUID:?PUID must be set}"
    : "${PGID:?PGID must be set}"
    [[ "${PUID}" =~ ^[0-9]+$ ]] || die "PUID must be numeric"
    [[ "${PGID}" =~ ^[0-9]+$ ]] || die "PGID must be numeric"
    require_positive_integer \
        ENFORCER_SYNC_WAIT_SECONDS "${ENFORCER_SYNC_WAIT_SECONDS:-300}"
    require_positive_integer \
        SNAPSHOT_RPC_WAIT_SECONDS "${SNAPSHOT_RPC_WAIT_SECONDS:-43200}"
    require_positive_integer \
        SNAPSHOT_HEADER_WAIT_SECONDS "${SNAPSHOT_HEADER_WAIT_SECONDS:-1800}"
    require_positive_integer \
        SNAPSHOT_ACTIVATION_WAIT_SECONDS "${SNAPSHOT_ACTIVATION_WAIT_SECONDS:-86400}"
    require_positive_integer \
        MONITOR_STARTUP_WAIT_SECONDS "${MONITOR_STARTUP_WAIT_SECONDS:-60}"
    require_positive_integer \
        MONITOR_EVENT_WAIT_SECONDS "${MONITOR_EVENT_WAIT_SECONDS:-60}"
    require_positive_integer \
        BMM_REQUEST_WAIT_SECONDS "${BMM_REQUEST_WAIT_SECONDS:-30}"
    require_positive_integer \
        WORKER_HEALTH_WAIT_SECONDS "${WORKER_HEALTH_WAIT_SECONDS:-60}"
    require_positive_integer \
        LIVE_BLOCK_WAIT_SECONDS "${LIVE_BLOCK_WAIT_SECONDS:-3600}"
    require_positive_integer \
        LIVE_EVENT_WAIT_SECONDS "${LIVE_EVENT_WAIT_SECONDS:-60}"
    require_positive_integer \
        HISTORY_WAIT_SECONDS "${HISTORY_WAIT_SECONDS:-43200}"
    require_positive_integer \
        BIP300_MONITOR_REQUEST_TIMEOUT_SECONDS \
        "${BIP300_MONITOR_REQUEST_TIMEOUT_SECONDS:-120}"
    require_positive_integer \
        BIP300_MONITOR_BACKFILL_PAGE_BLOCKS \
        "${BIP300_MONITOR_BACKFILL_PAGE_BLOCKS:-128}"
    ((${BIP300_MONITOR_BACKFILL_PAGE_BLOCKS:-128} <= 512)) ||
        die "BIP300_MONITOR_BACKFILL_PAGE_BLOCKS must not exceed 512"
    [[ "${BIP300_MONITOR_BACKFILL_PAGE_PAUSE_MS:-100}" =~ ^[0-9]+$ ]] ||
        die "BIP300_MONITOR_BACKFILL_PAGE_PAUSE_MS must be numeric"
    tip_poll_interval_seconds="${BIP300_MONITOR_TIP_POLL_INTERVAL_SECONDS:-30}"
    bmm_poll_interval_seconds="${BIP300_MONITOR_BMM_REQUEST_POLL_INTERVAL_SECONDS:-5}"
    stream_stall_timeout_seconds="${BIP300_MONITOR_STREAM_STALL_TIMEOUT_SECONDS:-60}"
    require_positive_integer \
        BIP300_MONITOR_TIP_POLL_INTERVAL_SECONDS \
        "${tip_poll_interval_seconds}"
    require_positive_integer \
        BIP300_MONITOR_BMM_REQUEST_POLL_INTERVAL_SECONDS \
        "${bmm_poll_interval_seconds}"
    ((bmm_poll_interval_seconds <= 60)) ||
        die "BIP300_MONITOR_BMM_REQUEST_POLL_INTERVAL_SECONDS must not exceed 60"
    require_positive_integer \
        BIP300_MONITOR_STREAM_STALL_TIMEOUT_SECONDS \
        "${stream_stall_timeout_seconds}"
    ((stream_stall_timeout_seconds >= tip_poll_interval_seconds)) ||
        die "BIP300_MONITOR_STREAM_STALL_TIMEOUT_SECONDS must be at least BIP300_MONITOR_TIP_POLL_INTERVAL_SECONDS"
    if [[ -n "${BIP300_MONITOR_BACKFILL_MAX_BLOCKS+x}" ]]; then
        die "BIP300_MONITOR_BACKFILL_MAX_BLOCKS was removed; replace it with BIP300_MONITOR_BACKFILL_PAGE_BLOCKS"
    fi
    TRUST_ASSUMEUTXO_SNAPSHOT="${TRUST_ASSUMEUTXO_SNAPSHOT:-false}"
    require_boolean TRUST_ASSUMEUTXO_SNAPSHOT "${TRUST_ASSUMEUTXO_SNAPSHOT}"
    export TRUST_ASSUMEUTXO_SNAPSHOT

    # Restore every repository pin after loading operator-owned configuration.
    load_versions

    if [[ "${ECASH_DATA_BASE}" == /* ]]; then
        resolved_data_base="$(realpath -m -- "${ECASH_DATA_BASE}")"
    else
        resolved_data_base="$(realpath -m -- "${DEPLOYMENT_ROOT}/${ECASH_DATA_BASE}")"
    fi
    [[ "${resolved_data_base}" != / ]] || die "ECASH_DATA_BASE must not resolve to /"
    COMPOSE_PROJECT_NAME="bip300-ecash-${NETWORK_ID}"
    ECASH_DATA_ROOT="$(realpath -m -- "${resolved_data_base}/${NETWORK_ID}")"
    export COMPOSE_PROJECT_NAME ECASH_DATA_ROOT
}

data_root() {
    : "${ECASH_DATA_ROOT:?load deployment environment before resolving its data root}"
    printf '%s\n' "${ECASH_DATA_ROOT}"
}

# Remove reproducible transform inputs only after the caller has verified that
# the promoted destination exists and is valid.
cleanup_transformed_snapshot_source() {
    local snapshot_path="$1"
    local source_path="$2"
    local source_partial_path="$3"

    [[ -f "${snapshot_path}" ]] || return 1
    rm -f -- "${source_path}" "${source_partial_path}"
}

compose() {
    local env_file
    env_file="$(deployment_env_file)"
    docker compose \
        --env-file "${env_file}" \
        --env-file "${VERSIONS_FILE}" \
        --file "${DEPLOYMENT_ROOT}/compose.yaml" \
        "$@"
}

node_cli() {
    compose exec -T ecash-node \
        bitcoin-cli \
        -datadir=/data \
        -conf=/etc/ecash/ecash.conf \
        -rpcport="${ECASH_NODE_RPC_PORT}" \
        "$@"
}

service_is_running() {
    [[ -n "$(compose ps --status running --quiet "$1")" ]]
}

require_service_running() {
    service_is_running "$1" || die "$1 is not running"
}

service_started_at() {
    local container_id
    container_id="$(compose ps --status running --quiet "$1")"
    [[ -n "${container_id}" ]] || die "$1 is not running"
    docker inspect --format '{{.State.StartedAt}}' "${container_id}"
}

enforcer_mempool_tracking_is_enabled() {
    local command_json
    local container_id
    local enforcer_logs

    container_id="$(compose ps --status running --quiet enforcer)"
    [[ -n "${container_id}" ]] || return 1
    command_json="$(docker inspect --format '{{json .Config.Cmd}}' "${container_id}")" || return 1
    jq -e 'index("--enable-mempool") != null' <<<"${command_json}" >/dev/null || return 1
    enforcer_logs="$(compose logs --no-color enforcer 2>/dev/null)" || return 1
    grep -Fq 'mempool sync task w/validator: starting' <<<"${enforcer_logs}"
}

enforcer_rpc() {
    local method="$1"
    compose exec -T enforcer \
        curl --fail-with-body --silent --show-error --max-time 30 \
        --request POST \
        --header 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:50051/cusf.mainchain.v1.ValidatorService/${method}"
}

# Active sidechains as `slot activation_height`, sorted by slot. The validator
# is the authority for both values; a deployment-local list would go stale on
# the next activation.
active_sidechain_activations() {
    local response
    response="$(enforcer_rpc GetSidechains)" || return 1
    jq -r '
        if (.sidechains | type) != "array" then
            error("GetSidechains response has no sidechains array")
        else
            [.sidechains[]
             | {slot: (.sidechainNumber | tonumber),
                height: (.activationHeight | tonumber)}]
            | sort_by(.slot)
            | unique_by(.slot)
            | .[]
            | "\(.slot) \(.height)"
        end' <<<"${response}"
}

nats_monitor() {
    local endpoint="$1"
    compose exec -T nats \
        wget -qO- "http://127.0.0.1:8222${endpoint}"
}

nats_is_healthy() {
    local health
    health="$(nats_monitor /healthz 2>/dev/null)" &&
        jq -e '.status == "ok"' <<<"${health}" >/dev/null
}

nats_has_client() {
    local client_name="$1"
    local connections
    connections="$(nats_monitor /connz 2>/dev/null)" &&
        jq -e --arg client_name "${client_name}" \
            '.connections | any(.name == $client_name)' \
            <<<"${connections}" >/dev/null
}

nats_has_enforcer_subscription() {
    local subscriptions
    subscriptions="$(nats_monitor '/subsz?subs=true' 2>/dev/null)" &&
        jq -e \
            '.subscriptions_list | any(.account == "$G" and .subject == "bip300.*")' \
            <<<"${subscriptions}" >/dev/null
}

wait_for_nats_health() {
    local wait_seconds="${MONITOR_STARTUP_WAIT_SECONDS:-60}"
    local deadline="$((SECONDS + wait_seconds))"
    until nats_is_healthy; do
        ((SECONDS < deadline)) ||
            die "Core NATS was not healthy after ${wait_seconds}s"
        sleep 2
    done
}

wait_for_event_logger_subscription() {
    local wait_seconds="${MONITOR_STARTUP_WAIT_SECONDS:-60}"
    local deadline="$((SECONDS + wait_seconds))"
    until nats_has_client bip300-monitor-event-logger &&
        nats_has_enforcer_subscription; do
        ((SECONDS < deadline)) ||
            die "event logger subscription was not ready after ${wait_seconds}s"
        sleep 2
    done
}

# The statement arrives on stdin rather than through `--command` because psql
# does not interpolate `:'variable'` in a `--command` string. Passing values as
# psql variables is what keeps a shell value from being spliced into SQL.
postgres_query() {
    local statement="$1"
    shift

    compose exec -T postgres \
        psql --username="${POSTGRES_USER}" --dbname="${POSTGRES_DB}" \
        --no-align --tuples-only --quiet --set=ON_ERROR_STOP=1 "$@" <<<"${statement}"
}

postgres_is_healthy() {
    compose exec -T postgres \
        pg_isready --host=127.0.0.1 \
        --username="${POSTGRES_USER}" --dbname="${POSTGRES_DB}" >/dev/null 2>&1
}

wait_for_postgres_health() {
    local wait_seconds="${MONITOR_STARTUP_WAIT_SECONDS:-60}"
    local deadline="$((SECONDS + wait_seconds))"
    until postgres_is_healthy; do
        ((SECONDS < deadline)) ||
            die "Postgres was not ready after ${wait_seconds}s"
        sleep 2
    done
}

wait_for_nats_client() {
    local client_name="$1"
    local wait_seconds="${MONITOR_STARTUP_WAIT_SECONDS:-60}"
    local deadline="$((SECONDS + wait_seconds))"
    until nats_has_client "${client_name}"; do
        ((SECONDS < deadline)) ||
            die "NATS client ${client_name} was not connected after ${wait_seconds}s"
        sleep 2
    done
}

line_contains_sidechain() {
    local line="$1"
    local sidechain="$2"
    local slot_pattern

    [[ "${sidechain}" =~ ^[0-9]+$ ]] || return 1
    slot_pattern="(^|[[:space:]])(summary=\"?)?sidechain=${sidechain}([^0-9]|$)"
    [[ "${line}" =~ ${slot_pattern} ]]
}

logs_contain_live_event() {
    local logs="$1"
    local message="$2"
    local sidechain="$3"
    local block_hash="$4"
    local line

    [[ "${sidechain}" =~ ^[0-9]+$ ]] || return 1

    while IFS= read -r line; do
        if [[ "${line}" == *"${message}"* ]] &&
            line_contains_sidechain "${line}" "${sidechain}" &&
            [[ "${line}" == *"${block_hash}"* ]]; then
            return 0
        fi
    done <<<"${logs}"
    return 1
}

logs_contain_snapshot_completion() {
    local logs="$1"
    local sidechain_count="$2"
    local count_pattern
    local line

    [[ "${sidechain_count}" =~ ^[0-9]+$ ]] || return 1
    count_pattern="(^|[[:space:]])desired_sidechain_count=${sidechain_count}([^0-9]|$)"
    while IFS= read -r line; do
        if [[ "${line}" == *"published initial enforcer snapshot"* &&
            "${line}" =~ ${count_pattern} ]]; then
            return 0
        fi
    done <<<"${logs}"
    return 1
}

valid_event_kind() {
    local kind="$1"

    [[ "${kind}" =~ ^[a-z][a-z0-9_]*$ ]]
}

count_snapshot_events() {
    local logs="$1"
    local kind="$2"
    local sidechain="${3:-}"
    local count=0
    local event_pattern
    local line

    valid_event_kind "${kind}" || return 1
    event_pattern="(^|[[:space:]])event=\"?${kind}\"?([[:space:]]|$)"
    if [[ -n "${sidechain}" ]]; then
        [[ "${sidechain}" =~ ^[0-9]+$ ]] || return 1
    fi

    while IFS= read -r line; do
        [[ "${line}" == *"received enforcer event"* ]] || continue
        [[ "${line}" =~ ${event_pattern} ]] || continue
        if [[ -n "${sidechain}" ]] &&
            ! line_contains_sidechain "${line}" "${sidechain}"; then
            continue
        fi
        ((count += 1))
    done <<<"${logs}"
    printf '%s\n' "${count}"
}

# State facts have no block anchor. Verify their occurrences in one read window
# of the scoped current run. Deduplication never erases a fresh observation.
record_snapshot_group() {
    record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT o.snapshot_group_id FROM event_observation o JOIN scope s USING(dataset_id,run_id)
        JOIN event e ON e.id=o.event_id AND e.event_contract_version=s.event_contract_version
        WHERE e.source='enforcer' AND e.kind='active_sidechains' AND o.snapshot_group_id IS NOT NULL
        ORDER BY o.capture_seq DESC LIMIT 1"
}
record_snapshot_height() {
    local group="$1"
    record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT g.tip_before_height FROM snapshot_group g JOIN scope s USING(dataset_id,run_id)
        WHERE g.snapshot_group_id=:'group'::uuid" --set=group="${group}"
}
record_snapshot_sidechain_activations() {
    local group="$1"
    record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT (x->>'sidechain_number') || ' ' || (x->>'activation_height')
        FROM event_observation o JOIN scope s USING(dataset_id,run_id)
        JOIN event e ON e.id=o.event_id AND e.event_contract_version=s.event_contract_version
        CROSS JOIN LATERAL jsonb_array_elements(e.payload #> '{monitor_event,Enforcer,event,ActiveSidechains,sidechains}') x
        WHERE e.source='enforcer' AND e.kind='active_sidechains' AND o.snapshot_group_id=:'group'::uuid
        ORDER BY (x->>'sidechain_number')::integer" --set=group="${group}"
}
record_current_state_is_usable() {
    local kind="$1" sidechain="${2:-}" result
    valid_event_kind "${kind}" || return 1
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql)), latest AS (
        SELECT g.* FROM event_observation o JOIN scope s USING(dataset_id,run_id)
        JOIN event e ON e.id=o.event_id AND e.dataset_id=s.dataset_id AND e.event_contract_version=s.event_contract_version
        JOIN snapshot_group g ON g.snapshot_group_id=o.snapshot_group_id AND g.dataset_id=s.dataset_id AND g.run_id=s.run_id
        WHERE e.source='enforcer' AND e.kind=:'kind'
        AND e.sidechain IS NOT DISTINCT FROM NULLIF(:'slot','')::smallint
        AND (e.sidechain IS NULL OR EXISTS(SELECT 1 FROM current_sidechain_instance i
            WHERE i.dataset_id=e.dataset_id AND i.sidechain=e.sidechain AND i.sidechain_instance_id=e.sidechain_instance_id))
        ORDER BY o.capture_seq DESC LIMIT 1)
        SELECT EXISTS(SELECT 1 FROM latest WHERE consistency='tip_matched'
        AND revision_before IS NULL AND revision_after IS NULL
        AND tip_before_hash=tip_after_hash AND tip_before_height=tip_after_height)" \
        --set=kind="${kind}" --set=slot="${sidechain}")" || return 1
    [[ "${result}" == t ]]
}

# Whether the newest running enforcer extractor has completed at least one
# successful BMM poll. Empty request lists are facts too, so this checks the
# observation occurrence instead of requiring a non-empty request array.
record_current_run_has_bmm_observation() {
    local result

    result="$(
        postgres_query \
            "WITH current_run AS (
                 SELECT run_id, dataset_id, event_contract_version
                   FROM extractor_run
                  WHERE source = 'enforcer' AND status = 'running'
                  ORDER BY started_at DESC, run_id DESC
                  LIMIT 1
             )
             SELECT EXISTS (
                 SELECT 1
                   FROM current_run
                   JOIN event_observation observation
                     ON observation.run_id = current_run.run_id
                    AND observation.dataset_id = current_run.dataset_id
                   JOIN event
                     ON event.id = observation.event_id
                    AND event.dataset_id = current_run.dataset_id
                    AND event.event_contract_version = current_run.event_contract_version
                  WHERE observation.capture_method = 'poll'
                    AND event.source = 'enforcer'
                    AND event.kind = 'bmm_requests'
             )"
    )" || return 1
    [[ "${result}" == t ]]
}

# Matching tips describe a read window, not atomic state or mempool readiness.
record_current_run_has_tip_matched_bmm_observation() {
    local result

    result="$(
        postgres_query \
            "WITH current_run AS (
                 SELECT run_id, dataset_id, event_contract_version
                   FROM extractor_run
                  WHERE source = 'enforcer' AND status = 'running'
                  ORDER BY started_at DESC, run_id DESC
                  LIMIT 1
             )
             SELECT EXISTS (
                 SELECT 1
                   FROM current_run
                   JOIN event_observation observation
                     ON observation.run_id = current_run.run_id
                    AND observation.dataset_id = current_run.dataset_id
                   JOIN snapshot_group snapshot
                     ON snapshot.snapshot_group_id = observation.snapshot_group_id
                    AND snapshot.run_id = current_run.run_id
                   JOIN event
                     ON event.id = observation.event_id
                    AND event.dataset_id = current_run.dataset_id
                    AND event.event_contract_version = current_run.event_contract_version
                  WHERE observation.capture_method = 'poll'
                    AND event.source = 'enforcer'
                    AND event.kind = 'bmm_requests'
                    AND snapshot.consistency = 'tip_matched'
                    AND snapshot.revision_before IS NULL AND snapshot.revision_after IS NULL
                    AND snapshot.tip_before_height = snapshot.tip_after_height
                    AND snapshot.tip_before_height = event.height
                    AND snapshot.tip_before_hash = snapshot.tip_after_hash
                    AND snapshot.tip_before_hash = event.block_hash
                    AND decode(
                        event.payload #>> '{monitor_event,Enforcer,event,BmmRequests,previous_mainchain_block_hash}',
                        'hex'
                    ) = event.block_hash
             )"
    )" || return 1
    [[ "${result}" == t ]]
}

record_has_single_running_enforcer() {
    local result

    result="$(
        postgres_query \
            "WITH current_dataset AS (
                 SELECT dataset_id
                   FROM extractor_run
                  WHERE source = 'enforcer'
                  ORDER BY started_at DESC, run_id DESC
                  LIMIT 1
             )
             SELECT count(*) = 1
               FROM extractor_run
              WHERE source = 'enforcer'
                AND dataset_id = (SELECT dataset_id FROM current_dataset)
                AND status = 'running'"
    )" || return 1
    [[ "${result}" == t ]]
}

record_current_dataset_created_at() {
    postgres_query \
        "SELECT manifest.created_at
           FROM extractor_run run
           JOIN dataset_manifest manifest USING (dataset_id)
          WHERE run.source = 'enforcer' AND run.status = 'running'
          ORDER BY run.started_at DESC, run.run_id DESC
          LIMIT 1"
}

record_current_event_contract_version() {
    postgres_query \
        "SELECT event_contract_version
           FROM extractor_run
          WHERE source = 'enforcer' AND status = 'running'
          ORDER BY started_at DESC, run_id DESC
          LIMIT 1"
}

record_current_run_capabilities() {
    postgres_query \
        "SELECT capabilities::text
           FROM extractor_run
          WHERE source = 'enforcer' AND status = 'running'
          ORDER BY started_at DESC, run_id DESC
          LIMIT 1"
}

record_current_worker_status_json() {
    postgres_query \
        "WITH current_run AS (
             SELECT run_id
               FROM extractor_run
              WHERE source = 'enforcer' AND status = 'running'
              ORDER BY started_at DESC, run_id DESC
              LIMIT 1
         )
         SELECT COALESCE(
             jsonb_agg(
                 jsonb_build_object(
                     'worker', worker.worker,
                     'consecutive_failures', worker.consecutive_failures,
                     'last_error', worker.last_error,
                     'last_success_at', worker.last_success_at,
                     'last_failure_at', worker.last_failure_at,
                     'updated_at', worker.updated_at
                 ) ORDER BY worker.worker
             ),
             '[]'::jsonb
         )
           FROM current_run
           JOIN extractor_worker_status worker USING (run_id)"
}

record_current_workers_are_healthy() {
    local result
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT count(*) FILTER (WHERE w.worker IN ('mainchain_tip','bmm_requests','mainchain_events','enforcer_state'))=4
          AND bool_and(w.last_success_at IS NOT NULL AND w.last_error IS NULL)
          FROM scope s JOIN extractor_worker_status w USING(run_id)")" || return 1
    [[ "${result}" == t ]]
}

record_node_worker_is_healthy() {
    local RECORD_SOURCE=node result
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT count(*)=1 AND bool_and(w.last_success_at IS NOT NULL AND w.last_error IS NULL)
        FROM scope s JOIN extractor_worker_status w USING(run_id) WHERE w.worker='node_history'")" || return 1
    [[ "${result}" == t ]]
}

# All verification uses the locked deployment identity and its current run.
record_scope_sql() {
    cat <<'SQL'
SELECT d.dataset_id, r.event_contract_version, r.run_id
  FROM dataset_manifest d JOIN extractor_status x USING(dataset_id)
  JOIN extractor_run r ON r.run_id=x.run_id
 WHERE d.network_id=:'network' AND d.activation_height=:'network_activation'::integer
   AND d.activation_block_hash=:'network_activation_hash'
   AND d.initial_event_contract_version=:'contract'::integer
   AND r.event_contract_version=:'contract'::integer
   AND x.source=:'source' AND r.source=:'source' AND r.status='running'
SQL
}

record_scoped_query() {
    postgres_query "$@" \
        --set=network="${NETWORK_ID}" \
        --set=network_activation="${ECASH_ACTIVATION_HEIGHT}" \
        --set=network_activation_hash="${ECASH_ACTIVATION_BLOCK_HASH}" \
        --set=contract="${MONITOR_EVENT_CONTRACT_VERSION}" \
        --set=source="${RECORD_SOURCE:-enforcer}"
}

record_has_block() {
    local kind="$1" sidechain="$2" block_hash="$3" result
    valid_event_kind "${kind}" || return 1
    [[ "${sidechain}" =~ ^[0-9]+$ && "${block_hash}" =~ ^[[:xdigit:]]{64}$ ]] || return 1
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT count(*)=1 FROM event e JOIN scope s USING(dataset_id,event_contract_version)
        JOIN current_sidechain_instance i USING(dataset_id,sidechain,sidechain_instance_id)
        WHERE e.source='enforcer' AND e.kind=:'kind' AND e.sidechain=:'slot'::smallint
          AND e.block_hash=decode(:'hash','hex')" \
        --set=kind="${kind}" --set=slot="${sidechain}" --set=hash="${block_hash}")" || return 1
    [[ "${result}" == t ]]
}

record_has_global_block() {
    local kind="$1" block_hash="$2" result
    local RECORD_SOURCE=enforcer
    [[ "${kind}" != mainchain_block ]] || RECORD_SOURCE=node
    valid_event_kind "${kind}" || return 1
    [[ "${block_hash}" =~ ^[[:xdigit:]]{64}$ ]] || return 1
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT count(*)=1 FROM event e JOIN scope s USING(dataset_id,event_contract_version)
        WHERE e.source=:'source' AND e.kind=:'kind' AND e.sidechain IS NULL
          AND e.sidechain_instance_id IS NULL AND e.block_hash=decode(:'hash','hex')" \
        --set=kind="${kind}" --set=hash="${block_hash}")" || return 1
    [[ "${result}" == t ]]
}

# Prove an actual live global connect in this verification window, including
# deployments with zero active sidechain slots.
record_has_live_mainchain_connect() {
    local block_hash="$1" since="$2" result
    [[ "${block_hash}" =~ ^[[:xdigit:]]{64}$ ]] || return 1
    result="$(record_scoped_query "WITH scope AS ($(record_scope_sql))
        SELECT EXISTS(SELECT 1 FROM event e JOIN scope s USING(dataset_id,event_contract_version)
        JOIN event_observation o ON o.event_id=e.id AND o.dataset_id=s.dataset_id AND o.run_id=s.run_id
        WHERE e.source='enforcer' AND e.kind='mainchain_transition'
          AND e.payload #>> '{monitor_event,Enforcer,event,MainchainTransition,action}'='1'
          AND e.block_hash=decode(:'hash','hex') AND o.capture_method='live'
          AND o.observed_at>=:'since'::timestamptz)" \
        --set=hash="${block_hash}" --set=since="${since}")" || return 1
    [[ "${result}" == t ]]
}

record_history_is_complete() {
    local stream="$1" sidechain="$2" activation="$3" minimum="$4"
    local height canonical_hash activation_hash result
    local RECORD_SOURCE=enforcer
    [[ "${stream}" != mainchain_block ]] || RECORD_SOURCE=node
    [[ "${activation}" =~ ^[0-9]+$ && "${minimum}" =~ ^[0-9]+$ ]] || return 1
    [[ "${stream}" == block || "${stream}" == mainchain_block ]] || return 1
    [[ -z "${sidechain}" || "${sidechain}" =~ ^[0-9]+$ ]] || return 1
    local coverage_sql="SELECT c.* FROM history_coverage c
        JOIN scope s USING(dataset_id,event_contract_version)
        WHERE c.source=:'source' AND c.stream=:'stream'
          AND c.sidechain IS NOT DISTINCT FROM NULLIF(:'slot','')::smallint
          AND (c.sidechain IS NULL OR EXISTS(SELECT 1 FROM current_sidechain_instance i
              WHERE (i.dataset_id,i.sidechain,i.sidechain_instance_id)=
                    (c.dataset_id,c.sidechain,c.sidechain_instance_id)))
          AND c.status='complete' AND c.next_hash IS NULL
          AND c.covered_tip_hash=c.target_tip_hash AND c.covered_tip_height=c.target_tip_height
          AND c.coverage_start_height=:'activation'::integer
          AND c.covered_tip_height>=:'minimum'::integer"
    height="$(record_scoped_query "WITH scope AS ($(record_scope_sql)), coverage AS (${coverage_sql})
        SELECT covered_tip_height FROM coverage" \
        --set=stream="${stream}" --set=slot="${sidechain}" --set=activation="${activation}" --set=minimum="${minimum}")" || return 1
    [[ "${height}" =~ ^[0-9]+$ ]] || return 1
    canonical_hash="$(node_cli getblockhash "${height}")" || return 1
    activation_hash="$(node_cli getblockhash "${activation}")" || return 1
    result="$(record_scoped_query "WITH RECURSIVE scope AS ($(record_scope_sql)),
        coverage AS (${coverage_sql}), facts AS NOT MATERIALIZED (
            SELECT e.* FROM event e JOIN coverage c ON
                (e.dataset_id,e.event_contract_version,e.source)=(c.dataset_id,c.event_contract_version,c.source)
                AND e.sidechain IS NOT DISTINCT FROM c.sidechain
                AND e.sidechain_instance_id IS NOT DISTINCT FROM c.sidechain_instance_id
            WHERE e.kind=CASE :'stream' WHEN 'block' THEN 'block_connected' ELSE 'mainchain_block' END
        ), path AS (
            SELECT e.id,e.block_hash,e.previous_hash,e.height FROM facts e JOIN coverage c
                ON e.block_hash=c.covered_tip_hash AND e.height=c.covered_tip_height
                AND e.block_hash=decode(:'canonical','hex')
            UNION ALL
            SELECT e.id,e.block_hash,e.previous_hash,e.height FROM path p JOIN facts e
                ON e.block_hash=p.previous_hash AND e.height=p.height-1
                WHERE p.height>:'activation'::integer
        ) SELECT count(*)=:'height'::bigint-:'activation'::bigint+1
            AND count(*)=count(DISTINCT height)
            AND bool_or(height=:'activation'::integer AND block_hash=decode(:'activation_hash','hex'))
            AND NOT EXISTS(SELECT 1 FROM event_conflict c JOIN path p
                ON p.id=c.first_event_id OR p.id=c.conflicting_event_id)
            FROM path" \
        --set=stream="${stream}" --set=slot="${sidechain}" --set=activation="${activation}" --set=minimum="${minimum}" \
        --set=height="${height}" --set=canonical="${canonical_hash}" --set=activation_hash="${activation_hash}")" || return 1
    [[ "${result}" == t ]]
}

record_node_history_is_complete() {
    record_history_is_complete mainchain_block '' "$1" "$2"
}

record_block_history_is_complete() {
    record_history_is_complete block "$1" "$2" "$3"
}

history_coverage_json() {
    postgres_query \
        "SELECT COALESCE(jsonb_agg(row ORDER BY (row->>'sidechain')::integer), '[]'::jsonb)
           FROM (
             SELECT jsonb_build_object(
                 'source', coverage.source,
                 'stream', coverage.stream,
                 'sidechain', coverage.sidechain,
                 'sidechain_instance_id', coverage.sidechain_instance_id,
                 'event_contract_version', coverage.event_contract_version,
                 'status', coverage.status,
                 'start_height', coverage.coverage_start_height,
                 'target_height', coverage.target_tip_height,
                 'next_height', coverage.next_height,
                 'covered_height', coverage.covered_tip_height,
                 'rows_recorded', coverage.rows_recorded,
                 'page_blocks', coverage.effective_page_blocks,
                 'percent', CASE
                     WHEN coverage.status = 'complete' THEN 100
                     ELSE round(
                         100.0 * (coverage.target_tip_height - coverage.next_height)
                         / GREATEST(
                             coverage.target_tip_height - coverage.coverage_start_height + 1,
                             1
                         ),
                         2
                     )
                 END,
                 'last_error', coverage.last_error,
                 'updated_at', coverage.updated_at
             ) AS row
               FROM history_coverage coverage
               JOIN extractor_status extractor
                 ON extractor.dataset_id = coverage.dataset_id
                AND extractor.source = coverage.source
               JOIN extractor_run run
                 ON run.run_id = extractor.run_id
              WHERE coverage.source IN ('enforcer','node')
                AND coverage.event_contract_version = run.event_contract_version
           ) coverage_rows"
}

# The extractor republishes a snapshot kind whenever a block changes it, so a
# verification window can legitimately hold more than one of them. Only the
# kinds that are published exactly once may be counted exactly.
has_snapshot_event() {
    local count

    count="$(count_snapshot_events "$@")" || return 1
    [[ "${count}" =~ ^[0-9]+$ ]] || return 1
    ((count >= 1))
}

normalize_timestamp() {
    local timestamp="$1"
    local fraction
    local prefix

    # Docker renders StartedAt with Go's RFC3339Nano, which strips trailing
    # zeros. Pad the fraction back to a fixed width so two instants can be
    # compared as plain strings.
    if [[ "${timestamp}" =~ ^([0-9-]+T[0-9:]+)(\.([0-9]+))?Z$ ]]; then
        prefix="${BASH_REMATCH[1]}"
        fraction="${BASH_REMATCH[3]}"
        while ((${#fraction} < 9)); do
            fraction="${fraction}0"
        done
        printf '%s.%sZ\n' "${prefix}" "${fraction}"
        return 0
    fi
    printf '%s\n' "${timestamp}"
}

latest_timestamp() {
    local first="$1"
    local second="$2"
    if [[ "$(normalize_timestamp "${first}")" > "$(normalize_timestamp "${second}")" ]]; then
        printf '%s\n' "${first}"
    else
        printf '%s\n' "${second}"
    fi
}

require_activation_header() {
    local header
    header="$(node_cli getblockheader "${ECASH_ACTIVATION_BLOCK_HASH}")" ||
        die "${NETWORK_ID} activation header is unavailable"
    jq -e \
        --arg hash "${ECASH_ACTIVATION_BLOCK_HASH}" \
        --argjson height "${ECASH_ACTIVATION_HEIGHT}" \
        '.hash == $hash and .height == $height' <<<"${header}" >/dev/null ||
        die "unexpected ${NETWORK_ID} activation header"
}

require_activation_block() {
    local activation_hash
    activation_hash="$(node_cli getblockhash "${ECASH_ACTIVATION_HEIGHT}")" ||
        die "${NETWORK_ID} activation block is unavailable"
    [[ "${activation_hash}" == "${ECASH_ACTIVATION_BLOCK_HASH}" ]] ||
        die "unexpected block at height ${ECASH_ACTIVATION_HEIGHT}: ${activation_hash}"
}

require_activation_block_data() {
    node_cli getblock "${ECASH_ACTIVATION_BLOCK_HASH}" 0 >/dev/null 2>&1 ||
        die "raw ${NETWORK_ID} activation block ${ECASH_ACTIVATION_BLOCK_HASH} is unavailable"
}

# AssumeUTXO needs only the base UTXO set to follow the active tip, but the
# observer reads raw BIP300 messages beginning at activation. Fetch that one
# block explicitly when a fresh snapshot chainstate does not yet have it.
ensure_activation_block_data() {
    local deadline
    local last_request_seconds
    local peer_id
    local wait_seconds="${SNAPSHOT_ACTIVATION_WAIT_SECONDS:-86400}"

    if node_cli getblock "${ECASH_ACTIVATION_BLOCK_HASH}" 0 >/dev/null 2>&1; then
        return 0
    fi
    deadline="$((SECONDS + wait_seconds))"
    last_request_seconds="$((SECONDS - 60))"
    info "waiting up to ${wait_seconds}s for raw ${NETWORK_ID} activation block data"
    until node_cli getblock "${ECASH_ACTIVATION_BLOCK_HASH}" 0 >/dev/null 2>&1; do
        ((SECONDS < deadline)) ||
            die "activation block body was not received within ${wait_seconds}s"
        peer_id="$(
            node_cli getpeerinfo 2>/dev/null |
                jq -er --argjson height "${ECASH_ACTIVATION_HEIGHT}" '
                    [.[] | select(.inbound == false and (.synced_blocks // -1) >= $height)]
                    | first.id
                ' 2>/dev/null
        )" || peer_id=
        if [[ -n "${peer_id}" ]] && ((SECONDS - last_request_seconds >= 60)); then
            info "requesting raw activation block ${ECASH_ACTIVATION_BLOCK_HASH} from peer ${peer_id}"
            node_cli getblockfrompeer \
                "${ECASH_ACTIVATION_BLOCK_HASH}" "${peer_id}" >/dev/null 2>&1 || true
            last_request_seconds="${SECONDS}"
        fi
        sleep 10
    done
}

snapshot_v2_header_hex() {
    local path="$1"

    od -An -tx1 -N11 -- "${path}" | tr -d '[:space:]'
}

# Rewrite only the four network-magic bytes in a version-2 UTXO snapshot.
# The caller separately verifies source and destination size/SHA-256. Comparing
# both the prefix and everything after the metadata magic makes this helper
# fail if any other byte changes during the copy or rewrite.
rewrite_snapshot_network_magic_v2() {
    local destination="$2"
    local expected_destination_header
    local expected_source_header
    local source="$1"
    local source_magic="$3"
    local target_magic="$4"

    [[ "${source_magic}" =~ ^[[:xdigit:]]{8}$ ]] ||
        die "source snapshot network magic must contain 8 hexadecimal characters"
    [[ "${target_magic}" =~ ^[[:xdigit:]]{8}$ ]] ||
        die "target snapshot network magic must contain 8 hexadecimal characters"
    expected_source_header="7574786fff0200${source_magic,,}"
    expected_destination_header="7574786fff0200${target_magic,,}"
    [[ "$(snapshot_v2_header_hex "${source}")" == "${expected_source_header}" ]] ||
        die "source snapshot is not version 2 with network magic ${source_magic}"

    cp --reflink=auto -- "${source}" "${destination}"
    printf '%b' \
        "\\x${target_magic:0:2}\\x${target_magic:2:2}\\x${target_magic:4:2}\\x${target_magic:6:2}" |
        dd of="${destination}" bs=1 seek=7 conv=notrunc status=none

    [[ "$(snapshot_v2_header_hex "${destination}")" == "${expected_destination_header}" ]] ||
        die "transformed snapshot does not contain network magic ${target_magic}"
    cmp --silent --bytes=7 -- "${source}" "${destination}" ||
        die "snapshot transformation changed bytes before the network magic"
    cmp --silent --ignore-initial=11 -- "${source}" "${destination}" ||
        die "snapshot transformation changed bytes after the network magic"
}

require_rpc_cookie_readable() {
    local cookie
    local cookie_gid
    local cookie_mode
    local cookie_uid
    local mode

    cookie="$(data_root)/rpc-cookie/.cookie"
    [[ -f "${cookie}" ]] ||
        die "the node RPC cookie does not exist yet: ${cookie}; wait for ecash-node to finish starting"

    read -r cookie_uid cookie_gid cookie_mode < <(
        stat --format='%u %g %a' "${cookie}"
    )
    mode="$((8#${cookie_mode}))"

    if [[ "${cookie_uid}" == "${PUID}" ]] && ((mode & 0400)); then
        return 0
    fi
    if [[ "${cookie_gid}" == "${PGID}" ]] && ((mode & 0040)); then
        return 0
    fi
    if ((mode & 0004)); then
        return 0
    fi

    die "the node RPC cookie ${cookie} is owned by ${cookie_uid}:${cookie_gid} with mode ${cookie_mode}, but the enforcer runs as ${PUID}:${PGID} and cannot read it; the node image did not honour UID/GID"
}

# The extractor runs as 10001:${PGID} and reads the Postgres password from a
# bind mount. Nothing else notices when it cannot: the container just dies during
# startup with a connection error that names the wrong cause. The sibling check
# above exists for the node cookie for exactly this reason.
require_postgres_secret_readable() {
    local mode
    local secret
    local secret_gid
    local secret_mode
    local secret_uid

    secret="$(data_root)/secrets/postgres-password"
    [[ -f "${secret}" ]] ||
        die "the Postgres password does not exist yet: ${secret}; run \`just init\` first"

    read -r secret_uid secret_gid secret_mode < <(
        stat --format='%u %g %a' "${secret}"
    )
    mode="$((8#${secret_mode}))"

    # The extractor has its own UID, so only the group and other bits can help
    # it. PUID is still accepted because a run as that user reads its own file.
    if [[ "${secret_uid}" == "${PUID}" ]] && ((mode & 0400)); then
        return 0
    fi
    if [[ "${secret_gid}" == "${PGID}" ]] && ((mode & 0040)); then
        return 0
    fi
    if ((mode & 0004)); then
        return 0
    fi

    die "the Postgres password ${secret} is owned by ${secret_uid}:${secret_gid} with mode ${secret_mode}, but the extractor runs as 10001:${PGID} and cannot read it; re-run \`just init\` or chgrp it to ${PGID}"
}

# The Compose file and VERSIONS.lock describe one system and have to move
# together: Compose says the extractor records to Postgres and refreshes a
# liveness file, and only an image built from code that does both can honour
# that. A pin left behind starts containers that ignore the Postgres settings
# entirely and never go healthy, which reads as a broken deployment rather than
# as a forgotten promotion.
#
# Deliberately here and not in static-check.sh: that one runs in CI on every pull
# request, and pull requests structurally cannot publish images, so the same
# assertion there would fail every branch that touches the monitor.
require_pinned_images_current() {
    local changed
    local short="${MONITOR_IMAGE_COMMIT:0:12}"

    # A deployment unpacked from a tarball has no history to compare against.
    git -C "${DEPLOYMENT_ROOT}" rev-parse --git-dir >/dev/null 2>&1 || return 0

    git -C "${DEPLOYMENT_ROOT}" cat-file -e "${MONITOR_IMAGE_COMMIT}^{commit}" 2>/dev/null ||
        die "MONITOR_IMAGE_COMMIT ${short} is not a commit in this checkout; fetch it or correct VERSIONS.lock"
    git -C "${DEPLOYMENT_ROOT}" merge-base --is-ancestor "${MONITOR_IMAGE_COMMIT}" HEAD ||
        die "the pinned monitor images are from ${short}, which is not an ancestor of this checkout; deploy from a tree that contains what is pinned"

    # `:/` and `:(top,glob)` anchor each pathspec to the repository root. Keep
    # provenance documentation out of this check: CI rebuilds images only when
    # a protobuf definition changes, not when proto/upstream/README.md changes.
    changed="$(
        git -C "${DEPLOYMENT_ROOT}" rev-list --count \
            "${MONITOR_IMAGE_COMMIT}..HEAD" -- :/shared :/extractors :/tools \
            ':(top,glob)proto/**/*.proto'
    )"
    ((changed == 0)) ||
        die "the pinned monitor images are from ${short}, but ${changed} commits have changed the monitor sources since; merge to main, let CI publish, and promote the digests in VERSIONS.lock before deploying"
}

require_node_peer() {
    local peer_count
    peer_count="$(node_cli getconnectioncount)" ||
        die "ecash-node did not return a peer count"
    [[ "${peer_count}" =~ ^[0-9]+$ ]] ||
        die "ecash-node returned an invalid peer count: ${peer_count}"
    ((peer_count > 0)) || die "ecash-node has no ${NETWORK_ID} peers"
}

require_node_ready() {
    local blockchain_info
    require_service_running ecash-node
    blockchain_info="$(node_cli getblockchaininfo)"

    jq -e --arg chain "${ECASH_NODE_CHAIN}" '.chain == $chain' \
        <<<"${blockchain_info}" >/dev/null ||
        die "ecash-node is not using the expected ${NETWORK_ID} chain"
    jq -e '.pruned == false' <<<"${blockchain_info}" >/dev/null ||
        die "ecash-node has pruning enabled; the enforcer requires complete historical block data"
    jq -e --argjson height "${ECASH_ACTIVATION_HEIGHT}" \
        '.blocks >= $height and .headers >= $height' \
        <<<"${blockchain_info}" >/dev/null ||
        die "ecash-node has not reached ${NETWORK_ID} activation height ${ECASH_ACTIVATION_HEIGHT}"
    jq -e '.initialblockdownload == false' <<<"${blockchain_info}" >/dev/null ||
        die "ecash-node is still in initial block download"

    require_activation_block
    require_activation_block_data
    require_node_peer
}

node_history_is_fully_validated() {
    local chainstates="${1:-}"

    if [[ -z "${chainstates}" ]]; then
        chainstates="$(node_cli getchainstates 2>/dev/null)" || return 1
    fi

    jq -e --argjson activation_height "${ECASH_ACTIVATION_HEIGHT}" '
        (.chainstates | type == "array")
        and (.chainstates | length == 1)
        and (.chainstates[0].validated == true)
        and (.chainstates[0].blocks >= $activation_height)
    ' <<<"${chainstates}" >/dev/null 2>&1
}

node_trusted_snapshot_is_ready() {
    local chainstates="${1:-}"

    [[ "${TRUST_ASSUMEUTXO_SNAPSHOT:-false}" == true ]] || return 1
    if [[ -z "${chainstates}" ]]; then
        chainstates="$(node_cli getchainstates 2>/dev/null)" || return 1
    fi

    # loadtxoutset only activates a snapshot after the node has deserialized it
    # and matched its UTXO hash against the commitment compiled into the pinned
    # node image. Tie that active state back to the separately pinned snapshot
    # base before allowing monitoring to start ahead of the historical replay.
    jq -e \
        --arg snapshot_hash "${ECASH_SNAPSHOT_BLOCK_HASH}" \
        --argjson snapshot_height "${ECASH_SNAPSHOT_HEIGHT}" \
        --argjson activation_height "${ECASH_ACTIVATION_HEIGHT}" '
        (.headers | type == "number")
        and (.chainstates | type == "array")
        and (.chainstates | length == 2)
        and ((.chainstates[0] | has("snapshot_blockhash")) | not)
        and (.chainstates[0].validated == true)
        and (.chainstates[-1].snapshot_blockhash == $snapshot_hash)
        and (.chainstates[-1].blocks >= $snapshot_height)
        and (.chainstates[-1].blocks >= $activation_height)
        and (.headers >= .chainstates[-1].blocks)
    ' <<<"${chainstates}" >/dev/null 2>&1
}

node_monitoring_is_ready() {
    local chainstates="${1:-}"

    if [[ -z "${chainstates}" ]]; then
        chainstates="$(node_cli getchainstates 2>/dev/null)" || return 1
    fi
    node_history_is_fully_validated "${chainstates}" ||
        node_trusted_snapshot_is_ready "${chainstates}"
}

# Kept as a compatibility entrypoint for the VM bootstrap installed before the
# trusted-snapshot policy existed. "Ready" now means ready for monitoring; use
# node_history_is_fully_validated when the completed historical replay matters.
node_history_is_ready() {
    node_monitoring_is_ready "$@"
}

require_node_monitoring_ready() {
    local active_blocks
    local chainstate_count
    local chainstates
    local historical_blocks

    chainstates="$(node_cli getchainstates)" ||
        die "ecash-node did not return chainstate information"
    jq -e '.chainstates | type == "array" and length >= 1' \
        <<<"${chainstates}" >/dev/null ||
        die "ecash-node returned no usable chainstate"

    if node_history_is_fully_validated "${chainstates}"; then
        return 0
    fi
    if node_trusted_snapshot_is_ready "${chainstates}"; then
        info "accepting the pinned AssumeUTXO snapshot at ${ECASH_SNAPSHOT_BLOCK_HASH}; historical validation continues in the background"
        return 0
    fi

    chainstate_count="$(jq -r '.chainstates | length' <<<"${chainstates}")"
    historical_blocks="$(jq -r '.chainstates[0].blocks // "unavailable"' <<<"${chainstates}")"
    active_blocks="$(jq -r '.chainstates[-1].blocks // "unavailable"' <<<"${chainstates}")"
    die "ecash-node is not ready for monitoring (chainstates=${chainstate_count}, historical_blocks=${historical_blocks}, active_blocks=${active_blocks}); require one fully validated chainstate or set TRUST_ASSUMEUTXO_SNAPSHOT=true and load the pinned snapshot at ${ECASH_SNAPSHOT_BLOCK_HASH}"
}

# A cutover archive is valid only while this exact writer remains stopped.
# Preserve Docker's full timestamp; second-resolution file mtimes can miss a
# stop/start/stop within the same second.
frozen_writer_identity() {
    local container identity
    container="$(compose ps -a -q enforcer-extractor)"
    [[ "$container" =~ ^[a-f0-9]{12,64}$ ]] || die 'expected exactly one stopped extractor container'
    identity="$(docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.State.Running}}' "$container")"
    [[ "$identity" == *' false' ]] || die 'extractor must be stopped before freezing its identity'
    printf '%s\n' "$identity"
}
