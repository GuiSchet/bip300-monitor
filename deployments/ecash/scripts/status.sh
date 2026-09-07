#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

# The magic is recorded for provenance, not verified: no RPC exposes it. The
# activation block below is what actually proves which network this node is on.
info "locked deployment (magic is descriptive)"
jq -n \
    --arg network "${NETWORK_ID}" \
    --arg magic "${ECASH_NETWORK_MAGIC}" \
    --argjson activationHeight "${ECASH_ACTIVATION_HEIGHT}" \
    '{network: $network, magic: $magic, activationHeight: $activationHeight}'

compose ps
require_service_running ecash-node

info "blockchain"
node_cli getblockchaininfo |
    jq '{chain, blocks, headers, verificationprogress, initialblockdownload}'

info "chainstates"
chainstates="$(node_cli getchainstates)"
jq '{headers, chainstates}' <<<"${chainstates}"

info "AssumeUTXO readiness"
history_fully_validated=false
trusted_snapshot_ready=false
monitoring_ready=false
if node_history_is_fully_validated "${chainstates}"; then
    history_fully_validated=true
fi
if node_trusted_snapshot_is_ready "${chainstates}"; then
    trusted_snapshot_ready=true
fi
if node_monitoring_is_ready "${chainstates}"; then
    monitoring_ready=true
fi
jq \
    --argjson history_fully_validated "${history_fully_validated}" \
    --argjson trusted_snapshot_ready "${trusted_snapshot_ready}" \
    --argjson monitoring_ready "${monitoring_ready}" \
    --argjson snapshot_trust_enabled "${TRUST_ASSUMEUTXO_SNAPSHOT}" '{
    history_ready: $history_fully_validated,
    history_fully_validated: $history_fully_validated,
    snapshot_trust_enabled: $snapshot_trust_enabled,
    trusted_snapshot_ready: $trusted_snapshot_ready,
    monitoring_ready: $monitoring_ready,
    chainstate_count: (.chainstates | length),
    historical_blocks: (.chainstates[0].blocks // null),
    active_blocks: (.chainstates[-1].blocks // null),
    remaining_blocks: (
        if (.chainstates | length) > 1 then
            .chainstates[-1].blocks - .chainstates[0].blocks
        else
            0
        end
    )
}' <<<"${chainstates}"

info "peers"
node_cli getpeerinfo |
    jq '[.[] | {addr, inbound, startingheight, synced_headers, synced_blocks}]'

if ! service_is_running enforcer; then
    if [[ "${monitoring_ready}" == true ]]; then
        info "enforcer is not running; the node passed the configured readiness policy, so start it with 'just enforcer-up'"
    else
        info "enforcer is not running; wait for the pinned snapshot or full history validation before 'just enforcer-up'"
    fi
    exit 0
fi

info "enforcer network parameters"
if chain_info="$(enforcer_rpc GetChainInfo)"; then
    jq '{network, bip300Constants}' <<<"${chain_info}"
else
    info "enforcer RPC is not ready yet"
    exit 0
fi

info "enforcer tip"
if chain_tip="$(enforcer_rpc GetChainTip)"; then
    jq '{blockHeaderInfo}' <<<"${chain_tip}"
else
    info "enforcer validator has not established a chain tip yet"
fi

if ! service_is_running nats; then
    info "observation pipeline is not running; start it with 'just monitor-up'"
    exit 0
fi

info "Core NATS health"
if health="$(nats_monitor /healthz 2>/dev/null)" &&
    jq -e '.status' <<<"${health}" >/dev/null; then
    jq '{status}' <<<"${health}"
else
    info "Core NATS monitoring is temporarily unavailable"
fi

info "monitor NATS clients"
if connections="$(nats_monitor /connz 2>/dev/null)" &&
    jq -e '.connections' <<<"${connections}" >/dev/null; then
    jq '[.connections[] | select(.name | startswith("bip300-monitor-")) | {name, subscriptions, in_msgs, out_msgs}]' \
        <<<"${connections}"
else
    info "NATS client details are temporarily unavailable"
fi

info "enforcer event subscriptions"
if subscriptions="$(nats_monitor '/subsz?subs=true' 2>/dev/null)" &&
    jq -e '.subscriptions_list' <<<"${subscriptions}" >/dev/null; then
    jq '[.subscriptions_list[] | select(.subject == "bip300.enforcer") | {account, subject, msgs}]' \
        <<<"${subscriptions}"
else
    info "NATS subscription details are temporarily unavailable"
fi
