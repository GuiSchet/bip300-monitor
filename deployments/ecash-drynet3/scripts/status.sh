#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose ps
require_service_running ecash-node

info "blockchain"
node_cli getblockchaininfo |
    jq '{chain, blocks, headers, verificationprogress, initialblockdownload}'

info "chainstates"
node_cli getchainstates | jq '{headers, chainstates}'

info "peers"
node_cli getpeerinfo |
    jq '[.[] | {addr, inbound, startingheight, synced_headers, synced_blocks}]'

if ! service_is_running enforcer; then
    info "enforcer is not running; start it with 'just enforcer-up' after the node is ready"
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
