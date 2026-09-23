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
    --argjson snapshotHeight "${ECASH_SNAPSHOT_HEIGHT}" \
    --arg snapshotBlockHash "${ECASH_SNAPSHOT_BLOCK_HASH}" \
    --arg snapshotTransform "${ECASH_SNAPSHOT_TRANSFORM}" \
    --arg snapshotSha256 "${ECASH_SNAPSHOT_SHA256}" \
    --arg snapshotSourceSha256 "${ECASH_SNAPSHOT_SOURCE_SHA256:-}" \
    --arg snapshotSourceMagic "${ECASH_SNAPSHOT_SOURCE_NETWORK_MAGIC:-}" \
    '{network: $network, magic: $magic, activationHeight: $activationHeight,
      snapshotHeight: $snapshotHeight, snapshotBlockHash: $snapshotBlockHash,
      snapshotTransform: $snapshotTransform, snapshotSha256: $snapshotSha256,
      snapshotSourceSha256:
        (if $snapshotSourceSha256 == "" then null else $snapshotSourceSha256 end),
      snapshotSourceMagic:
        (if $snapshotSourceMagic == "" then null else $snapshotSourceMagic end)}'

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

if service_is_running postgres; then
    info "normalized monitor contract and BMM polling"
    event_contract_version="$(record_current_event_contract_version 2>/dev/null || true)"
    bmm_polling_live=false
    if record_current_run_has_bmm_observation 2>/dev/null; then
        bmm_polling_live=true
    fi
    jq -n \
        --arg event_contract_version "${event_contract_version:-unknown}" \
        --argjson bmm_polling_live "${bmm_polling_live}" \
        '{eventContractVersion: $event_contract_version,
          successfulBmmPollInCurrentRun: $bmm_polling_live}'

    info "block history coverage"
    if coverage="$(history_coverage_json 2>/dev/null)" && jq -e . <<<"${coverage}" >/dev/null; then
        jq . <<<"${coverage}"
    else
        info "history coverage is not available yet"
    fi
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
