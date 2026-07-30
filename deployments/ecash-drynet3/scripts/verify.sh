#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

compose config --quiet
"${DEPLOYMENT_ROOT}/scripts/verify-core.sh"

for service in nats event-logger enforcer-extractor; do
    require_service_running "${service}"
done

wait_for_nats_health
wait_for_event_logger_subscription
wait_for_nats_client bip300-monitor-enforcer-extractor

wait_seconds="${MONITOR_EVENT_WAIT_SECONDS:-60}"
deadline="$((SECONDS + wait_seconds))"
until compose logs --no-color --tail=1000 event-logger 2>/dev/null |
    grep -Fq 'received enforcer event'; do
    ((SECONDS < deadline)) ||
        die "event logger did not receive an enforcer event after ${wait_seconds}s"
    sleep 2
done

info "Drynet3 observation pipeline verification passed"
