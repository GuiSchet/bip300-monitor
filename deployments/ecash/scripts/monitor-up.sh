#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
require_command docker
require_command jq

"${DEPLOYMENT_ROOT}/scripts/verify-core.sh"

info "pulling the pinned Postgres, NATS and monitor images"
compose pull postgres nats event-logger enforcer-extractor

info "starting the Postgres record"
compose up --detach postgres
wait_for_postgres_health

info "starting Core NATS"
compose up --detach nats
wait_for_nats_health

info "starting the event logger before the publisher"
compose up --detach event-logger
wait_for_event_logger_subscription

info "starting the enforcer extractor"
compose up --detach enforcer-extractor

"${DEPLOYMENT_ROOT}/scripts/verify.sh"
