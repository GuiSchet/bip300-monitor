#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
service="${1:-}"

if [[ -n "${service}" ]]; then
    compose config --services | grep -Fxq -- "${service}" ||
        die "unknown service: ${service}"
    compose logs --follow --tail=200 "${service}"
else
    compose logs --follow --tail=200
fi
