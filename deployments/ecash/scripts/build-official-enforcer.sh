#!/usr/bin/env bash
# Local build only: no registry push, no fork checkout modifications.
# Select a docker-container buildx builder via BUILDX_BUILDER for OCI export.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${root}/VERSIONS.lock"
checkout="${1:?usage: build-official-enforcer.sh OFFICIAL_CHECKOUT OUTPUT_OCI}"
output="${2:?OCI destination is required}"
[[ "$(git -C "${checkout}" rev-parse HEAD)" == "${ENFORCER_COMMIT}" ]]
[[ -z "$(git -C "${checkout}" status --porcelain --untracked-files=normal)" ]]
[[ "${ENFORCER_REPO}" == https://github.com/LayerTwo-Labs/bip300301_enforcer.git ]]
# A fresh fetch must resolve the exact reviewed official object before packaging.
git -C "${checkout}" fetch --no-tags "${ENFORCER_REPO}" "${ENFORCER_COMMIT}"
[[ "$(git -C "${checkout}" rev-parse FETCH_HEAD)" == "${ENFORCER_COMMIT}" ]]
docker buildx build --platform linux/amd64 \
    --file "${root}/images/enforcer-official.Dockerfile" \
    --build-arg "ENFORCER_COMMIT=${ENFORCER_COMMIT}" \
    --tag "docker.io/guischet/bip300-enforcer-official:sha-${ENFORCER_COMMIT:0:12}" \
    --output "type=oci,dest=${output}" "${checkout}"
