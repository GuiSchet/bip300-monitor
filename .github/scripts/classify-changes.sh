#!/usr/bin/env bash

set -euo pipefail

rust=false
image_inputs=false
deployment=false
workflow=false

while IFS= read -r path; do
    case "${path}" in
        Cargo.toml | Cargo.lock | rust-toolchain.toml | build.rs | \
            extractors/* | proto/* | shared/* | tools/*)
            rust=true
            image_inputs=true
            ;;
        Dockerfile | .dockerignore | scripts/container-smoke.sh)
            image_inputs=true
            ;;
        deployments/*)
            deployment=true
            ;;
        .github/workflows/* | .github/scripts/*)
            workflow=true
            ;;
    esac
done

{
    printf 'rust=%s\n' "${rust}"
    printf 'image_inputs=%s\n' "${image_inputs}"
    printf 'deployment=%s\n' "${deployment}"
    printf 'workflow=%s\n' "${workflow}"
} >>"${GITHUB_OUTPUT}"
