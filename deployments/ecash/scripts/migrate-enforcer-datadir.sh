#!/usr/bin/env bash
# Moves the enforcer state into the layout of bip300301_enforcer#666
# (<fork>/<chain>), so an enforcer started with --fork resumes from its height
# instead of syncing again. Run it with the enforcer and the extractor stopped,
# before starting the #666 image. It archives the data directory first.
# --rollback restores the old layout for the previous image.

set -euo pipefail
umask 077

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env

fork="${1:?usage: migrate-enforcer-datadir.sh ecash-betanet|ecash-alphanet [--rollback]}"
mode="${2:-}"
# eCash mainnet starts with an empty data directory: only the preset networks
# have state in the old layout.
[[ "${fork}" =~ ^ecash-(alphanet|betanet)$ ]] || die "no old layout exists for ${fork}"
[[ -z "${mode}" || "${mode}" == --rollback ]] || die "unknown option ${mode}"
! service_is_running enforcer || die "stop the enforcer first"
! service_is_running enforcer-extractor || die "stop the enforcer extractor first"

dir="$(enforcer_data_dir)"
[[ -d "${dir}" ]] || die "missing ${dir}"
# Up to 1753fc0 a preset named the chain directory <chain>-<preset>, the main
# chain being "bitcoin"; #666 uses <fork>/<chain>, the main chain being "mainnet".
legacy="bitcoin-${fork#ecash-}"
target="${fork}/mainnet"

if [[ -z "${mode}" ]]; then
    archive="$(data_root)/backups/enforcer-layout-$(date -u +%Y%m%dT%H%M%SZ).tar"
    mkdir -p -- "$(dirname -- "${archive}")"
    tar --create --file "${archive}" --directory "$(dirname -- "${dir}")" "$(basename -- "${dir}")"
    info "archived ${dir} to ${archive}"
fi
move_enforcer_state "${dir}" "${legacy}" "${target}" "${mode}"
info "enforcer state is in the $([[ -z "${mode}" ]] && printf '%s' "${target}" || printf 'old') layout"
