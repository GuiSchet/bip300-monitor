#!/usr/bin/env bash
# Regression checks for move_enforcer_state on a synthetic data directory.

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

root="$(mktemp -d)"
trap 'rm -rf -- "${root}"' EXIT

fresh() {
    rm -rf -- "${root}/data"
    mkdir -p "${root}/data/validator/bitcoin-betanet/bitcoin.mdb" \
        "${root}/data/wallet/bitcoin-betanet" "${root}/data/logs"
    printf 'lmdb\n' >"${root}/data/validator/bitcoin-betanet/bitcoin.mdb/data.mdb"
    printf 'wallet\n' >"${root}/data/wallet/bitcoin-betanet/db.sqlite"
}
fails() {
    if ("$@") >/dev/null 2>&1; then
        die "expected failure: $*"
    fi
}
moved() {
    [[ "$(cat "${root}/data/ecash-betanet/mainnet/validator/bitcoin.mdb/data.mdb")" == lmdb ]] &&
        [[ "$(cat "${root}/data/ecash-betanet/mainnet/wallet/db.sqlite")" == wallet ]] &&
        [[ ! -e "${root}/data/validator/bitcoin-betanet" && ! -e "${root}/data/wallet/bitcoin-betanet" ]]
}

fresh
move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet >/dev/null
moved || die "the state did not move into the #666 layout"
move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet >/dev/null
moved || die "repeating a finished migration changed the state"

move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet --rollback >/dev/null
[[ "$(cat "${root}/data/validator/bitcoin-betanet/bitcoin.mdb/data.mdb")" == lmdb ]] ||
    die "rollback did not restore the old layout"

# An interrupted run that moved only the validator resumes with the wallet.
fresh
mkdir -p "${root}/data/ecash-betanet/mainnet"
mv -T "${root}/data/validator/bitcoin-betanet" "${root}/data/ecash-betanet/mainnet/validator"
move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet >/dev/null
moved || die "an interrupted migration did not resume"

# State in both layouts is never overwritten.
fresh
mkdir -p "${root}/data/ecash-betanet/mainnet/wallet"
fails move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet
[[ -e "${root}/data/validator/bitcoin-betanet" ]] || die "a refused migration moved part of the state"

# Nothing to migrate is an error, not a silent success.
rm -rf -- "${root}/data"
mkdir -p "${root}/data"
fails move_enforcer_state "${root}/data" bitcoin-betanet ecash-betanet/mainnet

info "enforcer data directory migration checks passed"
