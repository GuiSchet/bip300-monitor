#!/usr/bin/env bash
# Regression checks for move_enforcer_state on a synthetic data directory.

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck source=test-harness.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/test-harness.sh"

root="$(mktemp -d)"
trap 'rm -rf -- "${root}"' EXIT
load_versions
PUID="$(id -u)"
PGID="$(id -g)"

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

# The script itself, on a copy of the deployment.
copy="$(make_deployment_copy "${root}/e2e" "${VERSIONS_FILE}")"
data="${copy}/data/${NETWORK_ID}/enforcer-official-v8"
migrate() {
    PATH="${root}/e2e/bin:${PATH}" "${copy}/scripts/migrate-enforcer-datadir.sh" ecash-betanet "$@"
}
mkdir -p "${data}/validator/bitcoin-betanet/bitcoin.mdb" "${data}/wallet/bitcoin-betanet"
printf 'lmdb\n' >"${data}/validator/bitcoin-betanet/bitcoin.mdb/data.mdb"
chmod 0750 "${data}"

# A Docker error is not "stopped": nothing is archived or moved.
STUB_PS=fail fails migrate
STUB_PS=running fails migrate
[[ -e "${data}/validator/bitcoin-betanet" && ! -e "${data}/ecash-betanet" ]] ||
    die "the migration moved state without knowing the enforcer was stopped"
[[ ! -e "${copy}/data/${NETWORK_ID}/backups" ]] ||
    die "the migration archived state without knowing the enforcer was stopped"

# The script's umask 077 must not reach the directories the enforcer enters.
STUB_PS=empty migrate >/dev/null
for created in "${data}/ecash-betanet" "${data}/ecash-betanet/mainnet"; do
    [[ "$(stat --format='%u:%g %a' "${created}")" == "${PUID}:${PGID} 750" ]] ||
        die "${created} is $(stat --format='%u:%g %a' "${created}"), not ${PUID}:${PGID} 750"
done
[[ "$(cat "${data}/ecash-betanet/mainnet/validator/bitcoin.mdb/data.mdb")" == lmdb ]] ||
    die "the script did not move the state"
compgen -G "${copy}/data/${NETWORK_ID}/backups/enforcer-layout-*.tar" >/dev/null ||
    die "the script did not archive the data directory"
STUB_PS=empty migrate --rollback >/dev/null
[[ -e "${data}/validator/bitcoin-betanet/bitcoin.mdb/data.mdb" ]] ||
    die "the script did not roll back"
STUB_PS=empty migrate >/dev/null

info "enforcer data directory migration checks passed"
