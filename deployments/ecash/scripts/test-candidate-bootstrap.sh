#!/usr/bin/env bash
# A new network's node syncs from its candidate lock before the fork, while
# the enforcer and the monitor keep refusing that lock.

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck source=test-harness.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/test-harness.sh"

root="$(mktemp -d)"
trap 'rm -rf -- "${root}"' EXIT

# A candidate whatever the mainnet reference's own state: once it is ready, the
# values pinned at launch go back to placeholders here.
candidate="${root}/candidate.lock"
sed 's/^RELEASE_STATUS=.*/RELEASE_STATUS=candidate/' \
    "${DEPLOYMENT_ROOT}/VERSIONS.mainnet.lock.example" >"${candidate}"
for field in "${NODE_BOOTSTRAP_FIELDS[@]}" ECASH_SNAPSHOT_SHA256; do
    sed -i "s/^${field}=.*/${field}=REPLACE_WITH_TEST/" "${candidate}"
done
fails() {
    if ("$@") >"${root}/out" 2>&1; then
        die "expected failure: $*"
    fi
}
run() {
    PATH="${root}/bin:${PATH}" "${copy}/scripts/$1"
}
# 'just up' hands the snapshot load to a transient unit; record it instead.
mkdir -p -- "${root}/bin"
cat >"${root}/bin/systemd-run" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"${root}/systemd-run.log"
STUB
cat >"${root}/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
[[ "$1" == is-active ]] || exit 0
printf '%s\n' "${STUB_LOADER:-inactive}"
[[ "${STUB_LOADER:-inactive}" == active ]]
STUB
chmod +x -- "${root}/bin/systemd-run" "${root}/bin/systemctl"

# The node's own pins, the snapshot hash among them, must be complete.
sed 's/^ECASH_SNAPSHOT_SHA256=REPLACE_WITH_.*/ECASH_SNAPSHOT_SHA256='"$(printf 'a%.0s' {1..64})"'/' \
    "${candidate}" >"${root}/node-pinned.lock"
copy="$(make_deployment_copy "${root}" "${root}/node-pinned.lock")"

run init.sh >/dev/null
run up.sh >/dev/null
for started in 'pull ecash-node' 'up --detach ecash-node'; do
    grep -qx "compose .* ${started}" "${root}/docker.log" ||
        die "up.sh did not run '${started}' from the candidate lock"
done
! grep -E 'compose .* (pull|up)( |$)' "${root}/docker.log" | grep -qvE ' ecash-node$' ||
    die "up.sh started more than the node from the candidate lock"
grep -q -- "--unit=bip300-snapshot-mainnet .*/scripts/snapshot.sh$" "${root}/systemd-run.log" ||
    die "up.sh did not start the snapshot loader"

# A second 'just up' sees the running load instead of starting another.
STUB_LOADER=active run up.sh >"${root}/out" 2>&1
[[ "$(wc -l <"${root}/systemd-run.log")" -eq 1 ]] ||
    die "up.sh started a second snapshot loader"
grep -q 'snapshot loader bip300-snapshot-mainnet is already running' "${root}/out" ||
    die "up.sh did not report the running snapshot loader: $(cat "${root}/out")"

# The operator can keep the load manual. static-check exports its own env
# file, so write the opt-out to a copy of whichever one is in use.
cp -- "${COMPOSE_ENV_FILE:-${copy}/.env}" "${root}/manual.env"
printf '\nSNAPSHOT_ON_UP=false\n' >>"${root}/manual.env"
COMPOSE_ENV_FILE="${root}/manual.env" run up.sh >/dev/null
[[ "$(wc -l <"${root}/systemd-run.log")" -eq 1 ]] ||
    die "up.sh started the snapshot loader with SNAPSHOT_ON_UP=false"
if [[ -n "${REAL_DOCKER}" ]]; then
    bash -c 'source "$1/scripts/lib.sh"; allow_node_bootstrap; load_versions; load_deployment_env; compose config --format json' \
        _ "${copy}" | jq -e '.services.enforcer.image | startswith("candidate.invalid/")' >/dev/null ||
        die "compose did not render the stand-ins of the pending images"
fi

for script in enforcer-up.sh monitor-up.sh verify.sh accept.sh; do
    fails run "${script}"
    grep -q 'unresolved promotion placeholders' "${root}/out" ||
        die "${script} did not refuse the candidate lock: $(cat "${root}/out")"
done

cp -- "${candidate}" "${copy}/VERSIONS.lock"
fails run up.sh
grep -q 'ECASH_SNAPSHOT_SHA256 is still a placeholder' "${root}/out" ||
    die "up.sh accepted a pending snapshot hash: $(cat "${root}/out")"

info "candidate node bootstrap checks passed"
