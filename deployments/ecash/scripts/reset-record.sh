#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

load_versions
load_deployment_env
for command_name in chmod chown date docker mkdir mv sha256sum; do
    require_command "${command_name}"
done

confirmation="${1:-}"
[[ "${confirmation}" == "${NETWORK_ID}" ]] ||
    die "refusing to reset Postgres: pass the exact network id (${NETWORK_ID})"

require_service_running postgres
wait_for_postgres_health

resolved_data_root="$(data_root)"
postgres_root="${resolved_data_root}/postgres"
[[ "${resolved_data_root}" != / && "${postgres_root}" == "${resolved_data_root}/postgres" ]] ||
    die "unsafe Postgres reset target: ${postgres_root}"
[[ -d "${postgres_root}" ]] || die "Postgres data directory does not exist: ${postgres_root}"

timestamp="$(date --utc +%Y%m%dT%H%M%SZ)"
backup_root="${resolved_data_root}/backups/postgres-${timestamp}"
mkdir -p -- "${backup_root}"
chmod 0700 "${backup_root}"
dump_path="${backup_root}/record.dump"

info "backing up the current Postgres record"
compose exec -T postgres \
    pg_dump --username="${POSTGRES_USER}" --dbname="${POSTGRES_DB}" --format=custom \
    >"${dump_path}"
chmod 0600 "${dump_path}"
sha256sum "${dump_path}" >"${dump_path}.sha256"
chmod 0600 "${dump_path}.sha256"

info "stopping only the extractor and Postgres; node and enforcer data are preserved"
compose stop enforcer-extractor postgres

old_cluster="${backup_root}/cluster"
mv -- "${postgres_root}" "${old_cluster}"
mkdir -- "${postgres_root}"
chown "${PUID}:${PGID}" "${postgres_root}"
chmod 0700 "${postgres_root}"

marker="${resolved_data_root}/.deployment-accepted"
if [[ -f "${marker}" ]]; then
    mv -- "${marker}" "${backup_root}/deployment-accepted.before-reset"
fi

info "Postgres record reset is staged; run 'just monitor-up' to migrate and backfill"
info "recoverable backup: ${backup_root}"
