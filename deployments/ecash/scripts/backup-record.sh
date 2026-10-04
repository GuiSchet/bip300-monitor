#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
load_versions
load_deployment_env
mode="${1:-daily}"
[[ "$mode" == daily || "$mode" == cutover ]] || die 'expected daily or cutover'
require_service_running postgres
root="$(data_root)/backups"
mkdir -p "$root"
exec 9>"$root/.backup.lock"
flock -n 9 || die 'another record backup is running'
writer_identity=""
if [[ "$mode" == cutover ]]; then
    # Freeze observations BEFORE the final archive, including in-flight writes.
    compose stop enforcer-extractor
    writer_identity="$(frozen_writer_identity)"
fi
stamp="$(date --utc +%Y%m%dT%H%M%SZ)"
destination="$root/$mode-$stamp"
mkdir "$destination"
compose exec -T postgres pg_dump --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --format=custom >"$destination/record.dump.partial"
mv "$destination/record.dump.partial" "$destination/record.dump"
cp "$(dirname -- "${BASH_SOURCE[0]}")/../VERSIONS.lock" "$destination/VERSIONS.lock"
postgres_query "SELECT json_build_object('database',current_database(),'schema',(SELECT max(version) FROM schema_version),'datasets',(SELECT json_agg(d) FROM dataset_manifest d))" |
    jq --arg writer "$writer_identity" '. + {cutover_writer: $writer}' >"$destination/manifest.json"
if [[ "$mode" == cutover ]]; then
    [[ "$(frozen_writer_identity)" == "$writer_identity" ]] || die 'extractor changed during the final dump; prepare again'
fi
(
    cd "$destination"
    sha256sum record.dump VERSIONS.lock manifest.json >SHA256SUMS
)
date --utc +%FT%TZ >"$destination/COMPLETE"
# Cutover archives are permanent. Never prune a daily copy before it was
# checksum-verified and restored on the operator machine.
mapfile -t daily < <(find "$root" -mindepth 1 -maxdepth 1 -type d -name 'daily-*' | sort -r)
for old in "${daily[@]:7}"; do
    [[ -f "$old/OFFHOST_RESTORE_OK" ]] && rm -rf -- "$old"
done
info "record backup ready: $destination"
if [[ "$mode" == cutover ]]; then
    info 'extractor remains stopped; copy and restore this archive off-host before finalizing'
fi
