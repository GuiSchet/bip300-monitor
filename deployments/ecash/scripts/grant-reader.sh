#!/usr/bin/env bash
# Provision the read-only role the Observatory imports through. Idempotent:
# rerunning it re-applies the exact grants of the current record contract.
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
load_versions
load_deployment_env
require_service_running postgres

role="${OBSERVATORY_READER_ROLE:-pulse_sync_reader}"
[[ "$role" =~ ^[a-z_][a-z0-9_]{0,62}$ ]] || die 'invalid reader role name'
secret="$(data_root)/secrets/observatory-reader-password"
if [[ ! -s "$secret" ]]; then
    # Hex keeps the password URL-safe for the Observatory connection string.
    openssl rand -hex 24 >"$secret"
    info "generated the reader password in ${secret}"
fi
password="$(<"$secret")"
# Passed to psql on stdin, never on its command line (visible in ps).
[[ "$password" =~ ^[0-9a-f]{32,}$ ]] || die 'the reader password must be hexadecimal'

# Every relation the Observatory importer reads, including its compatibility
# probe. Keep in step with drivechain-observatory SOURCE_CONTRACT.md.
tables=(
    schema_version dataset_manifest extractor_run event event_observation
    tip_observation snapshot_group sidechain_instance current_sidechain_instance
    history_coverage history_coverage_revision extractor_status
    extractor_worker_status observation_failure
)
table_list="$(
    IFS=,
    echo "${tables[*]}"
)"

postgres_query "\\set password '${password}'
SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4', :'role')
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \\gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'role', :'password') \\gexec
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'role') \\gexec
SELECT format('ALTER ROLE %I SET statement_timeout = %L', :'role', '30s') \\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'role') \\gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'role') \\gexec
SELECT format('GRANT SELECT ON %s TO %I',
              (SELECT string_agg(format('public.%I', t), ',') FROM unnest(string_to_array(:'tables', ',')) t),
              :'role') \\gexec
" --set=role="$role" --set=tables="$table_list" >/dev/null

missing="$(postgres_query "
SELECT string_agg(t, ' ') FROM unnest(string_to_array(:'tables', ',')) t
 WHERE NOT has_table_privilege(:'role', format('public.%I', t), 'SELECT')
" --set=role="$role" --set=tables="$table_list")"
[[ -z "$missing" ]] || die "reader role ${role} still lacks SELECT on: ${missing}"
writable="$(postgres_query "
SELECT string_agg(t, ' ') FROM unnest(string_to_array(:'tables', ',')) t
 WHERE has_table_privilege(:'role', format('public.%I', t), 'INSERT,UPDATE,DELETE')
" --set=role="$role" --set=tables="$table_list")"
[[ -z "$writable" ]] || die "reader role ${role} can write to: ${writable}"
info "reader role ${role} can read every Observatory source relation and write none"
