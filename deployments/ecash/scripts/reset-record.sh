#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
load_versions
load_deployment_env
[[ "${1:-}" == "$NETWORK_ID" ]] || die "pass the exact network id: $NETWORK_ID"
mode="${2:---prepare}"
if [[ "$mode" == --prepare ]]; then
    exec bash "${DEPLOYMENT_ROOT}/scripts/backup-record.sh" cutover
fi
[[ "$mode" == --finalize ]] || die 'expected --prepare or --finalize ARCHIVE_NAME'
archive="${3:-}"
[[ "$archive" =~ ^cutover-[0-9]{8}T[0-9]{6}Z$ ]] || die 'expected the exact prepared cutover archive name'
root="$(data_root)"
backup="$root/backups/$archive"
[[ -f "$backup/COMPLETE" && -f "$backup/OFFHOST_RESTORE_OK" ]] || die 'verified off-host restore receipt is required'
service_is_running enforcer-extractor && die 'extractor must remain stopped since the final dump; prepare a new archive'
[[ ! -e "$backup/cluster" ]] || die 'this archive was already finalized'
(cd "$backup" && sha256sum --check SHA256SUMS)
python3 - "$backup" <<'PY'
import hashlib,json,sys
from pathlib import Path
p=Path(sys.argv[1])
with (p/'record.dump').open('rb') as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
r=json.loads((p/'OFFHOST_RESTORE_OK').read_text())
assert r['dump_sha256']==digest and r['validation']['schema']>=7, 'invalid off-host restore receipt'
assert r['restored_at']>0 and r['restored_on'], 'missing off-host restore evidence'
PY
# A restarted writer after preparation invalidates the frozen archive even if
# it was stopped again before this command.
frozen_writer="$(jq -er '.cutover_writer | select(type == "string" and length > 0)' "$backup/manifest.json")"
[[ "$(frozen_writer_identity)" == "$frozen_writer" ]] || die 'extractor restarted or was replaced after archive; prepare again'
postgres_root="$root/postgres"
[[ "$root" != / && -d "$postgres_root" && ! -L "$postgres_root" ]] || die 'unsafe Postgres directory'
compose stop postgres
mv -- "$postgres_root" "$backup/cluster"
mkdir -- "$postgres_root"
chown "$PUID:$PGID" "$postgres_root"
chmod 0700 "$postgres_root"
if [[ -f "$root/.deployment-accepted" ]]; then
    mv "$root/.deployment-accepted" "$backup/deployment-accepted.before-reset"
fi
info "new Postgres record staged; archived cluster and paired VERSIONS.lock: $backup"
info "start only the reviewed v7 release; rollback must restore the archived cluster AND its old binaries"
