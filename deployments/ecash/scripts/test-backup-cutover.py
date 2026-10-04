#!/usr/bin/env python3
"""Exercise cutover guards in disposable directories with simulated Docker.

This checks control flow and preservation, not pg_dump validity. The off-host
restore test must separately validate a real PostgreSQL custom-format archive.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
IDENTITY = 'a' * 64 + ' 2026-10-04T10:00:00.000000001Z false'


class CutoverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='observer-cutover-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scripts = self.root / 'scripts'
        self.scripts.mkdir()
        for name in ['lib.sh', 'backup-record.sh', 'reset-record.sh']:
            shutil.copyfile(SCRIPTS / name, self.scripts / name)
        with (self.scripts / 'lib.sh').open('a') as stream:
            stream.write(r'''
load_versions() { NETWORK_ID=betanet; POSTGRES_USER=test; POSTGRES_DB=test; }
load_deployment_env() { PUID="$(id -u)"; PGID="$(id -g)"; }
data_root() { printf '%s\n' "$TEST_ROOT/data"; }
require_service_running() { return 0; }
service_is_running() { grep -q ' true$' "$TEST_ROOT/writer"; }
postgres_query() { printf '%s\n' '{"schema":8,"datasets":[]}'; }
compose() {
    printf '%s\n' "$*" >> "$TEST_ROOT/actions"
    case "$*" in
        'stop enforcer-extractor') sed -i 's/ true$/ false/' "$TEST_ROOT/writer" ;;
        'stop postgres') ;;
        'ps -a -q enforcer-extractor') cut -d ' ' -f 1 "$TEST_ROOT/writer" ;;
        'exec -T postgres pg_dump '*) printf 'simulated dump bytes\n' ;;
        *) return 1 ;;
    esac
}
''')
        (self.root / 'VERSIONS.lock').write_text('old paired release\n')
        (self.root / 'writer').write_text(IDENTITY + '\n')
        docker = self.root / 'docker'
        docker.write_text('#!/bin/sh\ncat "$TEST_ROOT/writer"\n')
        docker.chmod(0o700)
        self.env = dict(os.environ, TEST_ROOT=str(self.root), PATH=str(self.root) + ':' + os.environ['PATH'])
        for component in ['postgres', 'node', 'enforcer']:
            directory = self.root / 'data' / component
            directory.mkdir(parents=True)
            (directory / 'sentinel').write_text(component)

    def run_script(self, name, *args, succeeds=True):
        result = subprocess.run(['bash', str(self.scripts / name), *args], env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, succeeds, result.stdout + result.stderr)
        return result

    def prepare(self):
        self.run_script('reset-record.sh', 'betanet', '--prepare')
        archive, = (self.root / 'data/backups').glob('cutover-*')
        actions = (self.root / 'actions').read_text()
        self.assertLess(actions.index('stop enforcer-extractor'), actions.index('pg_dump'))
        manifest = json.loads((archive / 'manifest.json').read_text())
        self.assertEqual(manifest['cutover_writer'], IDENTITY)
        return archive

    def receipt(self, archive):
        (archive / 'OFFHOST_RESTORE_OK').write_text(json.dumps({
            'dump_sha256': hashlib.sha256((archive / 'record.dump').read_bytes()).hexdigest(),
            'validation': {'schema': 8}, 'restored_at': 1, 'restored_on': 'isolated-test',
        }))

    def finalize(self, archive, succeeds=True):
        return self.run_script('reset-record.sh', 'betanet', '--finalize', archive.name, succeeds=succeeds)

    def test_requires_restore_and_moves_only_postgres(self):
        archive = self.prepare()
        self.finalize(archive, succeeds=False)
        self.assertTrue((self.root / 'data/postgres/sentinel').is_file())
        self.receipt(archive)
        self.finalize(archive)
        self.assertEqual((archive / 'cluster/sentinel').read_text(), 'postgres')
        self.assertEqual(list((self.root / 'data/postgres').iterdir()), [])
        for component in ['node', 'enforcer']:
            self.assertEqual((self.root / f'data/{component}/sentinel').read_text(), component)
        self.finalize(archive, succeeds=False)

    def test_same_second_restart_or_replacement_invalidates_archive(self):
        archive = self.prepare()
        self.receipt(archive)
        for identity in [IDENTITY.replace('000000001Z', '000000002Z'), IDENTITY.replace('a' * 64, 'b' * 64), IDENTITY.replace('false', 'true')]:
            (self.root / 'writer').write_text(identity + '\n')
            self.finalize(archive, succeeds=False)
            self.assertTrue((self.root / 'data/postgres/sentinel').is_file())

    def test_corrupt_archive_never_moves_postgres(self):
        archive = self.prepare()
        self.receipt(archive)
        with (archive / 'record.dump').open('ab') as stream:
            stream.write(b'corrupted')
        self.finalize(archive, succeeds=False)
        self.assertTrue((self.root / 'data/postgres/sentinel').is_file())

    def test_daily_retention_keeps_unverified_and_permanent_archives(self):
        root = self.root / 'data/backups'
        root.mkdir()
        for day in range(1, 10):
            archive = root / f'daily-202001{day:02d}T000000Z'
            archive.mkdir()
            if day != 1:
                (archive / 'OFFHOST_RESTORE_OK').write_text('verified fixture')
        permanent = root / 'cutover-20200101T000000Z'
        permanent.mkdir()
        self.run_script('backup-record.sh', 'daily')
        self.assertEqual(len(list(root.glob('daily-*'))), 8)
        self.assertTrue((root / 'daily-20200101T000000Z').is_dir())
        self.assertTrue(permanent.is_dir())


if __name__ == '__main__':
    unittest.main()
