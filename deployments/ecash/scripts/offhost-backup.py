#!/usr/bin/env python3
"""Pull and restore private record backups on the operator's computer.

No credentials are printed or copied. SSH uses the operator's existing key and
known_hosts. A receipt is returned only after checksum AND pg_restore validation.
"""
import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import time
import uuid

POSTGRES = "postgres:18.2-alpine@sha256:035b9ab53cfa147d7202b61f5f7782b939ae815b7d6bc81c96b7b42ff1fca950"


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def verify_checksums(path):
    checks = {}
    for line in (path / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        if name not in {"record.dump", "VERSIONS.lock", "manifest.json"} or name in checks:
            raise ValueError("unexpected checksum entry")
        with (path / name).open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != digest:
            raise ValueError(f"checksum mismatch: {name}")
        checks[name] = digest
    if len(checks) != 3:
        raise ValueError("incomplete checksum manifest")
    return checks


def restore(path):
    checks = verify_checksums(path)
    container = "observer-restore-" + uuid.uuid4().hex[:12]
    run(["docker", "run", "--detach", "--rm", "--network=none", "--name", container,
         "-e", "POSTGRES_HOST_AUTH_METHOD=trust", POSTGRES], stdout=subprocess.DEVNULL)
    try:
        for _ in range(60):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("restore database did not become ready")
        with (path / "record.dump").open("rb") as stream:
            run(["docker", "exec", "-i", container, "pg_restore", "-U", "postgres", "-d", "postgres",
                 "--exit-on-error", "--single-transaction", "--no-owner", "--no-privileges"], stdin=stream)
        result = run(["docker", "exec", container, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-c",
                      "SELECT json_build_object('schema',(SELECT max(version) FROM schema_version),'events',(SELECT count(*) FROM event),'observations',(SELECT count(*) FROM event_observation),'datasets',(SELECT count(*) FROM dataset_manifest))"], capture_output=True, text=True)
        evidence = json.loads(result.stdout)
        manifest = json.loads((path / "manifest.json").read_text())
        if evidence["schema"] != manifest["schema"] or evidence["datasets"] != len(manifest["datasets"] or []):
            raise ValueError("restored database does not match the manifest")
        receipt = dict(dump_sha256=checks["record.dump"], restored_at=int(time.time()),
                       restored_on=os.uname().nodename, validation=evidence)
        (path / "OFFHOST_RESTORE_OK").write_text(json.dumps(receipt) + "\n")
    finally:
        run(["docker", "stop", container], stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host")
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--remote-root", default="/srv/bip300-monitor/betanet/backups")
    parser.add_argument("--local-root", type=Path, required=True)
    parser.add_argument("--archive", help="copy only this permanent cutover archive")
    parser.add_argument("--restore-only", action="store_true", help="validate one already copied archive; no SSH")
    args = parser.parse_args()
    os.umask(0o077)
    if args.restore_only:
        restore(args.local_root.resolve())
        return
    if not args.host or not args.identity or not re.fullmatch(r"(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9][A-Za-z0-9.-]*", args.host):
        parser.error("--host and --identity are required")
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
           "-o", "HostKeyAlgorithms=ssh-ed25519", "-i", str(args.identity), args.host]
    root = shlex.quote(args.remote_root)
    listing = run(ssh + [f"find {root} -mindepth 2 -maxdepth 2 -name COMPLETE -type f"], capture_output=True, text=True).stdout
    args.local_root.mkdir(parents=True, exist_ok=True)
    restored = []
    for remote in sorted(listing.splitlines()):
        name = Path(remote).parent.name
        if not re.fullmatch(r"(daily|cutover)-[0-9]{8}T[0-9]{6}Z", name):
            continue
        if args.archive and name != args.archive:
            continue
        local = args.local_root / name
        remote_dir = shlex.quote(args.remote_root + "/" + name)
        local.mkdir(exist_ok=True)
        if not (local / "OFFHOST_RESTORE_OK").exists():
            for filename in ["record.dump", "VERSIONS.lock", "manifest.json", "SHA256SUMS", "COMPLETE"]:
                partial = local / (filename + ".partial")
                with partial.open("wb") as stream:
                    run(ssh + [f"cat {remote_dir}/{filename}"], stdout=stream)
                partial.replace(local / filename)
            restore(local)
        # Recheck the bytes even for an archive restored on an earlier run.
        digest = verify_checksums(local)["record.dump"]
        receipt = json.loads((local / "OFFHOST_RESTORE_OK").read_text())
        if receipt["dump_sha256"] != digest:
            raise ValueError("local archive changed after restore validation")
        with (local / "OFFHOST_RESTORE_OK").open("rb") as stream:
            run(ssh + [f"cat >{remote_dir}/OFFHOST_RESTORE_OK.partial && mv {remote_dir}/OFFHOST_RESTORE_OK.partial {remote_dir}/OFFHOST_RESTORE_OK"], stdin=stream)
        restored.append(local)
    daily = sorted((p for p in args.local_root.glob("daily-*") if (p / "OFFHOST_RESTORE_OK").is_file()), reverse=True)
    for old in daily[30:]:
        shutil.rmtree(old)
    if args.archive:
        if not restored:
            raise RuntimeError("requested archive is not complete on the remote host")
    elif not daily or time.time() - datetime.fromisoformat((daily[0] / "COMPLETE").read_text().strip().replace("Z", "+00:00")).timestamp() > 48 * 3600:
        raise RuntimeError("no verified daily backup newer than 48 hours")
    print(f"Verified {len(restored)} off-host archives; permanent cutover archives retained")


if __name__ == "__main__":
    main()
