# Record backup and dataset cutover

These commands are a reviewed runbook, not an instruction to apply an unreviewed
release. HOSTKEY cutover requires approval of the exact release and images.
Node/enforcer state and every five-second BMM occurrence are retained.

On the VPS, install the `bip300-record-backup` and `bip300-offhost-health`
service/timer pairs from `systemd/`. The first dumps daily at 03:00 UTC. The second
fails visibly in systemd/journal when no verified daily off-host copy is newer
than 48 hours. No external notification destination is configured.

On the operator computer, copy `scripts/offhost-backup.py` to
`~/.local/lib/bip300-monitor/`, and install the `bip300-offhost-pull` service/timer
as user units. The existing SSH identity and strict known-host verification are
required; no key contents are copied. The computer needs Docker and must be
online. Pull runs hourly, and each new archive is checksum-verified and restored
into an isolated PostgreSQL18 container without published ports. The receipt is
returned to the VPS only after `pg_restore --exit-on-error` and manifest checks.

Keep seven daily VPS copies and thirty local copies. Cutover archives are
permanent. Unverified VPS copies are never pruned, so an unavailable local
computer can temporarily exceed seven copies; watch disk use and the health
service. A restored database contains every occurrence, not only unique facts.

Cutover sequence, after release approval:

1. On the existing release, install the reviewed backup/reset scripts, then run
   `bash scripts/reset-record.sh betanet --prepare`. This stops the extractor
   **before** the final dump and leaves it stopped. The archive contains the old
   `VERSIONS.lock`, dataset manifest, dump, checksums and completion timestamp.
2. On this computer, run `offhost-backup.py --host root@162.141.78.240
   --identity ~/.ssh/bip300-alpha --local-root ~/.local/share/bip300-monitor/backups/betanet
   --archive cutover-TIMESTAMP`. Inspect the off-host restore receipt.
3. On the VPS, run `bash scripts/reset-record.sh betanet --finalize
   cutover-TIMESTAMP`. It refuses without the matching receipt, refuses a
   restarted or replaced extractor (container identity and full precision
   startup timestamp), and moves the old cluster into the permanent archive.
4. Apply the approved v7 enforcer+monitor images. The new monitor creates a
   fresh dataset; it refuses an existing v6 identity before applying migrations.
5. Grant the Observatory reader SELECT on `observation_failure`, configure the
   new dataset UUID and run the projection6 importer. Preserve the old record
   and old Observatory generation for archival access with their paired binary.
6. Run `just verify`, `just verify-live` for every slot and `just accept`.
   Collect `quality-report.sql` daily and retain transaction wait/duration logs.
   Enable `BIP300_MONITOR_CONFIRMED_BMM_FEES=true` only after core acceptance.
   Observe 24 hours before declaring final production data-quality acceptance.

Rollback stops the new extractor and Postgres, archives the new cluster, and
restores the old cluster **with the old images from its archived lock**. Never
start an older monitor against schema8. Run verification again and keep both
cutover archives. Do not reset node chainstate or enforcer LMDB.

Local regression checks: `python3 scripts/test-backup-cutover.py` simulates
Docker only to verify freeze-before-dump, missing receipts, corruption, rapid
restarts, permanent archives and directory preservation. It also runs in the
deployment static check. Separately, `offhost-backup.py --restore-only
--local-root ARCHIVE` validates the real dump in isolated PostgreSQL.
