# Observer v7 release candidate — 2026-10-04

This candidate is built and tested locally, not published or deployed. The
`VERSIONS.lock` image references name the exact OCI artifacts that must be
published unchanged before applying the deployment. Do not rebuild and assume
the digests will match.

Sources:

- Enforcer: `9b2a15621469a88ea5d3b8f1dcd5ee1bb21e0ac4`.
- Monitor image source: `ecf5b8290e6b5501508a4a3913bd93c83728cca1`.
- Observatory image source: `d349c23e20d52f287289a313c4d8d884846261c8`.
- Observatory deployment: `6a78a1adfaede62fce706ece5dd2159be6500e4d`.
- Observatory API/sync OCI: `docker.io/guischet/drivechain-observatory:sha-d349c23e20d5@sha256:d925d1d2244d122c911b0c3aa3d27c95b649a107eee82b38a765d01ee1a34e59`.
  Set `PULSE_IMAGE` to this reference for the separate Observatory deployment
  after publication. Its web source is in the same reviewed checkout.

The later monitor deployment commit only pairs these image digests with the
release and strengthens backup control scripts/tests; it does not change the
Rust/protobuf/Dockerfile image inputs. The node, NATS and PostgreSQL versions
remain those in the previously reviewed lock. Event contract 7 / record schema8
requires a fresh record dataset; Observatory uses projection version 6 and a
fresh database configured with that dataset UUID.

## Validation

- Enforcer: 205 library and 15 application tests; Clippy; protobuf lint and
  byte-for-byte vendoring from the official base plus the reviewed patch.
- Monitor: 150 tests with all features (64 extractor, 7 conversions, 3 NATS,
  10 logger, 37 shared, 29 real PostgreSQL); all-feature Clippy and formatting.
- Operator SQL: exact canonical parent lineage, orphan-at-missing-height,
  incompatible dataset/contract, conflicting facts and a tip unlike the node.
- Deployment static checks: shell/YAML/Compose/Just and protobuf provenance;
  four simulated cutover tests covering freeze-before-dump, restore receipts,
  corruption, subsecond restarts/replacements, retention and directory safety.
- Observatory: source/sync/API integration and browser routes, mobile layout,
  exact JSON copy, charts, provenance, SSE/outage recovery. Reorg A→B→A, missing
  history, conflicting facts, readiness, stable revision brackets and exact
  `u64::MAX` confirmed fees were exercised. Final source pins passed another
  complete source/sync/API integration run. Clippy, TypeScript and the web's
  optimized production build passed. A PostgreSQL18 container replacement also
  preserved an inserted table; `PGDATA` is explicit within its named volume.
  Existing destinations must be backed up and restored to the new v7 volume,
  not silently reused under the changed data-directory setting.
- Scale: 1,000,000 synthetic events / 10,000 headers; seed 24.037 s, resumed
  rebuild 76.578 s, list p95 21.088 ms, one-block incremental extension 81 ms.
  These numbers describe the local test environment, not HOSTKEY capacity.
- A real schema8 custom-format dump restored in an isolated PostgreSQL18
  container. Corrupted bytes were rejected; 49-hour backup age and malformed
  receipts failed health checks. Production v6 restore remains a cutover gate.
- All four OCI images passed executable smoke tests with networking disabled.
  Loaded Docker configs matched the corresponding OCI config digests.

## Review disposition

| Review | Result |
|---|---|
| H1 | Reorg repair stops at a certified ancestor and retains the previous covered tip until a replacement proof commits. |
| H2 | Verification walks hashes and parents to activation and checks the node's selected hash, rather than counting distinct heights. |
| H3 | Conflicting immutable block payloads are retained in a conflict ledger and prevent certification. |
| H4 | Slot-independent global transitions include session/sequence; disconnect/restart gaps remain explicit. Observatory materializes canonical membership and retains alternatives. |
| H5 | Bounded snapshot retries, equal revision/tip requirements and stable occurrence views prevent changed windows from becoming authoritative state. |
| H6 | Fixed-width hashes, slot bounds and duplicate BMM identities are validated. |
| H7 | Every five-second BMM occurrence is retained intentionally; daily size/quality reporting and backup retention are added. No event pruning or partitioning yet. |
| H8 | Presence and completeness checks include dataset, contract, source and instance scope. |
| H9 | Dead production checkpoint accessor removed; remaining test helper is documented as diagnostic. Legacy capture labels remain compatible with stored observations. |
| H10 | Single-writer transactions remain; wait/duration metrics expose pressure. PostgreSQL loss still restarts the process and history repairs idempotently. No connection pool or in-process reconnection in this release. |
| H11 | PostgreSQL/raw evidence retains exact integers; public monetary/64-bit values are decimal strings and browser copying is lossless. |

Additional corrections separate per-block work from absolute cumulative work,
qualify mempool samples with readiness generations, bound timeout resync, and
make confirmed fees independent resumable enrichment with explicit unknowns.

## Apply and rollback

Follow [BACKUP_V7.md](BACKUP_V7.md). Publish these exact artifacts, integrate the
reviewed monitor deployment commit into main, then run the HOSTKEY skill's
read-only `--check` for that 40-character commit. Apply only after operator
approval and successful preflight. No Rust compilation happens on the VPS.

Freeze the existing extractor before the final v6 dump. Require checksum and
real off-host restore evidence before staging an empty record database. Keep
node/enforcer data. Deploy the paired images, grant the reader access and select
the new dataset. Require `just verify`, every-slot `just verify-live`, `just
accept`, and 24 hours of observations before final production acceptance.

Rollback pairs the preserved old database with its archived old images; never
start the old monitor against schema8. Preserve both old and new archives.
