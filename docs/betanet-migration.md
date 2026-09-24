# HOSTKEY migration from Alphanet to Betanet

This is the release and operational gate for replacing the existing single-VM
Alphanet pilot. The promoted `VERSIONS.lock` contains every immutable Betanet
value. `deployments/ecash/VERSIONS.betanet.lock.example` retains the same
assignments as an audit reference.

## Reviewed source baseline

- eCash node Betanet base: `ca64033c137457a3c8ca394186759819a2ab0694`
  (upstream `betanet`, reviewed 2026-09-22).
- BIP300/301 enforcer base:
  `0e27251ef351a522c72ab9ef079f75e06075390f`.
- Latest upstream revision reviewed for compatibility:
  `6e5f79f7668b0f82eab788531adb2ea21664d972`. It is six commits ahead of the
  pinned base, but none changes the vendored protobufs or validator observer
  paths used by the monitor; the relevant behavior fix is wallet-only, so the
  deployed observer remains pinned rather than taking an unrelated upgrade.
- Read-only observer enforcer:
  `0740a39380b39885fe8655f79f78150001d8a15b`. It retains upstream
  `GetSeenBmmRequests` and adds the historical `GetBip300BlockDelta` RPC.
- Betanet activation: height `967680`, block
  `00000000000000030101ba5cfea54b22becc79f95dc6040beb76e01dd9d04042`.
- Wire magic `eca5b104`; P2P/RPC ports `8533`/`8532`.

The next normalized monitor contract is v6. It keeps the validator mempool task
behind live BMM auctions and the independent tip/BMM worker health introduced
in v5. It also:

- calculates the BIP300 description hash over the decoded description bytes,
  excluding the `CompactSize` prefix, and stores it in display order;
- brackets each BMM RPC with tip reads and records it only if the parent is
  stable; its own five-second cycle reads the parent, while the independent
  30-second tip poll may wake it early after detecting a move;
- validates network, activation height, and the activation block reached by
  global history inside the extractor;
- treats transient `GetChainTip` failures during history reconciliation as
  deferred work instead of restarting the extractor; and
- closes an orphaned run transactionally while enforcing one active writer.

BMM facts contain slot, transaction id, critical hash and bid in satoshis; they
are sorted canonically and hashed without their observation timestamp, while
every poll occurrence remains in `event_observation`. A successful empty
response is recorded; an RPC error or a parent that moves during sampling is
not converted into an empty auction. Confirmed M8 `fee_sats` remains unavailable
because the reviewed enforcer always returns it absent; the monitor does not
invent a value.

## Reviewed AssumeUTXO bootstrap

The official Betanet node already commits to the Bitcoin height-`935000`
AssumeUTXO set. Its block hash, serialized UTXO hash and chain transaction count
are pinned in the candidate lock, so this rollout does not need a custom node
fork or a new consensus commitment.

Snapshot format v2 includes the source network magic even though the UTXO set
is the same pre-fork history. The deployment therefore:

1. downloads the pinned Bitcoin snapshot from either of two HTTPS sources;
2. verifies its exact `9387990306` byte size and SHA-256;
3. requires the v2 header and Bitcoin magic `f9beb4d9`;
4. copies it and replaces only bytes 7-10 with Betanet magic `eca5b104`;
5. proves the prefix and all bytes after the magic are unchanged;
6. verifies the transformed artifact's separately pinned SHA-256 before
   passing it to `loadtxoutset`.

The official node then validates the embedded base block and compiled UTXO
commitment. The base precedes activation at `967680`, so the active chainstate
downloads every activation-era block body normally. As a recovery measure the
deployment repeatedly asks a synced outbound peer for the exact activation
block if its header is present but its raw body is still unavailable.

Promotion remains blocked until both source URLs have been downloaded in full,
their source hashes match, and the deterministic Betanet artifact hash has
replaced the final placeholder in the candidate lock. This was reproduced on
2026-09-22: both sources matched
`e572ddbe456d254f05fb004cebe225bdb3656074b66f0e9b1c7fa83e1301d486`,
and the Betanet artifact matched
`dd38115648221d796a42685d449b84e7ebf352a57bdda37679cece5f6137d1e0`.

## Artifact promotion gate

Before touching HOSTKEY:

1. push the enforcer observer branch (the node is the official pinned image);
2. merge the monitor v6 change and publish the extractor/logger images;
3. run all Rust, Postgres, NATS and deployment static tests;
4. resolve every image to an immutable `linux/amd64` digest;
5. confirm neither the active nor audit-reference lock contains a
   `REPLACE_WITH_*` value;
6. verify the node digest belongs to the recorded official commit, enforcer
   commit is the reviewed observer commit, and monitor images were built from
   `MONITOR_IMAGE_COMMIT`;
7. set `MONITOR_EVENT_CONTRACT_VERSION=6`, replace both monitor image pins in
   `VERSIONS.lock` and its audit-reference lock in one reviewed commit, and run
   `deployments/ecash/scripts/static-check.sh` again.

Until step 7, the active lock deliberately remains on the last published v5
images even though the working tree contains v6 source. Do not deploy a mixed
state or point `MONITOR_IMAGE_COMMIT` at a commit without published immutable
digests.

Never deploy the example lock and never use mutable tags.

## Preserve Alphanet evidence

Before deleting any chain data, stop writers and create verified off-host
backups of:

- Postgres (`pg_dump` plus SHA-256);
- enforcer database/state;
- `.deployment-accepted`, deployed lock, repository commit and `.env` with
  secrets redacted;
- monitor/enforcer logs needed for the experiment;
- the snapshot manifest and checksums.

Restore the database into a temporary Postgres instance and run representative
row/count/coverage queries. Only after this restore test and explicit operator
approval may the re-downloadable Alphanet `node/blocks`, `node/chainstate` and
`node/indexes` (if present) and snapshot file be removed. Keep the evidence
backup and enforcer/Postgres data. Require at least 1.5 TB free after cleanup
before starting Betanet.
The Betanet data root must be `${ECASH_DATA_BASE}/betanet`; never reuse the
Alphanet chainstate directory.

## Betanet rollout order

1. Verify the SSH host-key fingerprint and run the read-only preflight/status.
2. Deploy the reviewed repository commit and complete `just init`.
3. Run `just preflight`; Compose must publish no ports.
4. Start only the node with `just up`.
5. Run `just snapshot`. The script verifies and transforms the source as
   described above, waits for the exact base header, invokes `loadtxoutset`,
   and accepts only the compiled UTXO commitment.
6. Confirm `just status` reports `trusted_snapshot_ready=true` and
   `monitoring_ready=true`. `history_fully_validated=false` is expected while
   genesis replay continues in the background.
7. Start the validator-only enforcer with `just enforcer-up`.
8. Before the first v6 monitor start, run `just reset-record betanet`. It makes
   a checksummed dump and moves the v5 Postgres cluster and acceptance marker
   into a recoverable timestamped backup; node and enforcer data are untouched.
   This is required because v5 sidechain-instance hashes used the wrong bytes,
   and the v6 extractor refuses to mix both identities in one dataset.
9. Start the pipeline with `just monitor-up`.
10. Run `just verify`. It requires contract v6, validator mempool
   synchronization, the four v6 correctness capabilities, exactly one running
   enforcer extractor, healthy independent tip/BMM workers, and a stable-parent
   BMM poll (an empty auction is valid), plus a semantic snapshot, complete
   global delta coverage and complete active-slot history.
11. Run `just verify-live`, then `just accept`. The marker records snapshot
   provenance, BMM capability and whether full Core history has finished.
12. Reboot once and repeat status/verification to prove ordered recovery and
    orphan-run reconciliation. Retain the v5 record backup for as long as v5
    rollback remains supported, including after the first v6 acceptance.

Historical Core validation is not disabled. AssumeUTXO removes it only from the
critical path, and `just status` continues to expose its independent progress.

## Rollback

If a v6 monitor gate fails, stop the bootstrap and stack without deleting the
Betanet data root and retain logs. Never run the v5 extractor against the v6
Postgres cluster: v5 cannot enforce the corrected description identity or close
a v6 orphaned run. Preserve the failed v6 cluster separately, restore the
`cluster/` and acceptance marker created by the pre-v6 `just reset-record
betanet`, and only then select the reviewed v5 commit and lock. Changing image
pins without restoring that backup is not a valid rollback.

An Alphanet rollback remains a separate recovery: restore its preserved
evidence only to its own data root. A rollback must never point one network's
binaries at another network's chainstate. Keep the pre-v6 Betanet backup while
v5 rollback remains supported.
