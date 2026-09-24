# eCash deployment

Single-VM infrastructure for exercising `bip300-monitor` against a pinned
eCash/Drivechain network. The active lock targets **Betanet** and deploys its
L1 node, validator enforcer, Postgres record, Core NATS transport, enforcer
extractor, and event logger. Network-specific values live in `VERSIONS.lock` and
the node configuration is generated from those values plus
`config/ecash.conf.template`. The scripts and Compose topology are shared so a
future Beta or Mainnet transition does not require another deployment copy.

The reviewed, fail-closed HOSTKEY transition procedure and its Alphanet
preservation gates are in
[`docs/betanet-migration.md`](../../docs/betanet-migration.md). The active lock
contains the reproduced snapshot hash and immutable image digests.

Betanet is experimental. Use a dedicated data directory and no real funds.

The lock pins the reviewed observer fork that adds `GetBip300BlockDelta`, while
recording its official base commit separately for provenance. The observer
commit and immutable image move together, and `just verify` requires complete
global BIP300 history before this pre-Pulse deployment can be accepted.

## Requirements

- Ubuntu 24.04 on `x86_64`.
- Docker Engine with the Compose and Buildx plugins.
- Bash, Coreutils, Diffutils, `awk`, `curl`, Git, Grep, `jq`, and `tar`. These
  provide every command used by initialization, preflight, verification, and
  acceptance, including `df`, `mktemp`, `realpath`, `sha256sum`,
  `stat`, and `timeout`.
- At least 16 GiB RAM. Disk preflight projects 1.2 TB of data while preserving
  20% of the filesystem; on a new 2 TB volume this requires about 1.6 TB free.

Install the pinned `just` command runner and initialize the deployment:

```bash
./scripts/install-just.sh
just init
```

Review `.env` and set `ECASH_DATA_BASE`. Initialization derives the Compose
project as `bip300-ecash-${NETWORK_ID}` and the data root as
`${ECASH_DATA_BASE}/${NETWORK_ID}`; neither derived value can be overridden by
the operator. Then run preflight and start the node:

```bash
just preflight
just up
just status
```

The AssumeUTXO bootstrap is explicit because it downloads about 9.5 GB,
verifies its pinned size and SHA-256, and waits for the exact snapshot-base
header before loading it:

```bash
just snapshot
just status
```

Once the snapshot-backed node reaches the active network tip, start the pinned
validator-only enforcer. Wallet and mining services remain disabled:

```bash
just enforcer-up
just status
```

The pinned enforcer connects the pre-activation prefix from the headers it has
already synchronized, without fetching the corresponding block bodies. The
raw block-file fast path remains available for the post-activation history,
where BIP300 messages must be inspected. On an initial sync, the journal should
therefore report `Connected ... pre-activation block(s) from stored headers`
before the validator processes blocks at and above the locked activation
height.

For Betanet, the official commitment is at height 935000. The downloader
verifies either pinned source mirror, then deterministically rewrites only the
four version-2 header bytes containing Bitcoin's network magic. The transformed
artifact has its own locked SHA-256; `loadtxoutset` still validates the base
block and UTXO commitment compiled into the official node.

The generated observer-only node configuration sets `prune=0` and `txindex=0`
explicitly. Runtime readiness
rejects `getblockchaininfo.pruned = true`, because the observer RPC needs raw
historical blocks to preserve exact scripts and transactions. The enforcer has
wallet and mining disabled and does not need Core's transaction index.

The default `.env.example` deliberately sets
`TRUST_ASSUMEUTXO_SNAPSHOT=true`. With that policy, `just enforcer-up` accepts
either a fully validated chainstate or exactly two chainstates whose active one
is backed by the pinned snapshot block. It never accepts an arbitrary
snapshot: the downloaded file must match the size and SHA-256 in
`VERSIONS.lock`, and `loadtxoutset` must deserialize it and match its UTXO hash
to the AssumeUTXO commitment compiled into the pinned node image.

This removes historical validation from the deployment's critical path; it
does not disable it. The node continues its independent replay from genesis in
the background. `just status` reports `history_fully_validated`,
`trusted_snapshot_ready`, and `monitoring_ready` separately so accepting the
snapshot is never confused with finishing that replay. Set
`TRUST_ASSUMEUTXO_SNAPSHOT=false` to restore the full-history gate.

This is an explicit trust tradeoff: before the replay finishes, the deployment
trusts the locked node build and snapshot commitments. Betanet is experimental
and must not carry real funds. The enforcer also performs its own initial block
sync, so its first startup can still take time even though node history no
longer blocks it.

Start the observation pipeline after the enforcer catches up:

```bash
just monitor-up
just verify
```

Verification asks the record, not the logs. It reads the block the newest
recorded snapshot is anchored to, selects the newest event-contract version at
that block, and requires **exactly one fact** per expected snapshot kind and
slot. It also requires complete per-instance `block` coverage and global
`bip300_delta` coverage even when there are zero active slots. Repeated captures
belong in `event_observation`, not as duplicate facts.

Contract v5 introduced the enforcer's validator mempool requirement and live
BMM-request polling. Contract v6 additionally accepts a poll only when tip
reads before and after the RPC prove that its parent stayed unchanged, verifies
the configured network and activation height inside the extractor, and stores
the corrected BIP300 description hash. An empty request list is an observed
fact; an RPC error or a moving parent is never treated as an empty auction.
Likewise, a response that cannot be normalized is not persisted: it counts as
a BMM-worker failure, degrades that worker after three consecutive failures and
blocks verification and acceptance without stopping unrelated history work.
Distinct auction states at the same parent block are separate facts, while a
repeated state adds only a new observation occurrence. The latest auction must
therefore be selected through its occurrence, not the fact's first-seen
timestamp:

```sql
SELECT observation.observed_at, fact.payload
  FROM event_observation observation
  JOIN event fact ON fact.id = observation.event_id
 WHERE observation.dataset_id = $1
   AND fact.dataset_id = observation.dataset_id
   AND fact.kind = 'bmm_requests'
 ORDER BY observation.observed_at DESC, observation.observation_id DESC
 LIMIT 1;
```

Rows outlive a container, and the block is what scopes them — a previous run's
rows are anchored at a previous block. Scoping by time instead would be wrong in
the other direction: republishing is idempotent, so a restart at an unchanged tip
inserts nothing, and a time window would read that healthy state as a missing
snapshot. Which instance published is a separate question, answered by the log
window before the record is asked. Separately, a log assertion proves the live
fan-out still reaches the logger, which is a different path from the record and
so gets its own check. Verification resets its log window if either monitor container
restarts.

```bash
just verify-live
```

After reviewing the verification output, `just accept` repeats the fast and
live checks and atomically writes `${ECASH_DATA_ROOT}/.deployment-accepted`.
The marker records the network, repository SHA, image/source pins, discovered
slots, event contract and extractor-run capabilities, snapshot provenance, BMM poll
status, background Core-validation status, block-history status,
mutable-state-history status, and verified live block. Acceptance refuses to
run from a dirty Git checkout.

Timeouts can be adjusted in `.env`. `SNAPSHOT_RPC_WAIT_SECONDS` lets the
snapshot command remain parked while a recovering node keeps RPC in warmup,
instead of exiting and re-hashing the 9.5 GB file on every service retry.
`SNAPSHOT_ACTIVATION_WAIT_SECONDS` bounds recovery of the raw activation block
from a synced outbound peer. `WORKER_HEALTH_WAIT_SECONDS` bounds the wait for
both independent extractor workers to report a successful cycle. All
values in `VERSIONS.lock` are repository-owned pins and cannot be overridden
there. `just down` stops the stack without deleting `${ECASH_DATA_ROOT}`.

`HISTORY_WAIT_SECONDS` bounds only deployment verification. The extractor
always continues page by page until complete. `BIP300_MONITOR_BACKFILL_PAGE_BLOCKS`
defaults to 128 (maximum 512) and bounds one RPC/transaction; it never limits
total coverage. `BIP300_MONITOR_BACKFILL_PAGE_PAUSE_MS` defaults to 100.
`BIP300_MONITOR_REQUEST_TIMEOUT_SECONDS` defaults to 120 for deployment RPCs,
so cold historical pages can complete without removing the deadline.
`BIP300_MONITOR_TIP_POLL_INTERVAL_SECONDS` defaults to 30.
`BIP300_MONITOR_STREAM_STALL_TIMEOUT_SECONDS` independently controls live
stream liveness (default 60); it must be at least the tip-poll interval and is
not the unary RPC deadline.
Existing `.env` files must remove `BIP300_MONITOR_BACKFILL_MAX_BLOCKS`, which is
rejected to prevent the old silent truncation semantics.

Existing installations keep their conservative behavior until
`TRUST_ASSUMEUTXO_SNAPSHOT=true` is added to their `.env`; `just init` never
overwrites an existing file. New installations receive the reviewed setting
from `.env.example`.

## Network configuration

The current Betanet lock uses activation height `967680`, P2P port `8533`,
internal RPC port `8532`, and the public peers published by the node project.
Preflight checks every peer dynamically from the single locked list. The public
Esplora tip is informational and an outage there does not block verification.
The snapshot is the reviewed height-`935000` artifact whose source and
transformed hashes are both pinned in the lock.

Moving to another network requires an intentional change to the network lock.
The project name, generated node configuration, and fresh data root then follow
from `NETWORK_ID`. Never point a new network at an existing chainstate.

## Network exposure

Compose publishes no host ports. Node RPC, REST, ZMQ, enforcer gRPC, Postgres,
NATS, and NATS monitoring remain reachable only on the internal Docker network.
The node accepts no inbound peers. The enforcer authenticates with a shared RPC
cookie, and Postgres with a password `just init` generates into
`${ECASH_DATA_ROOT}/secrets/postgres-password`. Neither is stored in the
repository or in container arguments.

That secret is group-readable so the extractor can read it while keeping a UID
of its own: it runs as `10001:${PGID}`, and Postgres as `${PUID}:${PGID}`.

## Where the data lives

Postgres is the authoritative record. NATS stays as best-effort live fan-out to
the event logger, so a message lost there costs nothing once the row is
committed: the extractor writes to Postgres first and publishes afterwards. A
failed write is fatal; a failed publication is a warning.

That holds at startup too. Only Postgres has to be reachable for the extractor
to start: the publisher reconnects in the background, so an outage of NATS
degrades the fan-out rather than stopping the record. A malformed NATS
configuration is still fatal, because that is a mistake rather than an outage.

Two different gaps follow from that, and only one of them is about transport:

- **The extractor was down or a live stream stopped delivering.**
  `SubscribeEvents` does not replay history, so only a backfill can close the
  resulting hole. Every polled tip move queues reconciliation for all active
  slots. A stream that stays silent after that move fails the process within the
  dedicated stream-stall timeout, and the next start subscribes before
  snapshotting. The
  extractor walks the whole gap in bounded pages, stores each page and its
  cursor atomically, and resumes after interruption. Historical pages bypass
  NATS. A quiet tip never arms the stream watchdog.
- **A newly activated slot cannot open its stream or read its anchor tip.**
  Transient gRPC failures defer only that instance and retry on the 30-second
  tip cadence; existing slots keep running. The instance is re-read from the
  durable active set before retry, and its backfill closes the pre-subscription
  gap. A permanent API or data error remains fatal.
- **A live consumer missed a message.** Cosmetic, because the record already
  has it.

`just status` reports start, cursor, target, page size and percentage per slot.
The Compose limits are 512 MiB for the extractor and 1 GiB for Postgres so a bad
response cannot turn into host-wide memory pressure.

For an intentional Betanet record rebuild, `just reset-record betanet` first
creates a checksummed `pg_dump`, stops only the extractor and Postgres, moves
the old cluster and acceptance marker into a timestamped recoverable backup,
and creates an empty Postgres directory. It never removes the node or enforcer
directories. Run `just monitor-up` afterwards to migrate and start the full
import.

The first contract-v6 deployment must use that recoverable rebuild. Contract
v5 calculated `sidechain_instance.description_sha256d` over a different byte
sequence, so mixing old and corrected instance identities would make joins
ambiguous. The v6 extractor refuses to extend a pre-v6 dataset once it contains
sidechain instances. Keep the dump and moved cluster until v6 history and live
verification pass; this reset does not touch node, AssumeUTXO, or enforcer data.

### Rolling the monitor back from v6 to v5

Never start a v5 extractor against a record created or migrated by v6. The old
binary does not know the corrected description identity and cannot reconcile a
run left active by an unclean v6 stop; it can either mix incompatible hashes or
fail on the single-active-run index.

If rollback is required after `just reset-record betanet`, stop the bootstrap
and the complete Compose stack first. Preserve the current v6 Postgres directory
as a separate recoverable failure artifact, then restore the exact `cluster/`
and `deployment-accepted.before-reset` saved under the selected
`${ECASH_DATA_ROOT}/backups/postgres-<timestamp>/`. Only after that restore may
the reviewed v5 commit and lock be selected and started. A pin-only rollback is
invalid. Keep the pre-v6 backup for as long as v5 rollback remains supported,
not merely until the first successful v6 acceptance.

## Deploying a new monitor build

The Compose file and `VERSIONS.lock` describe one system and move together: an
image older than the Compose file that runs it ignores the Postgres settings
entirely and never refreshes its liveness file, so the containers come up and
stay unhealthy with nothing naming the real cause.

Merge to `main`, let CI publish the `sha-<commit>` images, promote their digests,
`MONITOR_IMAGE_COMMIT`, and `MONITOR_EVENT_CONTRACT_VERSION` in both locks, and
deploy after that. The checked-in lock intentionally remains on the last
published contract until those immutable v6 images exist; source code alone is
not a deployable promotion.
`just preflight` refuses to start when the monitor sources have moved since the
pinned commit, so the order is checked rather than remembered. Details in
[`docs/container-images.md`](../../docs/container-images.md).

## Knowing the monitor is alive

Neither monitor service exposes a port, so a restart policy only covers a
process that exits — not one that is running and no longer doing its work. Each
refreshes `/tmp/liveness` from something that only succeeds when it is healthy,
and its healthcheck fails once that file is older than three intervals:

- The **extractor** refreshes it on every successful tip read, which proves both
  that it is scheduling and that the enforcer is answering. A quiet chain still
  counts: nothing to observe is not the same as unable to observe.
- The **event logger** refreshes it on every event and, on a timer, after a
  round-trip to the NATS server. A quiet chain and a dead subscription both
  deliver no messages, and the round-trip is what separates them.

The node uses `restart: unless-stopped` so it can resume long-running sync and
background validation after a VM reboot. Every dependent service uses
`restart: on-failure`: crashes still recover, but a Docker daemon restart does
not start the enforcer and monitor out of order before the bootstrap checks the
node and snapshot again.

## Persistent layout

```text
${ECASH_DATA_ROOT}/
├── .deployment-accepted
├── config/
├── enforcer/
├── node/
├── postgres/
├── rpc-cookie/
├── secrets/
└── snapshots/
```

Exact commits, image digests, tool versions, network values, and snapshot
checksums are kept in [`VERSIONS.lock`](VERSIONS.lock).
