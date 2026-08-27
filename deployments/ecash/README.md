# eCash deployment

Single-VM infrastructure for exercising `bip300-monitor` against a pinned
eCash/Drivechain network. The active lock targets **Alphanet** and deploys its
L1 node, validator enforcer, Postgres record, Core NATS transport, enforcer
extractor, and event logger. Network-specific values live in `VERSIONS.lock` and
the node configuration is generated from those values plus
`config/ecash.conf.template`. The scripts and Compose topology are shared so a
future Beta or Mainnet transition does not require another deployment copy.

Alphanet is experimental. Use a dedicated data directory and no real funds.

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
verifies its pinned size and SHA-256, and waits for the exact activation header
before loading it:

```bash
just snapshot
just status
```

Once the node reaches the active network tip and finishes validating the
AssumeUTXO history, start the pinned validator-only enforcer. Wallet and mining
services remain disabled:

```bash
just enforcer-up
just status
```

`initialblockdownload=false` only means the snapshot-backed chainstate can
serve the active tip. `just enforcer-up` also requires `getchainstates` to
return one fully validated chainstate, preventing the enforcer from reading
unavailable historical blocks.

Start the observation pipeline after the enforcer catches up:

```bash
just monitor-up
just verify
```

Verification asks the record, not the logs. It reads the block the newest
recorded snapshot is anchored to, then requires **exactly one** row at that block
for each snapshot kind, plus one CTIP and one withdrawal-bundle proposals row per
configured slot. Exactly one, not at least one: a second row at the same block
would mean the identity constraint stopped collapsing a republished snapshot,
which is the shape of a table that grows on every restart.

Rows outlive a container, and the block is what scopes them — a previous run's
rows are anchored at a previous block. Scoping by time instead would be wrong in
the other direction: republishing is idempotent, so a restart at an unchanged tip
inserts nothing, and a time window would read that healthy state as a missing
snapshot. Which instance published is a separate question, answered by the log
window before the record is asked. Separately, a log assertion proves the live
fan-out still reaches the logger, which is a different path from the record and
so gets its own check. Verification resets its log window if either monitor container
restarts.

Before accepting a VM, wait for a new network block and prove delivery for
every configured sidechain slot:

```bash
just verify-live
```

After reviewing the verification output, `just accept` repeats the fast and
live checks and atomically writes `${ECASH_DATA_ROOT}/.deployment-accepted`.
The marker records the network, repository SHA, image/source pins, and verified
live block. Acceptance refuses to run from a dirty Git checkout.

Timeouts can be adjusted in `.env`. All values in `VERSIONS.lock` are
repository-owned pins and cannot be overridden there. `just down` stops the
stack without deleting `${ECASH_DATA_ROOT}`.

## Network configuration

The current Alphanet lock uses fork height `963648`, P2P port `8533`, internal
RPC port `8532`, and the public peers published by the node project. Preflight
checks every peer dynamically from the single locked list. The public Esplora
tip is informational and an outage there does not block verification. The
snapshot is the fork-point `utxo-963648.dat` listed in the upstream
[`SHA256SUMS`](https://data.drivechain.dev/alphanet/SHA256SUMS).

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

- **The extractor was down.** `SubscribeEvents` does not replay history, so a
  restart leaves a real hole that only a backfill can close. How much of it can
  be closed is bounded by `BIP300_MONITOR_BACKFILL_MAX_BLOCKS`; past that the
  extractor records a window ending at the tip and **warns** that the rest was
  skipped, so a long outage is a decision to raise the bound rather than silent
  data loss.
- **A live consumer missed a message.** Cosmetic, because the record already
  has it.

## Deploying a new monitor build

The Compose file and `VERSIONS.lock` describe one system and move together: an
image older than the Compose file that runs it ignores the Postgres settings
entirely and never refreshes its liveness file, so the containers come up and
stay unhealthy with nothing naming the real cause.

Merge to `main`, let CI publish the `sha-<commit>` images, promote their digests
and `MONITOR_IMAGE_COMMIT` in `VERSIONS.lock`, and deploy after that.
`just preflight` refuses to start when the monitor sources have moved since the
pinned commit, so the order is checked rather than remembered. Details in
[`docs/container-images.md`](../../docs/container-images.md).

## Knowing the monitor is alive

Neither monitor service exposes a port, so `restart: unless-stopped` on its own
only covers a process that exits — not one that is running and no longer doing
its work. Each refreshes `/tmp/liveness` from something that only succeeds when
it is healthy, and its healthcheck fails once that file is older than three
intervals:

- The **extractor** refreshes it on every successful tip read, which proves both
  that it is scheduling and that the enforcer is answering. A quiet chain still
  counts: nothing to observe is not the same as unable to observe.
- The **event logger** refreshes it on every event and, on a timer, after a
  round-trip to the NATS server. A quiet chain and a dead subscription both
  deliver no messages, and the round-trip is what separates them.

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
