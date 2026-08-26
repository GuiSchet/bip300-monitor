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

The logger subscribes before the extractor starts. Verification requires a fresh
event of each semantic snapshot type, plus a CTIP and a withdrawal-bundle
proposals event for every configured slot. `chain_info` and `chain_tip` must
appear exactly once because they are published only at startup; the remaining
kinds are republished whenever a block changes them, so more than one is
expected. Verification resets its log window if either monitor container
restarts, and live block events cannot satisfy the snapshot check.

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

Two different gaps follow from that, and only one of them is about transport:

- **The extractor was down.** `SubscribeEvents` does not replay history, so a
  restart leaves a real hole that only a backfill can close.
- **A live consumer missed a message.** Cosmetic, because the record already
  has it.

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
