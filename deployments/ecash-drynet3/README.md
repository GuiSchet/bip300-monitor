# eCash Drynet3 deployment

Single-VM infrastructure for exercising `bip300-monitor` against the current
eCash/Drivechain dry-run network. This directory deploys a pinned Drynet3 L1
node, validator enforcer, Core NATS transport, enforcer extractor, and event
logger.

Drynet3 is an experimental fork network. It shares Bitcoin mainnet's network
magic, so this deployment uses a dedicated data directory and connects only to
the documented Drynet3 peer. Do not use real funds.

## Requirements

- Ubuntu 24.04 on `x86_64`.
- Docker Engine with the Compose plugin.
- `curl`, `jq`, `sha256sum`, and `tar`.
- At least 16 GiB RAM and 1.2 TB free disk; 2 TB is recommended.

Install the pinned `just` command runner, then initialize the deployment:

```bash
./scripts/install-just.sh
just init
```

Review `.env`, then start the node:

```bash
just up
just status
```

The AssumeUTXO bootstrap is a separate, explicit operation because it downloads
about 9.5 GB and waits until the node knows the snapshot block header:

```bash
just snapshot
just status
```

Once the node reaches the active Drynet3 tip and finishes validating the
AssumeUTXO history, start the pinned validator-only enforcer. Wallet and mining
services are deliberately disabled:

```bash
just enforcer-up
just status
```

`initialblockdownload=false` only means the snapshot-backed chainstate can
serve the active tip. It does not mean every historical block is available.
`just status` reports `history_ready=true` only after `getchainstates` returns
one fully validated chainstate; `just enforcer-up` refuses to start before that
point so the enforcer cannot enter a restart loop on unavailable blocks.

The first enforcer synchronization can take time. It reads the node's block
files directly when available and uses RPC for anything still missing. Once it
has caught up, start the observation pipeline:

```bash
just monitor-up
```

This command verifies the node and enforcer first, starts Core NATS, waits for
the event logger subscription, and only then starts the extractor. That ordering
ensures the logger receives the extractor's initial snapshot. It finishes by
running the operational acceptance check. You can repeat that check later with:

```bash
just verify
```

It checks the exact Drynet3 activation block, node/enforcer tip agreement, NATS
health, both monitor client connections, the `bip300.enforcer` subscription,
and evidence that the logger received at least one normalized event. Full event
JSON is enabled by default; inspect it with `just logs event-logger`.

Before accepting a VM deployment, exercise the continuous stream with:

```bash
just verify-live
```

It first runs the fast verification, then waits for a new Drynet3 block and
requires the extractor and logger to report that block for every configured
sidechain slot. `LIVE_BLOCK_WAIT_SECONDS` bounds the wait for chain activity;
`LIVE_EVENT_WAIT_SECONDS` bounds its delivery through every configured slot. A
timeout before a new block means the live path was not exercised.

`SNAPSHOT_HEADER_WAIT_SECONDS`, `ENFORCER_SYNC_WAIT_SECONDS`,
`MONITOR_STARTUP_WAIT_SECONDS`, and `MONITOR_EVENT_WAIT_SECONDS` control startup
and the fast snapshot verification. `LIVE_BLOCK_WAIT_SECONDS` controls the wait
for chain activity, while `LIVE_EVENT_WAIT_SECONDS` independently bounds live
delivery. Repository format and configuration checks run separately in GitHub
Actions. Values in `VERSIONS.lock` are repository-owned pins and cannot be
overridden from `.env`.

Useful commands are listed by `just --list`. `just down` stops the containers
without deleting `${ECASH_DATA_ROOT}`.

## Network exposure

Compose publishes no host ports. Node RPC, REST, ZMQ, enforcer gRPC, NATS, and
NATS monitoring are reachable only by containers on the deployment network.
The node makes an outbound connection to `drynet3.drivechain.dev:8337` and does
not accept inbound peers. The enforcer authenticates with a shared RPC cookie;
no RPC password is stored in the repository or container arguments.

This pilot uses anonymous Core NATS without JetStream. Messages exist only in
flight: a server or consumer outage can require restarting the extractor to
republish its snapshot. The network isolation is therefore part of the security
boundary, and durable delivery remains future work.

## Persistent layout

```text
${ECASH_DATA_ROOT}/
├── enforcer/
├── node/
├── rpc-cookie/
└── snapshots/
```

Exact commits, image digests, tool versions, and snapshot checksums are kept in
[`VERSIONS.lock`](VERSIONS.lock).
