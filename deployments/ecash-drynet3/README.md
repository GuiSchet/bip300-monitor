# eCash Drynet3 deployment

Single-VM infrastructure for exercising `bip300-monitor` against the current
eCash/Drivechain dry-run network. This directory currently deploys the pinned
Drynet3 L1 node; the enforcer and observation pipeline are added in the next
reviewable stages.

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

Once the node reaches the active Drynet3 tip, run the operational acceptance
check:

```bash
just verify
```

It checks the running node, the exact Drynet3 activation block, initial
synchronization state, chainstate availability, and that Compose publishes no
host ports. Repository format and configuration checks run separately in
GitHub Actions.

Useful commands are listed by `just --list`. `just down` stops the containers
without deleting `${ECASH_DATA_ROOT}`.

## Network exposure

Compose publishes no host ports. RPC, REST, and ZMQ are reachable only by
containers on the deployment network. The node makes an outbound connection to
`drynet3.drivechain.dev:8337` and does not accept inbound peers.

## Persistent layout

```text
${ECASH_DATA_ROOT}/
├── node/
└── snapshots/
```

Exact commits, image digests, tool versions, and snapshot checksums are kept in
[`VERSIONS.lock`](VERSIONS.lock).
