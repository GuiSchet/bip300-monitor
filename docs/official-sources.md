# Official sources: event contract 9

The monitor records only what the unmodified LayerTwo-Labs enforcer
(`proto/upstream/`, pinned in `VERSIONS.lock`) and the eCash node report. It
implements no BIP300 rules and infers no votes, expiries or treasury
transitions. This page lists every recorded kind, where it comes from and what
it does and does not prove. Record schema: 10.

## Byte order and amounts

- Every hash and txid, including the M6 identifier (`m6id`, the bundle txid),
  is stored in conventional display order. `bmm_commitment`/`critical_hash`
  (h*) are the raw bytes upstream returns.
- Amounts are integer satoshis (`u64`). The JSON payload keeps them exact;
  consumers must parse them without floating point.
- `block_work` is the work of one block; `cumulative_work` (absolute chain
  work) exists only on node headers.

## Kinds

| Kind | Source | Meaning |
|---|---|---|
| `chain_info` | `GetChainInfo`, at startup | Network and the six BIP300 thresholds plus activation height. Startup refuses an enforcer whose thresholds do not match the dataset's network preset. |
| `chain_tip` | `GetChainTip` (enforcer, startup) / `getblockchaininfo` (node, every poll) | The tip a source reported. |
| `block_connected` | `SubscribeEvents(slot)` live, `GetBlockInfo(slot)` backfill | Per-slot block fact: header, BMM commitment, deposits and withdrawal-bundle events. Recorded for every block from the instance's activation. |
| `block_disconnected` | `SubscribeEvents(slot)` | A live disconnect. Never backfilled; height unknown. |
| `mainchain_transition` | `SubscribeEvents(0)` | Global connect (1), disconnect (2) or subscription boundary (3). |
| `sidechain_proposals`, `active_sidechains`, `ctip`, `withdrawal_bundle_proposals` | unary state RPCs, read on every tip change | Point-in-time state. Unanchored; the snapshot group carries the observation window. |
| `bmm_requests` | `GetSeenBmmRequests(parent)`, every 5 s | A sample of the unconfirmed auction at one parent. |
| `mainchain_block` | node `getblockheader`/`getblock` | Verified raw block and header with absolute chain work. |
| `confirmed_bmm_fees` | node `getblock(hash, 3)` | Fees of observed bids that were committed in the block. |

## Snapshot consistency

Each state or BMM reading is bracketed by two tip reads:

- `tip_matched`: both reads returned the same tip. Not proof of atomicity:
  A → B → A between the reads is undetectable.
- `changed`: the tip moved; the payload is kept as evidence, never as state.
- `unknown`: no guarantee, used for BMM samples taken during the readiness
  grace after start (`BIP300_MONITOR_BMM_READINESS_GRACE_SECONDS`, default
  120 s). The official API exposes no mempool readiness.

Every reading is recorded. An unchanged value deduplicates to the same event
and gains another occurrence, so the latest occurrence says how recently the
value was confirmed. A `changed` reading never changes which sidechain instance
is current.

## Gaps

Upstream never replays events. Each subscription of the global stream records
a `mainchain_transition` with action 3. Its header is the tip read when
evidence resumes; `gap_start` is the last tip known before the cut (the
previous run's last tip, or the tip when the stream broke). Transitions in
between are unknown, not absent. The first subscription of a dataset has no
`gap_start`. Slot history is backfilled; global transitions are not.

## History and certification

`history_coverage` tracks per-scope backward walks (`block` per slot instance,
`mainchain_block` for the node). A block in `history_certified_block` has its
whole prefix proved down to the scope's start. Certification is not
canonicality: proofs on an orphaned branch remain. A page whose block facts
conflict with retained ones quarantines its scope (`last_error` names the
conflict) until an operator resolves it; live capture continues.

## Ordering for consumers

Every write takes one advisory lock before its first insert, so identity ids
are committed in increasing order. A consumer may page by `id > cursor`.

## Not available from the official sources

M1–M4/M7 coinbase messages and per-block vote deltas, treasury transitions
(CTIP before/after a block), the winning BMM bid, mempool readiness, a durable
event sequence and replay. The OP_DRIVECHAIN opcode is not reported; only the
preset's thresholds are checked.

## Capabilities

`dataset_manifest.capabilities` and each `extractor_run.capabilities` list what
a run can supply. Node evidence (`node_block_evidence`, `absolute_chain_work`,
`resumable_node_history`) exists only when a node RPC is configured.
