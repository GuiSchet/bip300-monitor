# Monitor event schema

The files in this directory define the stable protobuf contract published by
`bip300-monitor`. They are deliberately separate from `proto/upstream`, which
contains the pinned enforcer API used as an input.

`event.proto` defines the top-level envelope. Like `peer-observer`, every
payload carries the time at which the monitor constructed it and a `oneof`
identifying the extractor. `enforcer_extractor.proto` contains normalized
events derived from the enforcer's read-only validator API.

All envelopes are published to the stable Core NATS subject
`bip300.enforcer`. Consumers select the concrete payload through the protobuf
`oneof`; the subject is intentionally not split by sidechain slot or event
variant in the first pilot.

## Semantics

- `Event.timestamp` is the observation time in Unix milliseconds. It is not a
  Bitcoin block timestamp.
- `Event.observed_at_block` is the mainchain block the observation is anchored
  to, and it is what makes an event joinable to a height. For a block event it
  is the block the event is about. For a state snapshot it is the enforcer's
  tip when the state was read — an anchor, **not** a claim that the state is
  exactly as of that height, because reading state is a poll and the enforcer
  can advance between one field and the next.
- `ObservedBlock.height` is absent when the source did not report one. A
  disconnect names only the block being disconnected, so its height has to be
  recovered from the connect that preceded it. An absent height is never
  published as zero.
- Hashes and transaction IDs are decoded from the enforcer's `ReverseHex`
  values and stored as 32 bytes in conventional display order.
- Fields documented as consensus-encoded preserve the byte order and any
  length prefix supplied by the enforcer's `ConsensusHex` value.
- A sidechain description is stored with that consensus `CompactSize` length
  prefix, but its BIP300 identity is SHA256d of the decoded description bytes
  only. `description_hash` is stored in conventional display order and is
  verified against every M1 and proposal response that already carries the
  upstream hash. Active-sidechain responses do not carry it, so the monitor
  calculates and includes it in contract v6.
- `Bip300BlockDelta` is global, one fact per mainchain block. It preserves each
  exact matching coinbase `scriptPubKey`, its `vout`, parsed fields and whether
  the enforcer accepted it. Resolved effects and treasury transitions come
  from the enforcer's persisted block diff, not from monitor-side inference.
- Unknown protobuf enum numbers remain their raw `i32` values in the normalized
  event. Consumers must not collapse a future number into today's
  `UNSPECIFIED` meaning.
- Proposal, active-sidechain, CTIP, and withdrawal-bundle-proposal messages are
  snapshots, not deltas. They are published once at startup and then again
  whenever a connected or disconnected block changed their value, so a consumer
  sees the latest state without diffing. A stable observation after a changed
  window republishes even an identical payload. Changed windows are retried
  after 30 seconds even without a new tip. `ChainInfo` and `ChainTip` are
  published only at startup.
- Because a refresh is a poll rather than a per-block query, a snapshot
  describes the state at the moment it was read. Under fast blocks two heights
  can coalesce into one refresh, so consecutive snapshots are consecutive
  observations, not consecutive blocks. This is a property of the enforcer API,
  not of the monitor: `GetCtip`, `GetSidechains`, `GetSidechainProposals` and
  `GetWithdrawalBundleProposals` only answer for the current tip, and no RPC
  answers "the state at block X", so a value that changed and reverted inside one
  coalesced window leaves no observation behind.
- A snapshot's `observed_at_block` is the tip it was read against, which under a
  moving chain can be a later block than the one whose arrival triggered the
  read — and can therefore be a block with no `BlockConnected` of its own yet.
- PostgreSQL wraps every unary snapshot in a `snapshot_group` containing
  `tip_before`, `tip_after`, the read interval, attempt count, chain revisions
  and either `stable` or `changed`. Three bounded attempts are made; a moving
  chain is persisted as `changed`, anchored to `tip_before` for diagnostics.
  Join `state_snapshot_stable` for state at a proved anchor: it requires equal
  non-null revisions and equal tips. Consistency belongs to each occurrence,
  not to the deduplicated payload fact.
- A live BMM auction has a stricter rule: the extractor reads the tip, asks
  `GetSeenBmmRequests` for exactly that parent, and reads the tip again. It
  discards and retries a response when the parent moved and persists only a
  `stable` group whose two tips, event anchor, and payload parent are equal,
  with an unchanged persisted revision to reject A → B → A races. A non-empty
  mempool session and ready generation are required; a syncing or disabled
  mempool is never represented as a successful empty auction.
  The BMM worker reads the current tip on its own five-second sampling cycle.
  The independent 30-second tip poll can also wake it early after that poll
  detects a move, but this notification is not immediate and correctness does
  not depend on it.
- `BlockDisconnected` says a block left the chain; its `BlockConnected` fact
  remains immutable. A slot-independent `MainchainTransition` stream records
  connects, disconnects and subscription boundaries with a session/sequence.
  Sequence gaps, stream failures and process restarts leave durable entries in
  `observation_failure`; offline transitions cannot be reconstructed as live
  observations. Backfill repairs canonical history to a certified ancestor,
  preserving the previous coverage proof until the replacement completes.
  `event_observation` retains captures and `tip_observation` retains observed
  tips. Capture order is not a consensus ordering across RPCs and streams.
  Observatory materializes canonical membership from linked parent hashes;
  operator verification independently checks the node's selected chain.
- `WithdrawalBundleProposalsSnapshot` carries the bundles still being voted on.
  Its `vote_count` is read against
  `Bip300Constants.withdrawal_bundle_inclusion_threshold` and its
  `proposal_height` against `withdrawal_bundle_max_age`. The terminal outcome of
  a bundle stays where it was, in `BlockConnected.events`.
- Block events are scoped to the explicitly configured sidechain slot because
  the upstream subscription is also per slot.
- Block event order is preserved within a sidechain slot. A reorganization is
  represented by the enforcer's ordered disconnect and connect events.
- There is no global ordering guarantee across sidechain slots. The same
  mainchain block is published once per configured slot with that slot's
  filtered data.
- An absent `CtipSnapshot.ctip` means that the sidechain has no current CTIP.
- Startup can capture a block in both backfill and the buffered live stream.
  Consumers that need unique facts use `event`; consumers that need delivery,
  replay or reorg order join `event_observation` to `event` and order by
  `(run_id, capture_seq)`.

Conversions reject missing required input fields, malformed hex, and hashes
that are not exactly 32 bytes. This prevents incomplete upstream responses from
being published as valid-looking zero values.

`BlockHeader.block_work` is per-block work; `cumulative_work` is absolute
chain work, both 32-byte consensus-encoded values. Contract v7 reserves the old
misnamed `chain_work` field instead of changing its wire meaning.

`ConfirmedBmmRequest.fee_sats` remains absent in historical deltas. Optional
`ConfirmedBmmFees` facts independently enrich confirmed requests using node
transaction fees in exact satoshis. Missing historical prevouts yield an
explicit unknown reason and a resumable retry, never a zero fee. Live mempool
samples remain the source of `bid_sats`. JSON consumers must parse `u64` values
losslessly; JavaScript numbers cannot represent every value above 2^53.

Core NATS transport is at-most-once and non-durable. A successful bounded client
flush confirms that its transport write buffer was emptied; it does not confirm
that the server processed the bytes or that any consumer received or persisted
the event. The protobuf schema therefore defines observation data, not an
exactly-once delivery protocol.

The stored `event.envelope` is this normalized monitor protobuf, not the
upstream wire response. Raw consensus-relevant fields are included explicitly;
the envelope cannot recreate fields the input API never exposed.

When evolving the schema, add new fields with new numbers. Never change the
meaning of an existing field number or reuse a removed number.
