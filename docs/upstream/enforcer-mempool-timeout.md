# Draft upstream issue: mempool sync timeout exits the enforcer

Not filed. Target: LayerTwo-Labs/bip300301_enforcer, observed at
`1753fc0c23863bcb39c681e1cfaea2705613516f` with cusf-enforcer-mempool
`185e439807023902252ba85ca908b7133a2fb062`.

## Title

`ApplySyncActionTimeout` in the mempool sync task terminates the enforcer
instead of triggering a re-sync

## Body

With `--enable-mempool`, a `SyncTaskError::ApplySyncActionTimeout` from
cusf-enforcer-mempool (`lib/mempool/sync/task.rs`) reaches
`MempoolTask::is_resyncable` (`app/error.rs`). That match lists sequence-stream
and transport errors as resyncable and ends in `_ => false`, so the timeout is
treated as fatal and the process exits. The gRPC and block-template servers go
down with it.

The timeout means a sync action waited too long, typically while the node is
busy. Like a dropped ZMQ notification, a fresh mempool sync clears it. The
`is_resyncable` documentation already states that "re-syncing is strictly
better than exiting the process".

**Impact for API consumers.** Every exit ends all `SubscribeEvents` streams.
The enforcer does not replay events, so transitions during the restart are
lost to subscribers. A Betanet deployment saw 27–105 enforcer restarts.

**Suggested change.** Add `SyncTaskError::ApplySyncActionTimeout(_)` (and its
`InitialSync` counterpart, if reachable) to the resyncable arms, with a test
next to `a_transport_error_is_resyncable`.

**Reproduction.** Run with `--enable-mempool` against a node under load (for
example during IBD or a large reorg), until a sync action exceeds the timeout.
The enforcer logs the timeout and exits with the mempool task error.

## Monitor-side handling meanwhile

Each restart is recorded as a subscription boundary (`mainchain_transition`
action 3) with the interval whose transitions are unknown, and BMM samples in
the readiness grace after start are marked `unknown`.
