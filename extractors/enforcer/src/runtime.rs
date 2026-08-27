//! Continuous enforcer event extraction.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::future::try_join_all;
use futures_util::{Stream, StreamExt};
use shared::liveness::Heartbeat;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::event::MonitorEvent;
use shared::protobuf::event::{Event, ObservedBlock};
use shared::recorder::Recorder;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tonic::{Status, Streaming};

use crate::backfill;
use crate::config::Args;
use crate::event::envelope;
use crate::proto::mainchain;
use crate::snapshot::{self, InitialSnapshot, record_snapshot};
use crate::state;
use crate::{EnforcerClient, convert};

type EventStream = Streaming<mainchain::SubscribeEventsResponse>;
type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
const NATS_CLIENT_NAME: &str = "bip300-monitor-enforcer-extractor";
/// Names the writer of every row this extractor records.
const RECORD_SOURCE: &str = "enforcer";

trait StartupSource: Clone + Send {
    type Stream: Send;

    fn subscribe_events(&mut self, sidechain: u8) -> SourceFuture<'_, Self::Stream>;
    fn current_tip(&mut self) -> SourceFuture<'_, ObservedBlock>;
    fn collect_snapshot<'a>(
        &'a mut self,
        sidechains: &'a [u8],
    ) -> SourceFuture<'a, InitialSnapshot>;
}

impl StartupSource for EnforcerClient {
    type Stream = EventStream;

    fn subscribe_events(&mut self, sidechain: u8) -> SourceFuture<'_, Self::Stream> {
        Box::pin(EnforcerClient::subscribe_events(self, sidechain))
    }

    fn current_tip(&mut self) -> SourceFuture<'_, ObservedBlock> {
        Box::pin(snapshot::current_tip(self))
    }

    fn collect_snapshot<'a>(
        &'a mut self,
        sidechains: &'a [u8],
    ) -> SourceFuture<'a, InitialSnapshot> {
        Box::pin(snapshot::collect_snapshot(self, sidechains))
    }
}

/// Source of the mainchain tip, polled independently of any slot.
trait TipSource: Send {
    fn read_tip(&mut self) -> SourceFuture<'_, ObservedBlock>;
}

impl TipSource for EnforcerClient {
    fn read_tip(&mut self) -> SourceFuture<'_, ObservedBlock> {
        Box::pin(snapshot::current_tip(self))
    }
}

/// Source of the enforcer state that has to be re-read as the tip advances.
trait StateSource: Send {
    fn collect_state<'a>(&'a mut self, sidechains: &'a [u8]) -> SourceFuture<'a, state::Reading>;
}

impl StateSource for EnforcerClient {
    fn collect_state<'a>(&'a mut self, sidechains: &'a [u8]) -> SourceFuture<'a, state::Reading> {
        Box::pin(state::collect(self, sidechains))
    }
}

struct PreparedStartup {
    recorder: Recorder,
    client: EnforcerClient,
    /// Resolved slots, which may have been discovered rather than configured.
    sidechains: Vec<u8>,
    streams: Vec<(u8, EventStream)>,
    snapshot: InitialSnapshot,
    tip_before_snapshot: Vec<u8>,
    tip_after_snapshot: Vec<u8>,
}

struct PreparedObservation<S> {
    streams: Vec<(u8, S)>,
    snapshot: InitialSnapshot,
    tip_before_snapshot: Vec<u8>,
    tip_after_snapshot: Vec<u8>,
}

/// Publish an initial snapshot and then monitor every configured sidechain.
pub async fn run(args: Args, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    args.validate()
        .context("validating extractor configuration")?;

    let startup = prepare_startup(&args);
    tokio::pin!(startup);
    let prepared = tokio::select! {
        biased;
        () = wait_for_shutdown(&mut shutdown_rx) => {
            tracing::info!("shutdown requested during extractor startup");
            return Ok(());
        }
        result = &mut startup => result?,
    };

    let PreparedStartup {
        recorder,
        client,
        sidechains,
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    } = prepared;

    if !snapshot_tips_are_consistent(
        &tip_before_snapshot,
        &snapshot.anchor.hash,
        &tip_after_snapshot,
    ) {
        tracing::warn!(
            tip_before = %hex::encode(&tip_before_snapshot),
            snapshot_tip = %hex::encode(&snapshot.anchor.hash),
            tip_after = %hex::encode(&tip_after_snapshot),
            "mainchain tip changed while collecting the initial snapshot; \
             buffered live events may duplicate snapshot state"
        );
    }

    record_snapshot(&recorder, &snapshot)
        .await
        .context("recording the initial enforcer snapshot")?;
    tracing::info!(
        sidechain_count = sidechains.len(),
        "published initial enforcer snapshot"
    );

    if *shutdown_rx.borrow() {
        tracing::info!("shutdown requested after the initial snapshot");
        return Ok(());
    }

    // Before any live event is forwarded, recover whatever passed while the
    // extractor was not subscribed. The live streams are already open, so this
    // only competes with buffered events, and the identity of an observation
    // makes an overlap idempotent rather than duplicated.
    let mut backfill_client = client.clone();
    for sidechain in &sidechains {
        backfill::run(
            &mut backfill_client,
            &recorder,
            *sidechain,
            &snapshot.anchor,
            args.backfill_max_blocks,
        )
        .await
        .with_context(|| format!("backfilling sidechain {sidechain}"))?;

        if *shutdown_rx.borrow() {
            tracing::info!("shutdown requested during the backfill");
            return Ok(());
        }
    }

    // Every slot worker reports the blocks it sees here, so one state worker can
    // re-read the mutable enforcer state once per tip change instead of once per
    // slot. A tip worker reports there too, and it is what keeps the channel
    // open: with no slot resolved there is no slot worker, and a state worker
    // whose senders are all gone stops on its own — which used to end the whole
    // process with a success code and no work done.
    let snapshot_block = snapshot.anchor.hash.clone();
    let (block_tx, block_rx) = watch::channel(snapshot.anchor.hash.clone());
    let tracker = state::Tracker::new(snapshot.state);

    let mut workers = JoinSet::new();
    for (sidechain, stream) in streams {
        workers.spawn(monitor_sidechain(
            stream,
            recorder.clone(),
            sidechain,
            block_tx.clone(),
            shutdown_rx.clone(),
        ));
    }
    workers.spawn(monitor_tip(
        client.clone(),
        args.tip_poll_interval(),
        Heartbeat::new(args.liveness_file.clone()),
        block_tx,
        shutdown_rx.clone(),
    ));
    workers.spawn(monitor_state(
        client,
        recorder,
        sidechains.clone(),
        tracker,
        snapshot_block,
        block_rx,
        shutdown_rx,
    ));

    supervise_workers(workers).await?;
    tracing::info!("enforcer extractor stopped");
    Ok(())
}

/// Slots to observe: the configured list, or the enforcer's active sidechains
/// when none was configured.
async fn resolve_sidechains(client: &mut EnforcerClient, configured: &[u8]) -> Result<Vec<u8>> {
    if !configured.is_empty() {
        return Ok(configured.to_vec());
    }

    let payload = convert::active_sidechains(client.get_sidechains().await?)?;
    let mut discovered = state::active_slots(&payload)
        .context("expected an active-sidechains snapshot while discovering slots")?;
    discovered.sort_unstable();
    discovered.dedup();

    if discovered.is_empty() {
        // Not an error: a network before any activation genuinely has none. It
        // is still worth saying plainly, because the alternative reading is
        // that the monitor is broken.
        tracing::warn!(
            "no sidechain is active and no slot was configured; \
             recording chain state only"
        );
    } else {
        tracing::info!(
            sidechains = ?discovered,
            "discovered the active sidechain slots"
        );
    }
    Ok(discovered)
}

async fn prepare_startup(args: &Args) -> Result<PreparedStartup> {
    let recorder = Recorder::connect(
        &args.postgres,
        &args.nats,
        Subject::Enforcer,
        RECORD_SOURCE,
        NATS_CLIENT_NAME,
    )
    .await
    .context("connecting the event recorder")?;

    let mut client = EnforcerClient::connect(&args.enforcer_endpoint, args.request_timeout())
        .await
        .context("connecting the enforcer client")?;

    let sidechains = resolve_sidechains(&mut client, &args.sidechains)
        .await
        .context("resolving the sidechain slots to observe")?;

    let observation = prepare_observation(&mut client, &sidechains)
        .await
        .context("preparing enforcer subscriptions and initial snapshot")?;
    let PreparedObservation {
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    } = observation;

    Ok(PreparedStartup {
        recorder,
        client,
        sidechains,
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    })
}

async fn prepare_observation<C>(
    client: &mut C,
    sidechains: &[u8],
) -> Result<PreparedObservation<C::Stream>>
where
    C: StartupSource,
{
    let subscriptions = sidechains.iter().copied().map(|sidechain| {
        let mut subscription_client = client.clone();
        async move {
            let stream = subscription_client
                .subscribe_events(sidechain)
                .await
                .with_context(|| format!("subscribing to sidechain {sidechain} events"))?;
            tracing::info!(sidechain, "opened live enforcer event stream");
            Ok::<_, anyhow::Error>((sidechain, stream))
        }
    });
    let streams = try_join_all(subscriptions)
        .await
        .context("opening all sidechain subscriptions")?;

    let tip_before_snapshot = client
        .current_tip()
        .await
        .context("reading the mainchain tip before collecting the snapshot")?
        .hash;
    let snapshot = client
        .collect_snapshot(sidechains)
        .await
        .context("collecting the initial enforcer snapshot")?;
    let tip_after_snapshot = client
        .current_tip()
        .await
        .context("reading the mainchain tip after collecting the snapshot")?
        .hash;

    Ok(PreparedObservation {
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    })
}

fn snapshot_tips_are_consistent(tip_before: &[u8], snapshot_tip: &[u8], tip_after: &[u8]) -> bool {
    tip_before == snapshot_tip && snapshot_tip == tip_after
}

async fn monitor_sidechain(
    stream: EventStream,
    recorder: Recorder,
    sidechain: u8,
    block_tx: watch::Sender<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!(sidechain, "started sidechain event worker");

    forward_stream(sidechain, stream, shutdown_rx, move |event| {
        let recorder = recorder.clone();
        let block_tx = block_tx.clone();
        async move {
            log_published_event(sidechain, &event);
            // Announced only after the record holds the event, so the block that
            // triggers a refresh is always one the record already has. The
            // refresh then anchors itself to the tip it reads, which under fast
            // blocks can be a later one -- see `state::collect`.
            recorder
                .record(event.clone())
                .await
                .with_context(|| format!("recording a live event for sidechain {sidechain}"))?;
            announce_block(&block_tx, sidechain, &event);
            Ok(())
        }
    })
    .await?;

    tracing::info!(sidechain, "stopped sidechain event worker");
    Ok(())
}

/// Report the block a published live event refers to, so the state worker can
/// refresh. A send failure only means the state worker already stopped, which
/// its own supervision reports; it must not fail the slot worker.
fn announce_block(block_tx: &watch::Sender<Vec<u8>>, sidechain: u8, event: &Event) {
    let Some(anchor) = event.observed_at_block.as_ref() else {
        tracing::warn!(
            sidechain,
            "published a live event without an observation anchor"
        );
        return;
    };
    let _ = block_tx.send(anchor.hash.clone());
}

/// Report every change of the mainchain tip, independently of any slot.
///
/// The live streams already report every block a slot sees, so on the usual path
/// this only confirms what a slot worker just said and the state worker's own
/// deduplication drops it. It earns its place in the two cases the streams
/// cannot cover: no slot is resolved, so there is no stream to report anything;
/// and a stream that stops delivering without closing, which would otherwise
/// freeze the refresh with nothing in the log to say so.
///
/// A failed read is not fatal. The record is untouched by a poll, and the state
/// worker still has the slot workers: killing the extractor because one unary
/// call timed out would be a worse trade than waiting for the next tick.
async fn monitor_tip(
    client: EnforcerClient,
    interval: Duration,
    heartbeat: Heartbeat,
    block_tx: watch::Sender<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!(
        interval_seconds = interval.as_secs(),
        liveness_file = ?heartbeat.path(),
        "started mainchain tip worker"
    );

    announce_tip_changes(client, interval, heartbeat, block_tx, shutdown_rx).await?;

    tracing::info!("stopped mainchain tip worker");
    Ok(())
}

async fn announce_tip_changes<S>(
    mut source: S,
    interval: Duration,
    heartbeat: Heartbeat,
    block_tx: watch::Sender<Vec<u8>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()>
where
    S: TipSource,
{
    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => return Ok(()),
            () = tokio::time::sleep(interval) => {}
        }

        match source.read_tip().await {
            Ok(tip) => {
                // A read that succeeded is the proof the healthcheck wants: the
                // process is scheduling and the enforcer is answering. A tip
                // that has not moved still counts, because a quiet chain is not
                // an unhealthy extractor.
                heartbeat.beat();

                // Only a move is reported. Re-announcing the same hash would
                // still mark the channel changed, and the state worker would
                // wake once per tick for nothing.
                if *block_tx.borrow() != tip.hash {
                    tracing::debug!(
                        block_hash = %hex::encode(&tip.hash),
                        height = ?tip.height,
                        "the mainchain tip moved"
                    );
                    // A send failure only means the state worker already
                    // stopped, which its own supervision reports.
                    let _ = block_tx.send(tip.hash);
                }
            }
            Err(error) => tracing::warn!(
                error = %format!("{error:#}"),
                "could not read the mainchain tip; retrying on the next tick"
            ),
        }
    }
}

/// Re-read the mutable enforcer state on every tip change and publish what
/// changed.
async fn monitor_state(
    client: EnforcerClient,
    recorder: Recorder,
    sidechains: Vec<u8>,
    tracker: state::Tracker,
    snapshot_block: Vec<u8>,
    block_rx: watch::Receiver<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!("started enforcer state worker");

    refresh_on_new_blocks(
        client,
        sidechains,
        tracker,
        snapshot_block,
        block_rx,
        shutdown_rx,
        move |anchor: ObservedBlock, payloads: Vec<events::EnforcerEvent>| {
            let recorder = recorder.clone();
            async move {
                let published = payloads.len();
                let events = payloads
                    .into_iter()
                    .map(|payload| {
                        log_state_event(&payload);
                        envelope(payload, anchor.clone())
                    })
                    .collect::<Result<Vec<_>>>()?;
                recorder
                    .record_batch(events)
                    .await
                    .context("recording refreshed enforcer state")?;
                tracing::info!(published, "published refreshed enforcer state");
                Ok(())
            }
        },
    )
    .await?;

    tracing::info!("stopped enforcer state worker");
    Ok(())
}

async fn refresh_on_new_blocks<S, P, F>(
    mut source: S,
    sidechains: Vec<u8>,
    mut tracker: state::Tracker,
    snapshot_block: Vec<u8>,
    mut block_rx: watch::Receiver<Vec<u8>>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut publish: P,
) -> Result<()>
where
    S: StateSource,
    P: FnMut(ObservedBlock, Vec<events::EnforcerEvent>) -> F,
    F: Future<Output = Result<()>>,
{
    // The block the initial snapshot describes. It is taken as an argument
    // rather than read from the channel because a slot worker can report a newer
    // block before this worker first polls, and reading the channel here would
    // silently mark that block as already refreshed. Later values are whatever
    // block a slot worker last published an event for, which during a reorg is
    // the disconnected block rather than a tip.
    let mut refreshed_at = snapshot_block;

    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => return Ok(()),
            result = block_rx.changed() => {
                if result.is_err() {
                    // Nothing holds a sender any more, so no further tip change
                    // can arrive. In the running extractor the tip worker keeps
                    // one open for as long as the process lives, so this is
                    // reached only once every reporter is already gone.
                    return Ok(());
                }
            }
        }

        let block = block_rx.borrow_and_update().clone();
        // Each configured slot reports the same mainchain block, and startup can
        // replay the snapshot tip. Only the first report of a block refreshes.
        if block == refreshed_at {
            continue;
        }

        // A failed refresh is fatal for the same reason a failed publication is:
        // silently skipping it would leave a gap that looks like "nothing
        // changed". The deployment restarts the extractor, which republishes the
        // whole snapshot.
        let reading = source.collect_state(&sidechains).await.with_context(|| {
            format!("refreshing enforcer state at block {}", hex::encode(&block))
        })?;
        refreshed_at = block;

        let changed = tracker.take_changed(reading.payloads)?;
        report_unobserved_slots(&sidechains, &changed);
        if changed.is_empty() {
            continue;
        }
        publish(reading.anchor, changed)
            .await
            .context("publishing refreshed enforcer state")?;
    }
}

async fn forward_stream<S, P, F>(
    sidechain: u8,
    mut stream: S,
    mut shutdown_rx: watch::Receiver<bool>,
    mut publish: P,
) -> Result<()>
where
    S: Stream<Item = Result<mainchain::SubscribeEventsResponse, Status>> + Unpin,
    P: FnMut(Event) -> F,
    F: Future<Output = Result<()>>,
{
    loop {
        let response = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => return Ok(()),
            response = stream.next() => {
                match response {
                    Some(Ok(response)) => response,
                    Some(Err(error)) => {
                        return Err(error).with_context(|| {
                            format!("receiving a live event for sidechain {sidechain}")
                        });
                    }
                    None => {
                        bail!("sidechain {sidechain} event stream ended unexpectedly");
                    }
                }
            }
        };

        let payload = convert::subscription_event(sidechain, response)
            .with_context(|| format!("converting a live event for sidechain {sidechain}"))?;
        let anchor = state::block_anchor(&payload)
            .with_context(|| format!("anchoring a live event for sidechain {sidechain}"))?;
        let event = envelope(payload, anchor)?;
        publish(event)
            .await
            .with_context(|| format!("forwarding a live event for sidechain {sidechain}"))?;
    }
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn supervise_workers(mut workers: JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                abort_and_drain(&mut workers).await;
                return Err(error);
            }
            Err(error) => {
                abort_and_drain(&mut workers).await;
                return Err(error).context("joining a sidechain subscription task");
            }
        }
    }
    Ok(())
}

async fn abort_and_drain(workers: &mut JoinSet<Result<()>>) {
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

fn enforcer_payload(event: &Event) -> Result<&events::EnforcerEvent> {
    match event.monitor_event.as_ref() {
        Some(MonitorEvent::Enforcer(payload)) => Ok(payload),
        None => bail!("event envelope does not contain a monitor event"),
    }
}

/// Warn about a sidechain that activated into a slot nobody is subscribed to.
///
/// Subscriptions are opened once, at startup, so a slot that activates later is
/// invisible until a restart. Spawning a worker for it mid-flight would be the
/// obvious fix, but activation takes tens of thousands of blocks of miner ACKs,
/// and a slot set that flapped through a reorg would turn dynamic
/// re-subscription into a restart loop. Saying so loudly is the honest trade:
/// the gap becomes visible instead of silent.
fn report_unobserved_slots(observed: &[u8], changed: &[events::EnforcerEvent]) {
    let Some(active) = changed.iter().find_map(state::active_slots) else {
        return;
    };

    for slot in active {
        if !observed.contains(&slot) {
            tracing::warn!(
                sidechain = slot,
                "a sidechain is active in a slot this extractor is not subscribed to; \
                 restart it to observe that slot"
            );
        }
    }
}

fn log_state_event(payload: &events::EnforcerEvent) {
    match payload.event.as_ref() {
        Some(events::enforcer_event::Event::SidechainProposals(proposals)) => {
            tracing::debug!(
                event = "sidechain_proposals",
                proposal_count = proposals.proposals.len(),
                "refreshed enforcer state changed"
            );
        }
        Some(events::enforcer_event::Event::ActiveSidechains(sidechains)) => {
            tracing::debug!(
                event = "active_sidechains",
                sidechain_count = sidechains.sidechains.len(),
                "refreshed enforcer state changed"
            );
        }
        Some(events::enforcer_event::Event::Ctip(ctip)) => {
            tracing::debug!(
                event = "ctip",
                sidechain = ctip.sidechain_number,
                present = ctip.ctip.is_some(),
                "refreshed enforcer state changed"
            );
        }
        Some(events::enforcer_event::Event::WithdrawalBundleProposals(proposals)) => {
            tracing::debug!(
                event = "withdrawal_bundle_proposals",
                sidechain = proposals.sidechain_number,
                proposal_count = proposals.proposals.len(),
                "refreshed enforcer state changed"
            );
        }
        _ => {
            tracing::warn!("refreshed an unexpected enforcer state payload");
        }
    }
}

fn log_published_event(sidechain: u8, event: &Event) {
    let Ok(event) = enforcer_payload(event) else {
        tracing::warn!(
            sidechain,
            "published a live event with an unexpected envelope"
        );
        return;
    };

    match event.event.as_ref() {
        Some(events::enforcer_event::Event::BlockConnected(block)) => {
            if let Some(header) = block.header.as_ref() {
                tracing::info!(
                    event = "block_connected",
                    sidechain,
                    height = header.height,
                    block_hash = %hex::encode(&header.hash),
                    "published live enforcer event"
                );
            } else {
                tracing::warn!(
                    event = "block_connected",
                    sidechain,
                    "published a connected-block event without a header"
                );
            }
        }
        Some(events::enforcer_event::Event::BlockDisconnected(block)) => {
            tracing::info!(
                event = "block_disconnected",
                sidechain,
                block_hash = %hex::encode(&block.block_hash),
                "published live enforcer event"
            );
        }
        _ => {
            tracing::warn!(
                sidechain,
                "published an unexpected live enforcer event type"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use anyhow::{Context as _, anyhow};
    use futures_util::{StreamExt, stream};
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::event::MonitorEvent;
    use tokio::sync::{Notify, watch};
    use tokio::time::timeout;

    use super::{
        Heartbeat, SourceFuture, StartupSource, StateSource, TipSource, announce_tip_changes,
        forward_stream, prepare_observation, refresh_on_new_blocks, report_unobserved_slots,
        snapshot_tips_are_consistent, supervise_workers,
    };
    use crate::proto::{common, mainchain};
    use crate::snapshot::InitialSnapshot;
    use crate::state;
    use shared::protobuf::event::ObservedBlock;

    fn reverse_hex(byte: u8) -> Option<common::ReverseHex> {
        Some(common::ReverseHex {
            hex: Some(hex::encode([byte; 32])),
        })
    }

    fn consensus_hex(bytes: &[u8]) -> Option<common::ConsensusHex> {
        Some(common::ConsensusHex {
            hex: Some(hex::encode(bytes)),
        })
    }

    fn disconnected(byte: u8) -> mainchain::SubscribeEventsResponse {
        mainchain::SubscribeEventsResponse {
            event: Some(mainchain::subscribe_events_response::Event {
                event: Some(
                    mainchain::subscribe_events_response::event::Event::DisconnectBlock(
                        mainchain::subscribe_events_response::event::DisconnectBlock {
                            block_hash: reverse_hex(byte),
                        },
                    ),
                ),
            }),
        }
    }

    fn connected(hash: u8, previous_hash: u8, height: u32) -> mainchain::SubscribeEventsResponse {
        mainchain::SubscribeEventsResponse {
            event: Some(mainchain::subscribe_events_response::Event {
                event: Some(
                    mainchain::subscribe_events_response::event::Event::ConnectBlock(
                        mainchain::subscribe_events_response::event::ConnectBlock {
                            header_info: Some(mainchain::BlockHeaderInfo {
                                block_hash: reverse_hex(hash),
                                prev_block_hash: reverse_hex(previous_hash),
                                height,
                                work: consensus_hex(&[0x44; 32]),
                                timestamp: 1_750_000_000,
                            }),
                            block_info: Some(mainchain::BlockInfo {
                                bmm_commitment: None,
                                events: Vec::new(),
                            }),
                        },
                    ),
                ),
            }),
        }
    }

    fn payload(event: &shared::protobuf::event::Event) -> &events::enforcer_event::Event {
        let Some(MonitorEvent::Enforcer(event)) = event.monitor_event.as_ref() else {
            panic!("expected enforcer envelope");
        };
        event.event.as_ref().expect("normalized enforcer event")
    }

    fn ctip_payload(sidechain_number: u32, value_sats: u64) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                sidechain_number,
                ctip: Some(events::Ctip {
                    txid: vec![0x11; 32],
                    vout: 0,
                    value_sats,
                    sequence_number: 1,
                }),
            })),
        }
    }

    #[derive(Clone)]
    struct FakeStartupSource {
        calls: Arc<Mutex<Vec<String>>>,
        tips: Arc<Mutex<VecDeque<Vec<u8>>>>,
        snapshot_tip: Vec<u8>,
    }

    impl StartupSource for FakeStartupSource {
        type Stream = futures_util::stream::Pending<
            std::result::Result<mainchain::SubscribeEventsResponse, tonic::Status>,
        >;

        fn subscribe_events(&mut self, sidechain: u8) -> SourceFuture<'_, Self::Stream> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls
                    .lock()
                    .expect("startup call lock")
                    .push(format!("subscribe:{sidechain}"));
                Ok(stream::pending())
            })
        }

        fn current_tip(&mut self) -> SourceFuture<'_, ObservedBlock> {
            let calls = Arc::clone(&self.calls);
            let tips = Arc::clone(&self.tips);
            Box::pin(async move {
                calls
                    .lock()
                    .expect("startup call lock")
                    .push("tip".to_owned());
                Ok(ObservedBlock::at_height(
                    tips.lock()
                        .expect("startup tip lock")
                        .pop_front()
                        .expect("configured fake tip"),
                    996_259,
                ))
            })
        }

        fn collect_snapshot<'a>(
            &'a mut self,
            _sidechains: &'a [u8],
        ) -> SourceFuture<'a, InitialSnapshot> {
            let calls = Arc::clone(&self.calls);
            let snapshot_tip = self.snapshot_tip.clone();
            Box::pin(async move {
                calls
                    .lock()
                    .expect("startup call lock")
                    .push("snapshot".to_owned());
                Ok(InitialSnapshot {
                    constants: Vec::new(),
                    state: Vec::new(),
                    anchor: ObservedBlock::at_height(snapshot_tip, 996_259),
                })
            })
        }
    }

    /// Returns a scripted state collection per call, counts the calls, and
    /// signals each one so a test can sequence tip reports deterministically.
    struct FakeStateSource {
        collections: Arc<Mutex<VecDeque<Vec<events::EnforcerEvent>>>>,
        calls: Arc<Mutex<usize>>,
        collected: Arc<Notify>,
    }

    impl FakeStateSource {
        fn new(collections: Vec<Vec<events::EnforcerEvent>>) -> Self {
            Self {
                collections: Arc::new(Mutex::new(VecDeque::from(collections))),
                calls: Arc::new(Mutex::new(0)),
                collected: Arc::new(Notify::new()),
            }
        }
    }

    impl StateSource for FakeStateSource {
        fn collect_state<'a>(
            &'a mut self,
            _sidechains: &'a [u8],
        ) -> SourceFuture<'a, state::Reading> {
            let collections = Arc::clone(&self.collections);
            let calls = Arc::clone(&self.calls);
            let collected = Arc::clone(&self.collected);
            Box::pin(async move {
                let call = {
                    let mut calls = calls.lock().expect("state call lock");
                    *calls += 1;
                    *calls
                };
                let collection = collections
                    .lock()
                    .expect("state collection lock")
                    .pop_front();
                collected.notify_one();
                Ok(state::Reading {
                    // A distinct anchor per refresh, so a test can tell which
                    // reading a published batch came from.
                    anchor: ObservedBlock::at_height(vec![call as u8; 32], 996_259 + call as u32),
                    payloads: collection.context("configured fake state collection")?,
                })
            })
        }
    }

    #[tokio::test]
    async fn startup_opens_every_subscription_before_collecting_the_snapshot() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeStartupSource {
            calls: Arc::clone(&calls),
            tips: Arc::new(Mutex::new(VecDeque::from([vec![0x11; 32], vec![0x11; 32]]))),
            snapshot_tip: vec![0x11; 32],
        };

        let observation = prepare_observation(&mut source, &[9, 98])
            .await
            .expect("prepared observation");

        assert_eq!(
            observation
                .streams
                .iter()
                .map(|(sidechain, _)| *sidechain)
                .collect::<Vec<_>>(),
            vec![9, 98]
        );

        let calls = calls.lock().expect("startup call lock");
        let tip_positions = calls
            .iter()
            .enumerate()
            .filter_map(|(index, call)| (call == "tip").then_some(index))
            .collect::<Vec<_>>();
        let snapshot_position = calls
            .iter()
            .position(|call| call == "snapshot")
            .expect("snapshot call");

        assert_eq!(tip_positions.len(), 2);
        assert!(
            calls
                .iter()
                .enumerate()
                .filter(|(_, call)| call.starts_with("subscribe:"))
                .all(|(index, _)| index < tip_positions[0])
        );
        assert!(tip_positions[0] < snapshot_position);
        assert!(snapshot_position < tip_positions[1]);
    }

    #[test]
    fn detects_tip_changes_anywhere_in_the_snapshot_window() {
        let tip = vec![0x11; 32];
        let different_tip = vec![0x22; 32];

        assert!(snapshot_tips_are_consistent(&tip, &tip, &tip));
        assert!(!snapshot_tips_are_consistent(&different_tip, &tip, &tip));
        assert!(!snapshot_tips_are_consistent(&tip, &different_tip, &tip));
        assert!(!snapshot_tips_are_consistent(&tip, &tip, &different_tip));
    }

    #[tokio::test]
    async fn publishes_disconnect_then_connect_sequentially() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let published = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&published);
        let publish_shutdown = shutdown_tx.clone();
        let stream = stream::iter([Ok(disconnected(0x77)), Ok(connected(0x88, 0x66, 501))])
            .chain(stream::pending());

        forward_stream(9, stream, shutdown_rx, move |event| {
            let output = Arc::clone(&output);
            let publish_shutdown = publish_shutdown.clone();
            async move {
                let mut output = output.lock().expect("published event lock");
                output.push(event);
                if output.len() == 2 {
                    publish_shutdown.send(true).expect("send shutdown");
                }
                Ok(())
            }
        })
        .await
        .expect("clean shutdown after both events");

        let published = published.lock().expect("published event lock");
        assert_eq!(published.len(), 2);
        let events::enforcer_event::Event::BlockDisconnected(disconnected) = payload(&published[0])
        else {
            panic!("expected disconnected block first");
        };
        assert_eq!(disconnected.block_hash, vec![0x77; 32]);

        let events::enforcer_event::Event::BlockConnected(connected) = payload(&published[1])
        else {
            panic!("expected connected block second");
        };
        assert_eq!(connected.sidechain_number, 9);
        let header = connected.header.as_ref().expect("connected block header");
        assert_eq!(header.hash, vec![0x88; 32]);
        assert_eq!(header.previous_hash, vec![0x66; 32]);
        assert_eq!(header.height, 501);
    }

    #[tokio::test]
    async fn shutdown_interrupts_an_already_waiting_idle_stream() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let stream = stream::pending::<Result<mainchain::SubscribeEventsResponse, tonic::Status>>();
        let worker = tokio::spawn(forward_stream(9, stream, shutdown_rx, |_event| async {
            Ok(())
        }));

        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("send shutdown");

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker reacts to changed notification")
            .expect("join worker")
            .expect("clean worker shutdown");
    }

    #[tokio::test]
    async fn a_stream_status_error_is_fatal() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stream = stream::iter([Err(tonic::Status::internal("upstream failed"))]);

        let error = forward_stream(9, stream, shutdown_rx, |_event| async { Ok(()) })
            .await
            .expect_err("stream status must fail");

        assert!(format!("{error:#}").contains("upstream failed"));
    }

    #[tokio::test]
    async fn a_publish_error_is_fatal() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stream = stream::iter([Ok(disconnected(0x77))]).chain(stream::pending());

        let error = forward_stream(9, stream, shutdown_rx, |_event| async {
            Err(anyhow!("fake sink failed"))
        })
        .await
        .expect_err("publish failure must fail");

        assert!(format!("{error:#}").contains("fake sink failed"));
    }

    #[tokio::test]
    async fn an_ended_stream_is_fatal() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stream = stream::empty::<Result<mainchain::SubscribeEventsResponse, tonic::Status>>();

        let error = forward_stream(9, stream, shutdown_rx, |_event| async { Ok(()) })
            .await
            .expect_err("unexpected stream end must fail");

        assert!(
            error
                .to_string()
                .contains("sidechain 9 event stream ended unexpectedly")
        );
    }

    #[tokio::test]
    async fn two_blocks_arriving_before_a_refresh_coalesce_into_one() {
        // Pins the documented contract rather than avoiding it: the announcement
        // channel keeps only the newest hash, so a refresh is a poll of "what is
        // true now", not one reading per block. There is no way to do better
        // with this API -- GetCtip and friends answer for the current tip, and no
        // RPC answers "the state at block X" -- so the guarantee is one reading
        // per observation, and consecutive readings are not consecutive blocks.
        let snapshot_tip = vec![0x11; 32];
        let (block_tx, block_rx) = watch::channel(snapshot_tip.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let source = FakeStateSource::new(vec![vec![ctip_payload(9, 250)]]);
        let calls = Arc::clone(&source.calls);
        let collected = Arc::clone(&source.collected);

        let worker = tokio::spawn(refresh_on_new_blocks(
            source,
            vec![9],
            state::Tracker::new(vec![ctip_payload(9, 100)]),
            snapshot_tip,
            block_rx,
            shutdown_rx,
            |_anchor, _payloads| async { Ok(()) },
        ));

        // Both sends happen with no await between them, so the worker cannot be
        // polled in the middle: it is guaranteed to see only the second block.
        block_tx.send(vec![0x22; 32]).expect("first new block");
        block_tx.send(vec![0x33; 32]).expect("second new block");

        collected.notified().await;
        assert_eq!(
            *calls.lock().expect("state call lock"),
            1,
            "two announcements before a refresh are one reading"
        );

        // And no second reading follows, because 0x22 was never observed.
        timeout(TEST_QUIET_WINDOW, collected.notified())
            .await
            .expect_err("the skipped block must not produce its own reading");
        assert_eq!(*calls.lock().expect("state call lock"), 1);

        shutdown_tx.send(true).expect("send shutdown");
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("state worker finishes")
            .expect("join state worker")
            .expect("clean state worker shutdown");
    }

    #[tokio::test]
    async fn state_refreshes_once_per_block_and_publishes_only_changes() {
        let snapshot_tip = vec![0x11; 32];
        let (block_tx, block_rx) = watch::channel(snapshot_tip.clone());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // One collection per expected refresh: unchanged, then changed.
        let source =
            FakeStateSource::new(vec![vec![ctip_payload(9, 100)], vec![ctip_payload(9, 250)]]);
        let calls = Arc::clone(&source.calls);
        let collected = Arc::clone(&source.collected);
        let published = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&published);
        let publish_shutdown = shutdown_tx.clone();

        let worker = tokio::spawn(refresh_on_new_blocks(
            source,
            vec![9],
            state::Tracker::new(vec![ctip_payload(9, 100)]),
            snapshot_tip.clone(),
            block_rx,
            shutdown_rx,
            move |anchor: ObservedBlock, payloads: Vec<events::EnforcerEvent>| {
                let output = Arc::clone(&output);
                let publish_shutdown = publish_shutdown.clone();
                async move {
                    output
                        .lock()
                        .expect("published state lock")
                        .push((anchor, payloads));
                    publish_shutdown.send(true).expect("send shutdown");
                    Ok(())
                }
            },
        ));

        // A replayed snapshot tip must not refresh. The channel only keeps the
        // latest value, so waiting for the refresh is what makes the count of
        // the following reports deterministic.
        block_tx.send(snapshot_tip).expect("replay snapshot tip");
        block_tx.send(vec![0x22; 32]).expect("first new block");
        collected.notified().await;

        // The same block reported by another slot must not refresh again.
        block_tx
            .send(vec![0x22; 32])
            .expect("same block, other slot");
        block_tx.send(vec![0x33; 32]).expect("second new block");

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("state worker finishes")
            .expect("join state worker")
            .expect("clean state worker shutdown");

        let published = published.lock().expect("published state lock");
        assert_eq!(published.len(), 1, "only the changed reading is published");
        let (anchor, payloads) = &published[0];
        assert_eq!(*payloads, vec![ctip_payload(9, 250)]);
        assert_eq!(
            anchor.height,
            Some(996_261),
            "the batch carries the anchor of the reading it came from"
        );
        assert_eq!(
            *calls.lock().expect("state call lock"),
            2,
            "one refresh per distinct block, none for the replayed snapshot tip"
        );
    }

    fn active_sidechains(slots: &[u32]) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::ActiveSidechains(
                events::ActiveSidechainsSnapshot {
                    sidechains: slots
                        .iter()
                        .map(|slot| events::ActiveSidechain {
                            sidechain_number: *slot,
                            ..Default::default()
                        })
                        .collect(),
                },
            )),
        }
    }

    #[test]
    fn an_activation_into_an_unobserved_slot_is_reported() {
        // A pure-function check would be better, but the report is a log line;
        // this at least pins that the scan reaches the right payload and does
        // not panic on collections that hold no snapshot.
        report_unobserved_slots(&[9, 98], &[active_sidechains(&[9, 98])]);
        report_unobserved_slots(&[9, 98], &[active_sidechains(&[9, 98, 5])]);
        report_unobserved_slots(&[9], &[ctip_payload(9, 100)]);
        report_unobserved_slots(&[], &[]);
    }

    #[tokio::test]
    async fn the_state_worker_stops_when_every_slot_worker_is_gone() {
        let (block_tx, block_rx) = watch::channel(vec![0x11; 32]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(refresh_on_new_blocks(
            FakeStateSource::new(Vec::new()),
            vec![9],
            state::Tracker::new(Vec::new()),
            vec![0x11; 32],
            block_rx,
            shutdown_rx,
            |_anchor, _payloads| async { Ok(()) },
        ));

        tokio::task::yield_now().await;
        drop(block_tx);

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("state worker reacts to a closed channel")
            .expect("join state worker")
            .expect("clean state worker shutdown");
    }

    #[tokio::test]
    async fn a_failed_state_refresh_is_fatal() {
        let (block_tx, block_rx) = watch::channel(vec![0x11; 32]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(refresh_on_new_blocks(
            // No scripted collection: the fake reports a failure.
            FakeStateSource::new(Vec::new()),
            vec![9],
            state::Tracker::new(Vec::new()),
            vec![0x11; 32],
            block_rx,
            shutdown_rx,
            |_anchor, _payloads| async { Ok(()) },
        ));

        block_tx.send(vec![0x22; 32]).expect("new block");

        let error = timeout(Duration::from_secs(1), worker)
            .await
            .expect("state worker finishes")
            .expect("join state worker")
            .expect_err("a failed refresh must fail the worker");

        assert!(format!("{error:#}").contains("refreshing enforcer state at block"));
    }

    #[tokio::test]
    async fn a_failed_worker_aborts_its_siblings() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let mut workers = tokio::task::JoinSet::new();

        let failure_started = Arc::clone(&started);
        workers.spawn(async move {
            failure_started.notified().await;
            Err(anyhow!("worker failed"))
        });

        let sibling_started = Arc::clone(&started);
        let sibling_dropped = Arc::clone(&dropped);
        workers.spawn(async move {
            let _guard = Dropped(sibling_dropped);
            sibling_started.notify_one();
            std::future::pending::<anyhow::Result<()>>().await
        });

        let error = supervise_workers(workers)
            .await
            .expect_err("one failed worker must fail the supervisor");

        assert!(error.to_string().contains("worker failed"));
        assert!(dropped.load(Ordering::SeqCst), "sibling future was dropped");
    }

    /// Answers a configured sequence of tips, repeating the last one forever.
    struct FakeTipSource {
        tips: Arc<Mutex<VecDeque<Vec<u8>>>>,
        last: Arc<Mutex<Vec<u8>>>,
        reads: Arc<Mutex<usize>>,
    }

    impl FakeTipSource {
        fn new(tips: Vec<Vec<u8>>) -> Self {
            Self {
                tips: Arc::new(Mutex::new(VecDeque::from(tips))),
                last: Arc::new(Mutex::new(Vec::new())),
                reads: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl TipSource for FakeTipSource {
        fn read_tip(&mut self) -> SourceFuture<'_, ObservedBlock> {
            let tips = Arc::clone(&self.tips);
            let last = Arc::clone(&self.last);
            let reads = Arc::clone(&self.reads);
            Box::pin(async move {
                *reads.lock().expect("tip read lock") += 1;
                let mut last = last.lock().expect("tip memo lock");
                if let Some(tip) = tips.lock().expect("tip queue lock").pop_front() {
                    *last = tip;
                }
                Ok(ObservedBlock::at_height(last.clone(), 996_259))
            })
        }
    }

    /// Short enough that a test can see several ticks, long enough that a busy
    /// machine still gets through one.
    const TEST_TIP_INTERVAL: Duration = Duration::from_millis(5);
    /// Covers many ticks, so "nothing was announced" is a real observation
    /// rather than a race the test won.
    const TEST_QUIET_WINDOW: Duration = Duration::from_millis(150);

    fn temporary_liveness_path(test: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("bip300-tip-liveness-{}-{test}", std::process::id()))
    }

    #[tokio::test]
    async fn the_tip_worker_reports_only_a_moved_tip() {
        let source = FakeTipSource::new(vec![
            vec![0x11; 32], // the tip the channel already holds
            vec![0x22; 32], // a move
        ]);
        let (block_tx, mut block_rx) = watch::channel(vec![0x11; 32]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let liveness = temporary_liveness_path("moved-tip");
        let worker = tokio::spawn(announce_tip_changes(
            source,
            TEST_TIP_INTERVAL,
            Heartbeat::new(Some(liveness.clone())),
            block_tx,
            shutdown_rx,
        ));

        timeout(Duration::from_secs(5), block_rx.changed())
            .await
            .expect("the moved tip is announced")
            .expect("the channel stays open");
        assert_eq!(*block_rx.borrow_and_update(), vec![0x22; 32]);

        // The fake keeps answering 0x22 from here on. Every following tick must
        // be dropped, or the state worker would re-read the enforcer once per
        // tick for a tip that never moved.
        timeout(TEST_QUIET_WINDOW, block_rx.changed())
            .await
            .expect_err("an unchanged tip must not wake the state worker");

        assert!(
            liveness.exists(),
            "a successful tip read is what the healthcheck reads"
        );
        std::fs::remove_file(&liveness).expect("clean up the liveness file");

        shutdown_tx.send(true).expect("send shutdown");
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("the tip worker reacts to shutdown")
            .expect("join tip worker")
            .expect("clean tip worker shutdown");
    }

    #[tokio::test]
    async fn a_failed_tip_read_is_not_fatal() {
        struct FailingTipSource;

        impl TipSource for FailingTipSource {
            fn read_tip(&mut self) -> SourceFuture<'_, ObservedBlock> {
                Box::pin(async { Err(anyhow!("the enforcer is not answering")) })
            }
        }

        let (block_tx, _block_rx) = watch::channel(vec![0x11; 32]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let liveness = temporary_liveness_path("failed-tip");
        let mut worker = tokio::spawn(announce_tip_changes(
            FailingTipSource,
            TEST_TIP_INTERVAL,
            Heartbeat::new(Some(liveness.clone())),
            block_tx,
            shutdown_rx,
        ));

        // A poll is not a record write, so a failure waits for the next tick
        // instead of taking the extractor down with it.
        timeout(TEST_QUIET_WINDOW, &mut worker)
            .await
            .expect_err("a failed tip read must not stop the worker");
        assert!(
            !liveness.exists(),
            "an extractor that cannot read the tip must not report itself healthy"
        );

        shutdown_tx.send(true).expect("send shutdown");
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("the tip worker reacts to shutdown")
            .expect("join tip worker")
            .expect("clean tip worker shutdown");
    }

    #[tokio::test]
    async fn the_state_worker_waits_for_shutdown_when_no_slot_is_observed() {
        // Discovery on a network with no activation resolves zero slots, so no
        // slot worker exists to report a block. The state worker must still be
        // waiting on the tip worker's sender rather than deciding it is done:
        // returning here used to end the process with a success code.
        let (block_tx, block_rx) = watch::channel(vec![0x11; 32]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let worker = tokio::spawn(refresh_on_new_blocks(
            FakeStateSource::new(Vec::new()),
            Vec::new(),
            state::Tracker::new(Vec::new()),
            vec![0x11; 32],
            block_rx,
            shutdown_rx,
            |_anchor, _payloads| async { Ok(()) },
        ));

        tokio::task::yield_now().await;
        assert!(
            !worker.is_finished(),
            "a live sender means further tip changes can still arrive"
        );

        shutdown_tx.send(true).expect("send shutdown");
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("the state worker reacts to shutdown")
            .expect("join state worker")
            .expect("clean state worker shutdown");
        drop(block_tx);
    }
}
