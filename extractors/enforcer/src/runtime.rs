//! Continuous enforcer event extraction.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result, bail};
use futures_util::future::try_join_all;
use futures_util::{Stream, StreamExt};
use shared::nats::EventPublisher;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::Event;
use shared::protobuf::event::event::MonitorEvent;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tonic::{Status, Streaming};

use crate::config::Args;
use crate::event::envelope;
use crate::proto::mainchain;
use crate::snapshot::{self, InitialSnapshot, publish_snapshot};
use crate::state;
use crate::{EnforcerClient, convert};

type EventStream = Streaming<mainchain::SubscribeEventsResponse>;
type SourceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
const NATS_CLIENT_NAME: &str = "bip300-monitor-enforcer-extractor";

trait StartupSource: Clone + Send {
    type Stream: Send;

    fn subscribe_events(&mut self, sidechain: u8) -> SourceFuture<'_, Self::Stream>;
    fn current_tip_hash(&mut self) -> SourceFuture<'_, Vec<u8>>;
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

    fn current_tip_hash(&mut self) -> SourceFuture<'_, Vec<u8>> {
        Box::pin(snapshot::current_tip_hash(self))
    }

    fn collect_snapshot<'a>(
        &'a mut self,
        sidechains: &'a [u8],
    ) -> SourceFuture<'a, InitialSnapshot> {
        Box::pin(snapshot::collect_snapshot(self, sidechains))
    }
}

/// Source of the enforcer state that has to be re-read as the tip advances.
trait StateSource: Send {
    fn collect_state<'a>(
        &'a mut self,
        sidechains: &'a [u8],
    ) -> SourceFuture<'a, Vec<events::EnforcerEvent>>;
}

impl StateSource for EnforcerClient {
    fn collect_state<'a>(
        &'a mut self,
        sidechains: &'a [u8],
    ) -> SourceFuture<'a, Vec<events::EnforcerEvent>> {
        Box::pin(state::collect(self, sidechains))
    }
}

struct PreparedStartup {
    publisher: EventPublisher,
    client: EnforcerClient,
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
        publisher,
        client,
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    } = prepared;

    if !snapshot_tips_are_consistent(
        &tip_before_snapshot,
        &snapshot.tip_hash,
        &tip_after_snapshot,
    ) {
        tracing::warn!(
            tip_before = %hex::encode(&tip_before_snapshot),
            snapshot_tip = %hex::encode(&snapshot.tip_hash),
            tip_after = %hex::encode(&tip_after_snapshot),
            "mainchain tip changed while collecting the initial snapshot; \
             buffered live events may duplicate snapshot state"
        );
    }

    publish_snapshot(&publisher, &snapshot)
        .await
        .context("publishing and flushing the initial enforcer snapshot")?;
    tracing::info!(
        sidechain_count = args.sidechains.len(),
        "published initial enforcer snapshot"
    );

    if *shutdown_rx.borrow() {
        tracing::info!("shutdown requested after the initial snapshot");
        return Ok(());
    }

    // Every slot worker reports the blocks it sees here, so one state worker can
    // re-read the mutable enforcer state once per tip change instead of once per
    // slot. The original sender is dropped below: when the last slot worker
    // exits, the state worker's wait ends on its own.
    let snapshot_block = snapshot.tip_hash.clone();
    let (block_tx, block_rx) = watch::channel(snapshot.tip_hash);
    let tracker = state::Tracker::new(snapshot.state);

    let mut workers = JoinSet::new();
    for (sidechain, stream) in streams {
        workers.spawn(monitor_sidechain(
            stream,
            publisher.clone(),
            sidechain,
            block_tx.clone(),
            shutdown_rx.clone(),
        ));
    }
    drop(block_tx);
    workers.spawn(monitor_state(
        client,
        publisher,
        args.sidechains.clone(),
        tracker,
        snapshot_block,
        block_rx,
        shutdown_rx,
    ));

    supervise_workers(workers).await?;
    tracing::info!("enforcer extractor stopped");
    Ok(())
}

async fn prepare_startup(args: &Args) -> Result<PreparedStartup> {
    let publisher = EventPublisher::connect(&args.nats, NATS_CLIENT_NAME)
        .await
        .context("connecting the event publisher")?;

    let mut client = EnforcerClient::connect(&args.enforcer_endpoint, args.request_timeout())
        .await
        .context("connecting the enforcer client")?;

    let observation = prepare_observation(&mut client, &args.sidechains)
        .await
        .context("preparing enforcer subscriptions and initial snapshot")?;
    let PreparedObservation {
        streams,
        snapshot,
        tip_before_snapshot,
        tip_after_snapshot,
    } = observation;

    Ok(PreparedStartup {
        publisher,
        client,
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
        .current_tip_hash()
        .await
        .context("reading the mainchain tip before collecting the snapshot")?;
    let snapshot = client
        .collect_snapshot(sidechains)
        .await
        .context("collecting the initial enforcer snapshot")?;
    let tip_after_snapshot = client
        .current_tip_hash()
        .await
        .context("reading the mainchain tip after collecting the snapshot")?;

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
    publisher: EventPublisher,
    sidechain: u8,
    block_tx: watch::Sender<Vec<u8>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!(sidechain, "started sidechain event worker");

    forward_stream(sidechain, stream, shutdown_rx, move |event| {
        let publisher = publisher.clone();
        let block_tx = block_tx.clone();
        async move {
            publisher
                .publish_and_flush(Subject::Enforcer, &event)
                .await
                .with_context(|| {
                    format!("publishing and flushing a live event for sidechain {sidechain}")
                })?;
            log_published_event(sidechain, &event);
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
    let hash = match enforcer_payload(event).and_then(state::block_hash) {
        Ok(hash) => hash.to_vec(),
        Err(error) => {
            tracing::warn!(
                sidechain,
                error = %format!("{error:#}"),
                "could not read the block hash of a published live event"
            );
            return;
        }
    };
    let _ = block_tx.send(hash);
}

/// Re-read the mutable enforcer state on every tip change and publish what
/// changed.
async fn monitor_state(
    client: EnforcerClient,
    publisher: EventPublisher,
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
        move |payloads| {
            let publisher = publisher.clone();
            async move {
                let published = payloads.len();
                for payload in payloads {
                    log_state_event(&payload);
                    publisher
                        .publish(Subject::Enforcer, &envelope(payload)?)
                        .await
                        .context("publishing a refreshed enforcer state event")?;
                }
                publisher
                    .flush()
                    .await
                    .context("flushing refreshed enforcer state events")?;
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
    P: FnMut(Vec<events::EnforcerEvent>) -> F,
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
                    // Every slot worker dropped its sender, so no further tip
                    // change can arrive.
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
        let current = source.collect_state(&sidechains).await.with_context(|| {
            format!("refreshing enforcer state at block {}", hex::encode(&block))
        })?;
        refreshed_at = block;

        let changed = tracker.take_changed(current)?;
        if changed.is_empty() {
            continue;
        }
        publish(changed)
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
        let event = envelope(payload)?;
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
        SourceFuture, StartupSource, StateSource, forward_stream, prepare_observation,
        refresh_on_new_blocks, snapshot_tips_are_consistent, supervise_workers,
    };
    use crate::proto::{common, mainchain};
    use crate::snapshot::InitialSnapshot;
    use crate::state;

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

        fn current_tip_hash(&mut self) -> SourceFuture<'_, Vec<u8>> {
            let calls = Arc::clone(&self.calls);
            let tips = Arc::clone(&self.tips);
            Box::pin(async move {
                calls
                    .lock()
                    .expect("startup call lock")
                    .push("tip".to_owned());
                Ok(tips
                    .lock()
                    .expect("startup tip lock")
                    .pop_front()
                    .expect("configured fake tip"))
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
                    tip_hash: snapshot_tip,
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
        ) -> SourceFuture<'a, Vec<events::EnforcerEvent>> {
            let collections = Arc::clone(&self.collections);
            let calls = Arc::clone(&self.calls);
            let collected = Arc::clone(&self.collected);
            Box::pin(async move {
                *calls.lock().expect("state call lock") += 1;
                let collection = collections
                    .lock()
                    .expect("state collection lock")
                    .pop_front();
                collected.notify_one();
                collection.context("configured fake state collection")
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
            move |payloads| {
                let output = Arc::clone(&output);
                let publish_shutdown = publish_shutdown.clone();
                async move {
                    output.lock().expect("published state lock").push(payloads);
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
        assert_eq!(
            *published,
            vec![vec![ctip_payload(9, 250)]],
            "only the changed payload is published"
        );
        assert_eq!(
            *calls.lock().expect("state call lock"),
            2,
            "one refresh per distinct block, none for the replayed snapshot tip"
        );
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
            |_payloads| async { Ok(()) },
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
            |_payloads| async { Ok(()) },
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
}
