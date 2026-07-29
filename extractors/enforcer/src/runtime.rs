//! Continuous enforcer event extraction.

use std::future::Future;

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
use crate::snapshot::{InitialSnapshot, collect_snapshot, current_tip_hash, publish_snapshot};
use crate::{EnforcerClient, convert};

type EventStream = Streaming<mainchain::SubscribeEventsResponse>;

struct PreparedStartup {
    publisher: EventPublisher,
    streams: Vec<(u8, EventStream)>,
    snapshot: InitialSnapshot,
    tip_before_snapshot: Vec<u8>,
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
        streams,
        snapshot,
        tip_before_snapshot,
    } = prepared;

    if tip_before_snapshot != snapshot.tip_hash {
        tracing::warn!(
            tip_before = %hex::encode(&tip_before_snapshot),
            snapshot_tip = %hex::encode(&snapshot.tip_hash),
            "mainchain tip changed while opening subscriptions and collecting the snapshot; \
             buffered live events may duplicate snapshot state"
        );
    }

    publish_snapshot(&publisher, snapshot)
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

    let mut workers = JoinSet::new();
    for (sidechain, stream) in streams {
        workers.spawn(monitor_sidechain(
            stream,
            publisher.clone(),
            sidechain,
            shutdown_rx.clone(),
        ));
    }

    let runtime_result = supervise_workers(workers).await;
    let flush_result = publisher
        .flush()
        .await
        .context("flushing live enforcer events during shutdown");

    match (runtime_result, flush_result) {
        (Ok(()), Ok(())) => {
            tracing::info!("enforcer extractor stopped");
            Ok(())
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(flush_error)) => Err(flush_error),
        (Err(primary), Err(flush_error)) => {
            tracing::error!(
                error = %format!("{flush_error:#}"),
                "final NATS flush also failed while handling a fatal extractor error"
            );
            Err(primary)
        }
    }
}

async fn prepare_startup(args: &Args) -> Result<PreparedStartup> {
    let publisher = EventPublisher::connect(&args.nats)
        .await
        .context("connecting the event publisher")?;

    let mut client = EnforcerClient::connect(&args.enforcer_endpoint, args.request_timeout())
        .await
        .context("connecting the enforcer client")?;
    tracing::info!(endpoint = %args.enforcer_endpoint, "connected to enforcer");

    let subscriptions = args.sidechains.iter().copied().map(|sidechain| {
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

    let tip_before_snapshot = current_tip_hash(&mut client)
        .await
        .context("reading the mainchain tip before collecting the snapshot")?;
    let snapshot = collect_snapshot(&mut client, &args.sidechains)
        .await
        .context("collecting the initial enforcer snapshot")?;

    Ok(PreparedStartup {
        publisher,
        streams,
        snapshot,
        tip_before_snapshot,
    })
}

async fn monitor_sidechain(
    stream: EventStream,
    publisher: EventPublisher,
    sidechain: u8,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    tracing::info!(sidechain, "started sidechain event worker");

    forward_stream(sidechain, stream, shutdown_rx, move |event| {
        let publisher = publisher.clone();
        async move {
            publisher
                .publish_and_flush(Subject::Enforcer, &event)
                .await
                .with_context(|| {
                    format!("publishing and flushing a live event for sidechain {sidechain}")
                })?;
            log_published_event(sidechain, &event);
            Ok(())
        }
    })
    .await?;

    tracing::info!(sidechain, "stopped sidechain event worker");
    Ok(())
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

fn log_published_event(sidechain: u8, event: &Event) {
    let Some(MonitorEvent::Enforcer(event)) = event.monitor_event.as_ref() else {
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use anyhow::anyhow;
    use futures_util::{StreamExt, stream};
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::event::MonitorEvent;
    use tokio::sync::{Notify, watch};
    use tokio::time::timeout;

    use super::{forward_stream, supervise_workers};
    use crate::proto::{common, mainchain};

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
