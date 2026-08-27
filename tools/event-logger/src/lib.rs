#![cfg_attr(feature = "strict", deny(warnings))]

//! NATS consumer that logs normalized bip300-monitor events.

mod config;
mod format;

use anyhow::{Context, Result};
use shared::liveness::Heartbeat;
use shared::nats::{EventSubscriber, ReceivedEvent};
use shared::nats_subjects::Subject;
use shared::protobuf::event::Event;
use tokio::sync::watch;

pub use config::Args;

const CLIENT_NAME: &str = "bip300-monitor-event-logger";

/// Subscribe to enforcer events and log every decoded envelope.
pub async fn run(args: Args, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
    args.validate().context("validating logger configuration")?;

    let connect = EventSubscriber::connect(&args.nats, Subject::Enforcer, CLIENT_NAME);
    tokio::pin!(connect);
    let mut subscriber = tokio::select! {
        biased;
        () = wait_for_shutdown(&mut shutdown_rx) => {
            tracing::info!("shutdown requested during event logger startup");
            return Ok(());
        }
        result = &mut connect => result.context("connecting the event subscriber")?,
    };

    tracing::info!(
        subject = %Subject::Enforcer,
        full_events = args.full_events,
        "event logger subscription is ready"
    );

    let heartbeat = Heartbeat::new(args.liveness_file.clone());
    let liveness_interval = args.liveness_interval();
    // The first beat says the subscription was established, so a healthcheck
    // does not have to wait out one interval before the logger looks alive.
    heartbeat.beat();

    let mut invalid_event_count = 0_u64;
    loop {
        let received = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => break,
            () = tokio::time::sleep(liveness_interval) => {
                // A quiet chain and a dead transport both deliver nothing, so
                // the timer round-trips the server rather than beating blind. A
                // failure is only reported: the next `next_event` is what turns
                // a genuinely lost subscription into a fatal error.
                match subscriber.confirm_connection().await {
                    Ok(()) => heartbeat.beat(),
                    Err(error) => tracing::warn!(
                        error = %format!("{error:#}"),
                        "could not confirm the NATS subscription"
                    ),
                }
                continue;
            }
            result = subscriber.next_event() => result.context("receiving the next monitor event")?,
        };
        heartbeat.beat();
        let event = match received {
            ReceivedEvent::Decoded(event) => event,
            ReceivedEvent::Invalid { error, payload_len } => {
                invalid_event_count = invalid_event_count.saturating_add(1);
                tracing::warn!(
                    subject = %Subject::Enforcer,
                    invalid_event_count,
                    payload_len,
                    error = %error,
                    "discarded an invalid protobuf event"
                );
                continue;
            }
        };
        let Some(rendered) = render_event(&event, args.full_events, &mut invalid_event_count)
        else {
            continue;
        };
        tracing::info!(
            subject = %Subject::Enforcer,
            timestamp_ms = event.timestamp,
            event = rendered.kind,
            summary = %rendered.summary,
            "received enforcer event"
        );
        if let Some(payload) = rendered.full_json.as_deref() {
            tracing::info!(
                subject = %Subject::Enforcer,
                timestamp_ms = event.timestamp,
                event = rendered.kind,
                payload = %payload,
                "received full enforcer event payload"
            );
        }
    }

    subscriber
        .close()
        .await
        .context("closing the event subscriber")?;
    tracing::info!("event logger stopped");
    Ok(())
}

fn render_event(
    event: &Event,
    full_events: bool,
    invalid_event_count: &mut u64,
) -> Option<format::RenderedEvent> {
    match format::render(event, full_events) {
        Ok(rendered) => Some(rendered),
        Err(error) => {
            *invalid_event_count = invalid_event_count.saturating_add(1);
            tracing::warn!(
                subject = %Subject::Enforcer,
                invalid_event_count = *invalid_event_count,
                error = %format!("{error:#}"),
                "discarded an invalid enforcer event"
            );
            None
        }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio::time::timeout;

    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::Event;
    use shared::protobuf::event::event::MonitorEvent;

    use super::{render_event, wait_for_shutdown};

    #[tokio::test]
    async fn shutdown_interrupts_an_idle_wait() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let wait = tokio::spawn(async move {
            wait_for_shutdown(&mut shutdown_rx).await;
        });
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("send shutdown");

        timeout(Duration::from_secs(1), wait)
            .await
            .expect("wait is interruptible")
            .expect("wait task");
    }

    #[test]
    fn invalid_event_does_not_prevent_the_next_event_from_rendering() {
        let invalid = Event {
            timestamp: 1,
            observed_at_block: None,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: None,
            })),
        };
        let valid = Event {
            timestamp: 2,
            observed_at_block: None,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                    sidechain_number: 9,
                    ctip: None,
                })),
            })),
        };
        let mut invalid_event_count = 0;

        assert!(render_event(&invalid, false, &mut invalid_event_count).is_none());
        assert_eq!(invalid_event_count, 1);
        let rendered = render_event(&valid, false, &mut invalid_event_count)
            .expect("valid event after invalid event");
        assert_eq!(rendered.kind, "ctip");
        assert_eq!(invalid_event_count, 1);
    }

    #[test]
    fn the_full_payload_is_built_only_when_full_events_is_set() {
        let event = Event {
            timestamp: 3,
            observed_at_block: None,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                    sidechain_number: 9,
                    ctip: None,
                })),
            })),
        };
        let mut invalid_event_count = 0;

        let lean = render_event(&event, false, &mut invalid_event_count).expect("rendered");
        assert_eq!(lean.kind, "ctip");
        assert!(!lean.summary.is_empty());
        assert!(
            lean.full_json.is_none(),
            "the logger never reads this field unless full events are enabled"
        );

        let full = render_event(&event, true, &mut invalid_event_count).expect("rendered");
        assert!(full.full_json.is_some());
        assert_eq!(invalid_event_count, 0);
    }
}
