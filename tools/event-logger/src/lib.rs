#![cfg_attr(feature = "strict", deny(warnings))]

//! NATS consumer that logs normalized bip300-monitor events.

mod config;
mod format;

use anyhow::{Context, Result};
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

    let mut invalid_event_count = 0_u64;
    loop {
        let received = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => break,
            result = subscriber.next_event() => result.context("receiving the next monitor event")?,
        };
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
        let Some(rendered) = render_event(&event, &mut invalid_event_count) else {
            continue;
        };
        tracing::info!(
            subject = %Subject::Enforcer,
            timestamp_ms = event.timestamp,
            event = rendered.kind,
            summary = %rendered.summary,
            "received enforcer event"
        );
        if args.full_events {
            tracing::info!(
                subject = %Subject::Enforcer,
                timestamp_ms = event.timestamp,
                event = rendered.kind,
                payload = %rendered.full_json,
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

fn render_event(event: &Event, invalid_event_count: &mut u64) -> Option<format::RenderedEvent> {
    match format::render(event) {
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
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: None,
            })),
        };
        let valid = Event {
            timestamp: 2,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                    sidechain_number: 9,
                    ctip: None,
                })),
            })),
        };
        let mut invalid_event_count = 0;

        assert!(render_event(&invalid, &mut invalid_event_count).is_none());
        assert_eq!(invalid_event_count, 1);
        let rendered = render_event(&valid, &mut invalid_event_count)
            .expect("valid event after invalid event");
        assert_eq!(rendered.kind, "ctip");
        assert_eq!(invalid_event_count, 1);
    }
}
