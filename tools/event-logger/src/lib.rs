#![cfg_attr(feature = "strict", deny(warnings))]

//! NATS consumer that logs normalized bip300-monitor events.

mod config;
mod format;

use anyhow::{Context, Result};
use shared::nats::EventSubscriber;
use shared::nats_subjects::Subject;
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

    loop {
        let event = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown_rx) => break,
            result = subscriber.next_event() => result.context("receiving the next monitor event")?,
        };
        let rendered = format::render(&event).context("validating and rendering monitor event")?;
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

    use super::wait_for_shutdown;

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
}
