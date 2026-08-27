//! Commit-then-publish recording of monitor events.
//!
//! Postgres is the authoritative record and NATS is best-effort live fan-out,
//! so the order matters and so does the asymmetry in how failures are handled:
//!
//! - A failed record write is **fatal**. Continuing would leave a hole that
//!   looks exactly like "nothing happened".
//! - A failed publication is a **warning**. The row is already committed, so a
//!   live consumer missing a message costs nothing but its own freshness.
//!
//! Committing first also means a live consumer can never see an event before it
//! is durable.

use anyhow::{Context, Result};

use crate::nats::{EventPublisher, NatsArgs};
use crate::nats_subjects::Subject;
use crate::protobuf::event::Event;
use crate::store::{PostgresArgs, Store};

/// Writes observed events to the record and fans them out to live consumers.
#[derive(Clone)]
pub struct Recorder {
    store: Store,
    publisher: EventPublisher,
    subject: Subject,
}

impl Recorder {
    /// Connect to the record and to the live transport.
    ///
    /// The record is connected first: without somewhere to store observations
    /// there is no point holding a publisher open. Only the record has to be
    /// reachable — the publisher reconnects in the background — so a NATS
    /// outage degrades the live fan-out instead of stopping the extractor.
    pub async fn connect(
        postgres: &PostgresArgs,
        nats: &NatsArgs,
        subject: Subject,
        source: &'static str,
        client_name: &'static str,
    ) -> Result<Self> {
        let store = Store::connect(postgres, source)
            .await
            .context("connecting the event record")?;
        let publisher = EventPublisher::connect(nats, client_name)
            .await
            .context("connecting the live event publisher")?;

        Ok(Self {
            store,
            publisher,
            subject,
        })
    }

    /// Record one event and fan it out.
    pub async fn record(&self, event: Event) -> Result<()> {
        self.record_batch(vec![event]).await
    }

    /// Record a batch of events in one transaction, then fan them out.
    pub async fn record_batch(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let recorded = self
            .store
            .record(&events)
            .await
            .context("recording observed events")?;
        if recorded < events.len() as u64 {
            // Expected after a restart republishes the snapshot, and after a
            // backfill replays blocks the record already holds.
            tracing::debug!(
                observed = events.len(),
                recorded,
                "some observations were already recorded"
            );
        }

        self.fan_out(&events).await;
        Ok(())
    }

    /// Access the record directly, for reads such as the backfill checkpoint.
    pub const fn store(&self) -> &Store {
        &self.store
    }

    /// Hand a recorded batch to live consumers, without letting the transport
    /// hold up the record.
    ///
    /// Bounded inside [`EventPublisher::publish_batch`], because publishing into
    /// a disconnected NATS client blocks once its queue fills. The result is
    /// only reported: by the time this runs the rows are committed, so nothing
    /// here can fail the caller.
    async fn fan_out(&self, events: &[Event]) {
        let outcome = self.publisher.publish_batch(self.subject, events).await;
        let Some(failure) = outcome.failure.as_deref() else {
            return;
        };

        // One warning per batch, not per event: a broken transport should be
        // legible, not a wall of identical lines.
        tracing::warn!(
            observed = outcome.observed,
            dropped = outcome.dropped(),
            error = %failure,
            "live fan-out dropped events; the record already holds them"
        );
    }
}
