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
use crate::protobuf::event::{Event, ObservedBlock};
use crate::store::{
    CaptureMethod, DatasetManifest, ExtractorWorker, PostgresArgs, SidechainInstanceRef,
    SnapshotMetadata, Store,
};
use std::time::SystemTime;

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
    /// The publisher is validated first so malformed NATS configuration cannot
    /// leave a durable extractor run behind. An unreachable NATS server still
    /// yields a reconnecting client, so an outage degrades only live fan-out.
    pub async fn connect(
        postgres: &PostgresArgs,
        nats: &NatsArgs,
        subject: Subject,
        source: &'static str,
        client_name: &'static str,
    ) -> Result<Self> {
        Self::connect_with_manifest(
            postgres,
            nats,
            subject,
            source,
            client_name,
            DatasetManifest::default(),
        )
        .await
    }

    /// Connect and attach every observation to an explicit dataset/build.
    pub async fn connect_with_manifest(
        postgres: &PostgresArgs,
        nats: &NatsArgs,
        subject: Subject,
        source: &'static str,
        client_name: &'static str,
        manifest: DatasetManifest,
    ) -> Result<Self> {
        let publisher = EventPublisher::connect(nats, client_name)
            .await
            .context("connecting the live event publisher")?;
        let store = Store::connect_with_manifest(postgres, source, manifest)
            .await
            .context("connecting the event record")?;

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

    /// Record an event against the exact activation whose stream supplied it.
    pub async fn record_for_instance(
        &self,
        event: Event,
        instance: &SidechainInstanceRef,
    ) -> Result<()> {
        let events = [event];
        let recorded = self
            .store
            .record_with_method_for_instance(&events, CaptureMethod::Live, instance)
            .await
            .context("recording an instance-scoped event")?;
        if recorded == 0 {
            tracing::debug!(
                sidechain = instance.sidechain,
                instance = %instance.sidechain_instance_id,
                "the instance-scoped event fact was already recorded"
            );
        }
        self.fan_out(&events).await;
        Ok(())
    }

    /// Record a batch of events in one transaction, then fan them out.
    pub async fn record_batch(&self, events: Vec<Event>) -> Result<()> {
        self.record_batch_with_method(events, CaptureMethod::Live)
            .await
    }

    /// Record a batch with explicit capture provenance, then fan it out.
    pub async fn record_batch_with_method(
        &self,
        events: Vec<Event>,
        method: CaptureMethod,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let recorded = self
            .store
            .record_with_method(&events, method)
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

    /// Record a consistent (or explicitly inconsistent) unary snapshot.
    pub async fn record_snapshot_batch(
        &self,
        events: Vec<Event>,
        method: CaptureMethod,
        metadata: &SnapshotMetadata,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let recorded = self
            .store
            .record_snapshot(&events, method, metadata)
            .await
            .context("recording an observed snapshot")?;
        if recorded < events.len() as u64 {
            tracing::debug!(
                observed = events.len(),
                recorded,
                "some snapshot facts were already recorded"
            );
        }
        self.fan_out(&events).await;
        Ok(())
    }

    /// Persist a changed mainchain-tip observation.
    pub async fn record_tip_observation(
        &self,
        tip: &ObservedBlock,
        previous: Option<&ObservedBlock>,
        method: CaptureMethod,
        observed_at: SystemTime,
    ) -> Result<()> {
        self.store
            .record_tip_observation(tip, previous, method, observed_at)
            .await
    }

    /// Initialize independent durable worker-health rows for this run.
    pub async fn initialize_worker_statuses(&self, workers: &[ExtractorWorker]) -> Result<()> {
        self.store.initialize_worker_statuses(workers).await
    }

    /// Persist a worker failure and return its durable consecutive count.
    pub async fn record_worker_failure(
        &self,
        worker: ExtractorWorker,
        error: &str,
        degraded_after: u32,
    ) -> Result<u32> {
        self.store
            .record_worker_failure(worker, error, degraded_after)
            .await
    }

    /// Mark exactly one worker healthy.
    pub async fn record_worker_success(&self, worker: ExtractorWorker) -> Result<()> {
        self.store.record_worker_success(worker).await
    }

    /// Mark the current extractor run as cleanly finished or failed.
    pub async fn finish_run(&self, status: &str, reason: Option<&str>) -> Result<()> {
        self.store.finish_run(status, reason).await
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
