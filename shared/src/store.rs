//! The authoritative Postgres record of observed monitor events.
//!
//! The `envelope` column holds normalized protobuf bytes. Rebuilds that need
//! observation order use `event_observation JOIN event ORDER BY capture_seq`;
//! the idempotent `event` rows alone deliberately do not represent replay or
//! reorg occurrence order. The upstream wire response is not stored verbatim;
//! raw consensus bytes needed for audit are explicit normalized fields.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use clap::Args as ClapArgs;
use prost::Message as _;
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row, Transaction};

use crate::json;
use crate::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;

/// Schema statements applied in order at startup.
///
/// Every statement is idempotent, and `schema_version` records how far the
/// record has been migrated so a future statement is applied exactly once.
const MIGRATIONS: &[&str] = &[
    include_str!("../schema/0001_event.sql"),
    include_str!("../schema/0002_event_identity_nulls.sql"),
    include_str!("../schema/0003_history_coverage.sql"),
    include_str!("../schema/0004_observation_provenance.sql"),
];

/// Advisory-lock key that serializes the migration of one record.
///
/// The check-then-apply below is two statements, so two processes starting at
/// once would both see a version as unapplied and both run it. `ADD CONSTRAINT`
/// is not idempotent, so the loser would fail its startup for no real reason.
const MIGRATION_LOCK_KEY: i64 = 0x6231_3330_305f_6d6f;

/// Reusable command-line arguments for the Postgres record.
#[derive(ClapArgs, Clone)]
pub struct PostgresArgs {
    /// Host of the Postgres record.
    #[arg(long, env = "BIP300_MONITOR_POSTGRES_HOST", default_value = DEFAULT_HOST)]
    pub postgres_host: String,

    /// Port of the Postgres record.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_PORT",
        default_value_t = DEFAULT_PORT
    )]
    pub postgres_port: u16,

    /// Database holding the record.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_DB",
        default_value = "bip300_monitor"
    )]
    pub postgres_db: String,

    /// Role used to write the record.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_USER",
        default_value = "bip300_monitor"
    )]
    pub postgres_user: String,

    /// Password for the role.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_PASSWORD",
        conflicts_with = "postgres_password_file"
    )]
    pub postgres_password: Option<String>,

    /// File containing the password for the role.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_PASSWORD_FILE",
        conflicts_with = "postgres_password"
    )]
    pub postgres_password_file: Option<PathBuf>,

    /// Maximum time to wait for the record connection to be established.
    #[arg(
        long,
        env = "BIP300_MONITOR_POSTGRES_CONNECT_TIMEOUT_SECONDS",
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub postgres_connect_timeout_seconds: u64,
}

impl Default for PostgresArgs {
    fn default() -> Self {
        Self {
            postgres_host: DEFAULT_HOST.to_owned(),
            postgres_port: DEFAULT_PORT,
            postgres_db: "bip300_monitor".to_owned(),
            postgres_user: "bip300_monitor".to_owned(),
            postgres_password: None,
            postgres_password_file: None,
            postgres_connect_timeout_seconds: 10,
        }
    }
}

impl PostgresArgs {
    /// Build a libpq connection string, resolving the password from a file when
    /// one is configured.
    ///
    /// The result carries a credential, so it must never be logged.
    fn connection_string(&self) -> Result<String> {
        let password = match (&self.postgres_password, &self.postgres_password_file) {
            (Some(_), Some(_)) => {
                bail!("only one of `postgres_password` and `postgres_password_file` may be set")
            }
            (Some(password), None) => Some(password.clone()),
            (None, Some(path)) => {
                let password = fs::read_to_string(path).with_context(|| {
                    format!("reading Postgres password file `{}`", path.display())
                })?;
                let password = password.trim_end_matches(['\r', '\n']).to_owned();
                if password.is_empty() {
                    bail!("Postgres password file `{}` is empty", path.display());
                }
                Some(password)
            }
            (None, None) => None,
        };

        let mut parts = vec![
            format!("host={}", escape(&self.postgres_host)),
            format!("port={}", self.postgres_port),
            format!("dbname={}", escape(&self.postgres_db)),
            format!("user={}", escape(&self.postgres_user)),
            format!("connect_timeout={}", self.postgres_connect_timeout_seconds),
            format!("application_name={}", escape("bip300-monitor")),
        ];
        if let Some(password) = password {
            parts.push(format!("password={}", escape(&password)));
        }
        Ok(parts.join(" "))
    }

    /// Return the maximum time allowed to establish the connection.
    pub const fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.postgres_connect_timeout_seconds)
    }
}

/// A libpq keyword/value connection string quotes with single quotes and
/// escapes backslashes.
fn escape(value: &str) -> String {
    format!("'{}'", value.replace('\\', r"\\").replace('\'', r"\'"))
}

/// Columns extracted from an envelope so the record can be queried without
/// decoding protobuf.
struct Facts<'a> {
    kind: &'static str,
    sidechain: Option<i16>,
    block_hash: Option<&'a [u8]>,
    height: Option<i32>,
}

fn facts(event: &Event) -> Result<Facts<'_>> {
    let payload = match event.monitor_event.as_ref() {
        Some(MonitorEvent::Enforcer(payload)) => payload,
        None => bail!("event envelope does not contain a monitor event"),
    };
    let payload = payload
        .event
        .as_ref()
        .context("enforcer event does not contain a concrete event")?;

    let sidechain = payload
        .sidechain_number()
        .map(|sidechain| {
            i16::try_from(sidechain)
                .with_context(|| format!("sidechain slot {sidechain} does not fit in a u8"))
        })
        .transpose()?;
    let anchor = event.observed_at_block.as_ref();
    let height = anchor
        .and_then(|anchor| anchor.height)
        .map(|height| {
            i32::try_from(height)
                .with_context(|| format!("block height {height} does not fit in an i32"))
        })
        .transpose()?;

    Ok(Facts {
        kind: payload.kind(),
        sidechain,
        block_hash: anchor.map(|anchor| anchor.hash.as_slice()),
        height,
    })
}

/// Handle to the Postgres record.
///
/// A transaction needs exclusive access to the connection, so the client sits
/// behind a mutex and concurrent writers queue. One slot worker per configured
/// sidechain plus one state worker, at mainchain block rate, never makes that
/// contention worth a pool.
#[derive(Clone)]
pub struct Store {
    client: Arc<Mutex<Client>>,
    source: &'static str,
    dataset_id: String,
    run_id: String,
    event_contract_version: i32,
}

/// Identity and build provenance of the record this extractor is extending.
#[derive(Clone, Debug)]
pub struct DatasetManifest {
    pub network_id: String,
    pub activation_height: u32,
    pub activation_block_hash: String,
    pub node_commit: String,
    pub enforcer_commit: String,
    pub monitor_commit: String,
    pub event_contract_version: u32,
    pub capabilities: JsonValue,
    pub creation_reason: String,
}

impl Default for DatasetManifest {
    fn default() -> Self {
        Self {
            network_id: "development".to_owned(),
            activation_height: 0,
            activation_block_hash: "unknown".to_owned(),
            node_commit: "unknown".to_owned(),
            enforcer_commit: "unknown".to_owned(),
            monitor_commit: "unknown".to_owned(),
            event_contract_version: crate::protobuf::enforcer_extractor::EVENT_CONTRACT_VERSION,
            capabilities: serde_json::json!([
                "event_facts",
                "event_observations",
                "tip_observations",
                "snapshot_consistency",
                "extractor_status"
            ]),
            creation_reason: "development or test dataset".to_owned(),
        }
    }
}

/// How an extractor acquired an observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureMethod {
    Startup,
    Live,
    Backfill,
    Poll,
    Reconcile,
}

/// Whether the mainchain tip stayed fixed around a grouped unary snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotConsistency {
    Stable,
    Changed,
}

impl SnapshotConsistency {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Changed => "changed",
        }
    }
}

/// Provenance shared by every event captured in one unary snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotMetadata {
    pub started_at: SystemTime,
    pub finished_at: SystemTime,
    pub tip_before: ObservedBlock,
    pub tip_after: ObservedBlock,
    pub consistency: SnapshotConsistency,
    pub attempts: u32,
}

impl CaptureMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Live => "live",
            Self::Backfill => "backfill",
            Self::Poll => "poll",
            Self::Reconcile => "reconcile",
        }
    }
}

/// Durable state of one bounded historical import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryCoverage {
    pub stream: String,
    pub sidechain: Option<u8>,
    pub sidechain_instance_id: Option<String>,
    pub event_contract_version: u32,
    pub coverage_start_height: u32,
    pub covered_tip: Option<ObservedBlock>,
    pub target_tip: ObservedBlock,
    pub floor_hash: Option<Vec<u8>>,
    pub floor_height: Option<u32>,
    pub next: Option<ObservedBlock>,
    pub status: HistoryStatus,
    pub rows_recorded: u64,
    pub effective_page_blocks: u32,
    pub last_error: Option<String>,
}

/// Whether a historical import is still walking, finished, or resumable after
/// a fatal page error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryStatus {
    Running,
    Complete,
    Error,
    Superseded,
}

impl HistoryStatus {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            "superseded" => Ok(Self::Superseded),
            other => bail!("record contains unknown history status `{other}`"),
        }
    }
}

/// Stable identity of one activation occupying a sidechain slot.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SidechainInstanceRef {
    pub sidechain: u8,
    pub sidechain_instance_id: String,
    pub activation_height: u32,
}

/// Derive the same sidechain-instance identity used by the durable record.
pub fn sidechain_instance_ref(
    sidechain: &crate::protobuf::enforcer_extractor::ActiveSidechain,
) -> Result<SidechainInstanceRef> {
    Ok(sidechain_instance_identity(sidechain)?.0)
}

fn sidechain_instance_identity(
    sidechain: &crate::protobuf::enforcer_extractor::ActiveSidechain,
) -> Result<(SidechainInstanceRef, Vec<u8>)> {
    let slot = u8::try_from(sidechain.sidechain_number).with_context(|| {
        format!(
            "active sidechain slot {} does not fit in a u8",
            sidechain.sidechain_number
        )
    })?;
    let first_hash = Sha256::digest(&sidechain.raw_description);
    let description_sha256d = Sha256::digest(first_hash).to_vec();
    Ok((
        SidechainInstanceRef {
            sidechain: slot,
            sidechain_instance_id: format!(
                "{}:{}:{}:{}",
                slot,
                sidechain.proposal_height,
                sidechain.activation_height,
                hex::encode(&description_sha256d)
            ),
            activation_height: sidechain.activation_height,
        },
        description_sha256d,
    ))
}

/// New cursor values committed together with one historical event page.
pub struct HistoryPage<'a> {
    pub stream: &'a str,
    pub sidechain: Option<u8>,
    pub sidechain_instance_id: Option<&'a str>,
    pub expected_next: &'a ObservedBlock,
    pub next: Option<&'a ObservedBlock>,
}

impl Store {
    /// Connect to the record and apply any outstanding schema statements.
    ///
    /// `source` names the extractor writing the rows.
    pub async fn connect(args: &PostgresArgs, source: &'static str) -> Result<Self> {
        Self::connect_with_manifest(args, source, DatasetManifest::default()).await
    }

    /// Connect and bind this process to one durable dataset and extractor run.
    pub async fn connect_with_manifest(
        args: &PostgresArgs,
        source: &'static str,
        manifest: DatasetManifest,
    ) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(&args.connection_string()?, NoTls)
            .await
            .with_context(|| {
                // Never interpolate the connection string: it carries the
                // password.
                format!(
                    "connecting to the Postgres record at {}:{}",
                    args.postgres_host, args.postgres_port
                )
            })?;

        // The connection future drives the socket. When it ends, every
        // subsequent query on the client fails, which is what makes a lost
        // record connection fatal rather than silent.
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "the Postgres record connection ended");
            }
        });

        let store = Self {
            client: Arc::new(Mutex::new(client)),
            source,
            dataset_id: String::new(),
            run_id: String::new(),
            event_contract_version: 0,
        };
        store.migrate().await?;
        let (dataset_id, run_id) = store.initialize_identity(&manifest).await?;
        Ok(Self {
            dataset_id,
            run_id,
            event_contract_version: i32::try_from(manifest.event_contract_version)
                .context("event contract version does not fit in an i32")?,
            ..store
        })
    }

    async fn initialize_identity(&self, manifest: &DatasetManifest) -> Result<(String, String)> {
        let activation_height = height_to_i32(manifest.activation_height)?;
        let event_contract_version = i32::try_from(manifest.event_contract_version)
            .context("event contract version does not fit in an i32")?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening the extractor identity transaction")?;
        let dataset_id: String = transaction
            .query_one(
                "INSERT INTO dataset_manifest
                    (network_id, activation_height, activation_block_hash,
                     initial_node_commit, initial_enforcer_commit, initial_monitor_commit,
                     initial_event_contract_version, capabilities, creation_reason)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT ON CONSTRAINT dataset_identity DO UPDATE SET
                    network_id = EXCLUDED.network_id
                 RETURNING dataset_id::text",
                &[
                    &manifest.network_id,
                    &activation_height,
                    &manifest.activation_block_hash,
                    &manifest.node_commit,
                    &manifest.enforcer_commit,
                    &manifest.monitor_commit,
                    &event_contract_version,
                    &manifest.capabilities,
                    &manifest.creation_reason,
                ],
            )
            .await
            .context("resolving the record dataset identity")?
            .get(0);
        let run_id: String = transaction
            .query_one(
                "INSERT INTO extractor_run
                    (dataset_id, source, node_commit, enforcer_commit, monitor_commit,
                     event_contract_version, capabilities)
                 VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7)
                 RETURNING run_id::text",
                &[
                    &dataset_id,
                    &self.source,
                    &manifest.node_commit,
                    &manifest.enforcer_commit,
                    &manifest.monitor_commit,
                    &event_contract_version,
                    &manifest.capabilities,
                ],
            )
            .await
            .context("starting an extractor run")?
            .get(0);
        transaction
            .execute(
                "INSERT INTO extractor_status (dataset_id, source, run_id)
                 VALUES ($1::text::uuid, $2, $3::text::uuid)
                 ON CONFLICT (dataset_id, source) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    last_error = NULL,
                    updated_at = now()",
                &[&dataset_id, &self.source, &run_id],
            )
            .await
            .context("initializing extractor status")?;
        transaction
            .commit()
            .await
            .context("committing extractor identity")?;
        Ok((dataset_id, run_id))
    }

    async fn migrate(&self) -> Result<()> {
        let client = self.client.lock().await;
        // Held for the whole migration, and released by the session ending even
        // if this returns early: two extractors starting at once must not both
        // decide a version is unapplied.
        client
            .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await
            .context("taking the record migration lock")?;
        let result = Self::apply_migrations(&client).await;
        // Reported rather than propagated: the session holds the lock, so a
        // failed unlock is released by the connection ending, and letting it
        // replace a migration failure would hide the error that matters.
        if let Err(error) = client
            .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_KEY])
            .await
        {
            tracing::warn!(%error, "could not release the record migration lock");
        }
        result
    }

    async fn apply_migrations(client: &Client) -> Result<()> {
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_version (
                     version integer PRIMARY KEY,
                     applied_at timestamptz NOT NULL DEFAULT now()
                 )",
            )
            .await
            .context("creating the schema version table")?;

        for (index, statements) in MIGRATIONS.iter().enumerate() {
            let version = i32::try_from(index + 1).expect("migration count fits in an i32");
            let applied = client
                .query_opt(
                    "SELECT version FROM schema_version WHERE version = $1",
                    &[&version],
                )
                .await
                .context("reading the applied schema version")?;
            if applied.is_some() {
                continue;
            }

            client
                .batch_execute(statements)
                .await
                .with_context(|| format!("applying schema version {version}"))?;
            client
                .execute(
                    "INSERT INTO schema_version (version) VALUES ($1)
                     ON CONFLICT (version) DO NOTHING",
                    &[&version],
                )
                .await
                .with_context(|| format!("recording schema version {version}"))?;
            tracing::info!(version, "applied a record schema version");
        }
        Ok(())
    }

    /// Record a batch of events in one transaction.
    ///
    /// Returns how many rows were inserted; a repeat of an already recorded
    /// observation is ignored rather than duplicated.
    pub async fn record(&self, events: &[Event]) -> Result<u64> {
        self.record_with_method(events, CaptureMethod::Live).await
    }

    /// Record facts and their capture occurrences in one transaction.
    pub async fn record_with_method(&self, events: &[Event], method: CaptureMethod) -> Result<u64> {
        self.record_batch(events, method, None).await
    }

    /// Record a sidechain-scoped batch against the exact activation that
    /// supplied it, even if that activation stops being current concurrently.
    pub async fn record_with_method_for_instance(
        &self,
        events: &[Event],
        method: CaptureMethod,
        instance: &SidechainInstanceRef,
    ) -> Result<u64> {
        self.record_batch(events, method, Some(instance)).await
    }

    async fn record_batch(
        &self,
        events: &[Event],
        method: CaptureMethod,
        instance: Option<&SidechainInstanceRef>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening a record transaction")?;
        if let Some(instance) = instance {
            validate_sidechain_instance(
                &transaction,
                &self.dataset_id,
                instance.sidechain,
                &instance.sidechain_instance_id,
            )
            .await?;
        }
        let first_capture_seq =
            reserve_capture_sequences(&transaction, &self.run_id, events.len()).await?;
        let mut instance_cache = BTreeMap::new();
        let mut inserted = 0;
        for (index, event) in events.iter().enumerate() {
            let capture_seq = first_capture_seq
                + i64::try_from(index).context("capture sequence offset overflow")?;
            inserted += insert(
                &transaction,
                self.source,
                &self.dataset_id,
                &self.run_id,
                self.event_contract_version,
                method,
                None,
                instance.map(|instance| i16::from(instance.sidechain)),
                instance.map(|instance| instance.sidechain_instance_id.as_str()),
                capture_seq,
                event,
                &mut instance_cache,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .context("committing a record transaction")?;

        Ok(inserted)
    }

    /// Record one grouped unary snapshot with its consistency window.
    pub async fn record_snapshot(
        &self,
        events: &[Event],
        method: CaptureMethod,
        metadata: &SnapshotMetadata,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }
        require_hash(&metadata.tip_before.hash, "snapshot tip before")?;
        require_hash(&metadata.tip_after.hash, "snapshot tip after")?;
        let tip_before_height = height_to_i32(required_height(
            &metadata.tip_before,
            "snapshot tip before",
        )?)?;
        let tip_after_height =
            height_to_i32(required_height(&metadata.tip_after, "snapshot tip after")?)?;
        let attempts = i32::try_from(metadata.attempts).context("snapshot attempts overflow")?;
        if attempts == 0 {
            bail!("snapshot attempts must be positive");
        }

        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening a snapshot record transaction")?;
        let snapshot_group_id: String = transaction
            .query_one(
                "INSERT INTO snapshot_group
                    (dataset_id, run_id, capture_method, started_at, finished_at,
                     tip_before_hash, tip_before_height, tip_after_hash,
                     tip_after_height, consistency, attempts)
                 VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 RETURNING snapshot_group_id::text",
                &[
                    &self.dataset_id,
                    &self.run_id,
                    &method.as_str(),
                    &metadata.started_at,
                    &metadata.finished_at,
                    &metadata.tip_before.hash,
                    &tip_before_height,
                    &metadata.tip_after.hash,
                    &tip_after_height,
                    &metadata.consistency.as_str(),
                    &attempts,
                ],
            )
            .await
            .context("recording snapshot consistency metadata")?
            .get(0);
        let first_capture_seq =
            reserve_capture_sequences(&transaction, &self.run_id, events.len()).await?;
        let mut instance_cache = BTreeMap::new();
        let mut inserted = 0_u64;
        for (index, event) in events.iter().enumerate() {
            let capture_seq = first_capture_seq
                + i64::try_from(index).context("capture sequence offset overflow")?;
            inserted += insert(
                &transaction,
                self.source,
                &self.dataset_id,
                &self.run_id,
                self.event_contract_version,
                method,
                Some(&snapshot_group_id),
                None,
                None,
                capture_seq,
                event,
                &mut instance_cache,
            )
            .await?;
        }
        update_extractor_status(
            &transaction,
            &self.dataset_id,
            self.source,
            &self.run_id,
            &metadata.tip_after,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("committing a snapshot record transaction")?;
        Ok(inserted)
    }

    /// Persist a mainchain-tip transition independently of deduplicated facts.
    pub async fn record_tip_observation(
        &self,
        tip: &ObservedBlock,
        previous: Option<&ObservedBlock>,
        method: CaptureMethod,
        observed_at: SystemTime,
    ) -> Result<()> {
        let tip_height = height_to_i32(required_height(tip, "observed tip")?)?;
        require_hash(&tip.hash, "observed tip")?;
        let (previous_hash, previous_height) = optional_block_parts(previous)?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening a tip observation transaction")?;
        let capture_seq = reserve_capture_sequences(&transaction, &self.run_id, 1).await?;
        transaction
            .execute(
                "INSERT INTO tip_observation
                    (dataset_id, run_id, capture_seq, capture_method,
                     tip_hash, tip_height, previous_observed_hash,
                     previous_observed_height, observed_at)
                 VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5, $6, $7, $8, $9)",
                &[
                    &self.dataset_id,
                    &self.run_id,
                    &capture_seq,
                    &method.as_str(),
                    &tip.hash,
                    &tip_height,
                    &previous_hash,
                    &previous_height,
                    &observed_at,
                ],
            )
            .await
            .context("recording a mainchain tip observation")?;
        update_extractor_status(
            &transaction,
            &self.dataset_id,
            self.source,
            &self.run_id,
            tip,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("committing a mainchain tip observation")?;
        Ok(())
    }

    /// Record an operational extractor error without changing durable facts.
    pub async fn record_extractor_error(&self, error: &str) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE extractor_status
                    SET last_error = $4, updated_at = now()
                  WHERE dataset_id = $1::text::uuid AND source = $2 AND run_id = $3::text::uuid",
                &[&self.dataset_id, &self.source, &self.run_id, &error],
            )
            .await
            .context("recording the extractor error")?;
        Ok(())
    }

    /// Clear a previously reported operational error after recovery.
    pub async fn clear_extractor_error(&self) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE extractor_status
                    SET last_error = NULL, updated_at = now()
                  WHERE dataset_id = $1::text::uuid AND source = $2
                    AND run_id = $3::text::uuid AND last_error IS NOT NULL",
                &[&self.dataset_id, &self.source, &self.run_id],
            )
            .await
            .context("clearing the extractor error")?;
        Ok(())
    }

    /// Close a run cleanly; a row left `running` documents an unclean exit.
    pub async fn finish_run(&self, status: &str, reason: Option<&str>) -> Result<()> {
        if !matches!(status, "completed" | "failed") {
            bail!("invalid terminal extractor run status `{status}`");
        }
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening the extractor finish transaction")?;
        transaction
            .execute(
                "UPDATE extractor_run
                    SET status = $2, finished_at = now(), finish_reason = $3
                  WHERE run_id = $1::text::uuid AND status = 'running'",
                &[&self.run_id, &status, &reason],
            )
            .await
            .context("finishing extractor run")?;
        transaction
            .execute(
                "UPDATE extractor_status
                    SET last_error = CASE WHEN $4 = 'failed' THEN $5 ELSE NULL END,
                        updated_at = now()
                  WHERE dataset_id = $1::text::uuid AND source = $2
                    AND run_id = $3::text::uuid",
                &[
                    &self.dataset_id,
                    &self.source,
                    &self.run_id,
                    &status,
                    &reason,
                ],
            )
            .await
            .context("updating terminal extractor status")?;
        transaction
            .commit()
            .await
            .context("committing the extractor finish transaction")?;
        Ok(())
    }

    /// Resolve the instance currently occupying one sidechain slot.
    ///
    /// An inactive or not-yet-recorded slot is ordinary absence, not a store
    /// failure; lifecycle callers decide whether to wait, skip, or retire it.
    pub async fn current_sidechain_instance_id(&self, sidechain: u8) -> Result<Option<String>> {
        let client = self.client.lock().await;
        current_sidechain_instance_client(&client, &self.dataset_id, i16::from(sidechain)).await
    }

    /// Resolve every sidechain instance in the latest durable state snapshot.
    pub async fn current_sidechain_instances(&self) -> Result<Vec<SidechainInstanceRef>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT current.sidechain, current.sidechain_instance_id,
                        instance.activation_height
                   FROM current_sidechain_instance current
                   JOIN sidechain_instance instance
                     ON instance.dataset_id = current.dataset_id
                    AND instance.sidechain_instance_id = current.sidechain_instance_id
                    AND instance.sidechain = current.sidechain
                  WHERE current.dataset_id = $1::text::uuid
                  ORDER BY current.sidechain",
                &[&self.dataset_id],
            )
            .await
            .context("resolving the current sidechain instances")?;
        rows.into_iter()
            .map(|row| {
                let sidechain = row.get::<_, i16>(0);
                let activation_height = row.get::<_, i32>(2);
                Ok(SidechainInstanceRef {
                    sidechain: u8::try_from(sidechain).with_context(|| {
                        format!("current sidechain slot {sidechain} is invalid")
                    })?,
                    sidechain_instance_id: row.get(1),
                    activation_height: u32::try_from(activation_height).with_context(|| {
                        format!("sidechain activation height {activation_height} is negative")
                    })?,
                })
            })
            .collect()
    }

    /// Read the durable cursor for one historical stream.
    pub async fn history_coverage(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        sidechain_instance_id: Option<&str>,
    ) -> Result<Option<HistoryCoverage>> {
        validate_history_scope(sidechain, sidechain_instance_id)?;
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT stream, sidechain, sidechain_instance_id, coverage_start_height,
                        covered_tip_hash, covered_tip_height,
                        target_tip_hash, target_tip_height,
                        floor_hash, floor_height, next_hash, next_height,
                        status, rows_recorded, effective_page_blocks, last_error,
                        event_contract_version
                   FROM history_coverage
                  WHERE dataset_id = $1::text::uuid AND source = $2 AND stream = $3
                    AND sidechain IS NOT DISTINCT FROM $4
                    AND sidechain_instance_id IS NOT DISTINCT FROM $5
                    AND event_contract_version = $6",
                &[
                    &self.dataset_id,
                    &self.source,
                    &stream,
                    &sidechain,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| format!("reading {stream} history coverage"))?;

        row.map(history_coverage_from_row).transpose()
    }

    /// Start a new backwards walk while preserving the cumulative inserted-row
    /// count from earlier completed walks.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_history_cycle(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        sidechain_instance_id: Option<&str>,
        coverage_start_height: u32,
        covered_tip: Option<&ObservedBlock>,
        target_tip: &ObservedBlock,
        floor_hash: Option<&[u8]>,
        floor_height: Option<u32>,
        effective_page_blocks: u32,
    ) -> Result<HistoryCoverage> {
        validate_history_scope(sidechain, sidechain_instance_id)?;
        let sidechain = sidechain.map(i16::from);
        let coverage_start_height = height_to_i32(coverage_start_height)?;
        let (covered_tip_hash, covered_tip_height) = optional_block_parts(covered_tip)?;
        let target_tip_height = required_height(target_tip, "history target tip")?;
        let target_tip_height = height_to_i32(target_tip_height)?;
        let floor_height = floor_height.map(height_to_i32).transpose()?;
        let page_blocks = height_to_i32(effective_page_blocks)?;

        let client = self.client.lock().await;
        let row = client
            .query_one(
                "INSERT INTO history_coverage
                    (dataset_id, event_contract_version, source, stream, sidechain,
                     sidechain_instance_id,
                     coverage_start_height,
                     covered_tip_hash, covered_tip_height,
                     target_tip_hash, target_tip_height,
                     floor_hash, floor_height, next_hash, next_height,
                     status, effective_page_blocks, last_error,
                     started_at, updated_at, completed_at)
                 VALUES
                    ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                     $12, $13, $10, $11, 'running', $14, NULL, now(), now(), NULL)
                 ON CONFLICT ON CONSTRAINT history_coverage_identity DO UPDATE SET
                    coverage_start_height = EXCLUDED.coverage_start_height,
                    covered_tip_hash = EXCLUDED.covered_tip_hash,
                    covered_tip_height = EXCLUDED.covered_tip_height,
                    target_tip_hash = EXCLUDED.target_tip_hash,
                    target_tip_height = EXCLUDED.target_tip_height,
                    floor_hash = EXCLUDED.floor_hash,
                    floor_height = EXCLUDED.floor_height,
                    next_hash = EXCLUDED.next_hash,
                    next_height = EXCLUDED.next_height,
                    status = 'running',
                    effective_page_blocks = EXCLUDED.effective_page_blocks,
                    last_error = NULL,
                    started_at = now(),
                    updated_at = now(),
                    completed_at = NULL
                 RETURNING stream, sidechain, sidechain_instance_id, coverage_start_height,
                           covered_tip_hash, covered_tip_height,
                           target_tip_hash, target_tip_height,
                           floor_hash, floor_height, next_hash, next_height,
                           status, rows_recorded, effective_page_blocks, last_error,
                           event_contract_version",
                &[
                    &self.dataset_id,
                    &self.event_contract_version,
                    &self.source,
                    &stream,
                    &sidechain,
                    &sidechain_instance_id,
                    &coverage_start_height,
                    &covered_tip_hash,
                    &covered_tip_height,
                    &target_tip.hash,
                    &target_tip_height,
                    &floor_hash,
                    &floor_height,
                    &page_blocks,
                ],
            )
            .await
            .with_context(|| format!("starting a {stream} history cycle"))?;

        history_coverage_from_row(row)
    }

    /// Record one bounded historical page and advance its cursor atomically.
    ///
    /// Historical rows deliberately bypass the live publisher.  This keeps a
    /// first-sight import out of NATS and, more importantly, means only this
    /// small page exists in memory at once.
    pub async fn record_history_page(
        &self,
        events: &[Event],
        page: HistoryPage<'_>,
    ) -> Result<u64> {
        if events.is_empty() {
            bail!("a historical page cannot be empty");
        }
        validate_history_scope(page.sidechain, page.sidechain_instance_id)?;
        let sidechain = page.sidechain.map(i16::from);
        let expected_height = required_height(page.expected_next, "history page cursor")?;
        let expected_height = height_to_i32(expected_height)?;
        let (next_hash, next_height) = optional_block_parts(page.next)?;

        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .context("opening a historical page transaction")?;
        if let (Some(sidechain), Some(instance_id)) = (page.sidechain, page.sidechain_instance_id) {
            validate_sidechain_instance(&transaction, &self.dataset_id, sidechain, instance_id)
                .await?;
        }
        let first_capture_seq =
            reserve_capture_sequences(&transaction, &self.run_id, events.len()).await?;
        let mut instance_cache = BTreeMap::new();
        let mut inserted = 0;
        for (index, event) in events.iter().enumerate() {
            let capture_seq = first_capture_seq
                + i64::try_from(index).context("capture sequence offset overflow")?;
            inserted += insert(
                &transaction,
                self.source,
                &self.dataset_id,
                &self.run_id,
                self.event_contract_version,
                CaptureMethod::Backfill,
                None,
                sidechain,
                page.sidechain_instance_id,
                capture_seq,
                event,
                &mut instance_cache,
            )
            .await?;
        }
        let inserted_i64 = i64::try_from(inserted).context("history page insert count overflow")?;
        let complete = page.next.is_none();
        let updated = transaction
            .execute(
                "UPDATE history_coverage
                    SET next_hash = $7,
                        next_height = $8,
                        status = CASE WHEN $9 THEN 'complete' ELSE 'running' END,
                        covered_tip_hash = CASE WHEN $9 THEN target_tip_hash ELSE covered_tip_hash END,
                        covered_tip_height = CASE WHEN $9 THEN target_tip_height ELSE covered_tip_height END,
                        rows_recorded = rows_recorded + $10,
                        last_error = NULL,
                        updated_at = now(),
                        completed_at = CASE WHEN $9 THEN now() ELSE NULL END
                  WHERE dataset_id = $1::text::uuid AND source = $2 AND stream = $3
                    AND sidechain IS NOT DISTINCT FROM $4
                    AND sidechain_instance_id IS NOT DISTINCT FROM $11
                    AND event_contract_version = $12
                    AND next_hash = $5 AND next_height = $6
                    AND status = 'running'",
                &[
                    &self.dataset_id,
                    &self.source,
                    &page.stream,
                    &sidechain,
                    &page.expected_next.hash,
                    &expected_height,
                    &next_hash,
                    &next_height,
                    &complete,
                    &inserted_i64,
                    &page.sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| format!("advancing {} history coverage", page.stream))?;
        if updated != 1 {
            bail!(
                "{} history cursor changed before its page could be committed",
                page.stream
            );
        }
        transaction
            .commit()
            .await
            .context("committing a historical page transaction")?;
        Ok(inserted)
    }

    /// Persist a smaller safe page size after a retryable gRPC failure.
    pub async fn resize_history_page(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        sidechain_instance_id: Option<&str>,
        effective_page_blocks: u32,
        error: &str,
    ) -> Result<()> {
        validate_history_scope(sidechain, sidechain_instance_id)?;
        let sidechain = sidechain.map(i16::from);
        let page_blocks = height_to_i32(effective_page_blocks)?;
        let client = self.client.lock().await;
        let updated = client
            .execute(
                "UPDATE history_coverage
                    SET effective_page_blocks = $5, last_error = $6, updated_at = now()
                  WHERE dataset_id = $1::text::uuid AND source = $2 AND stream = $3
                    AND sidechain IS NOT DISTINCT FROM $4
                    AND sidechain_instance_id IS NOT DISTINCT FROM $7
                    AND event_contract_version = $8
                    AND status = 'running'",
                &[
                    &self.dataset_id,
                    &self.source,
                    &stream,
                    &sidechain,
                    &page_blocks,
                    &error,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| format!("resizing {stream} history pages"))?;
        if updated != 1 {
            bail!("{stream} history was not running while resizing its pages");
        }
        Ok(())
    }

    /// Resume the exact cursor left by a previous fatal page failure.
    pub async fn resume_history(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        sidechain_instance_id: Option<&str>,
    ) -> Result<()> {
        validate_history_scope(sidechain, sidechain_instance_id)?;
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let updated = client
            .execute(
                "UPDATE history_coverage
                    SET status = 'running', last_error = NULL, updated_at = now()
                  WHERE dataset_id = $1::text::uuid AND source = $2 AND stream = $3
                    AND sidechain IS NOT DISTINCT FROM $4
                    AND sidechain_instance_id IS NOT DISTINCT FROM $5
                    AND event_contract_version = $6
                    AND status IN ('error', 'superseded') AND next_hash IS NOT NULL",
                &[
                    &self.dataset_id,
                    &self.source,
                    &stream,
                    &sidechain,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| format!("resuming {stream} history"))?;
        if updated != 1 {
            bail!("{stream} history had no resumable cursor");
        }
        Ok(())
    }

    /// Settle a failed cursor against the durable sidechain lifecycle.
    ///
    /// A scoped history that stopped being current while an RPC was in flight is
    /// superseded atomically instead of being reported as an extractor failure.
    pub async fn settle_history_failure(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        sidechain_instance_id: Option<&str>,
        error: &str,
    ) -> Result<HistoryStatus> {
        validate_history_scope(sidechain, sidechain_instance_id)?;
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "WITH scope AS (
                     SELECT $4::smallint IS NULL OR EXISTS (
                         SELECT 1
                           FROM current_sidechain_instance current
                          WHERE current.dataset_id = $1::text::uuid
                            AND current.sidechain = $4
                            AND current.sidechain_instance_id = $6
                     ) AS is_current
                 ), updated AS (
                     UPDATE history_coverage coverage
                        SET status = CASE
                                WHEN scope.is_current THEN 'error'
                                ELSE 'superseded'
                            END,
                            last_error = CASE
                                WHEN scope.is_current THEN $5
                                ELSE 'sidechain instance is no longer active'
                            END,
                            updated_at = now()
                       FROM scope
                      WHERE coverage.dataset_id = $1::text::uuid
                        AND coverage.source = $2 AND coverage.stream = $3
                        AND coverage.sidechain IS NOT DISTINCT FROM $4
                        AND coverage.sidechain_instance_id IS NOT DISTINCT FROM $6
                        AND coverage.event_contract_version = $7
                        AND coverage.status = 'running'
                     RETURNING coverage.status
                 )
                 SELECT status FROM updated
                 UNION ALL
                 SELECT coverage.status
                   FROM history_coverage coverage
                  WHERE coverage.dataset_id = $1::text::uuid
                    AND coverage.source = $2 AND coverage.stream = $3
                    AND coverage.sidechain IS NOT DISTINCT FROM $4
                    AND coverage.sidechain_instance_id IS NOT DISTINCT FROM $6
                    AND coverage.event_contract_version = $7
                    AND NOT EXISTS (SELECT 1 FROM updated)
                 LIMIT 1",
                &[
                    &self.dataset_id,
                    &self.source,
                    &stream,
                    &sidechain,
                    &error,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| format!("settling a failed {stream} history cursor"))?
            .with_context(|| format!("{stream} history coverage does not exist"))?;
        HistoryStatus::parse(row.get(0))
    }

    /// Stop extending an incomplete cursor after its sidechain activation is
    /// no longer current. Completed coverage remains a valid historical fact.
    pub async fn supersede_history(
        &self,
        stream: &str,
        sidechain: u8,
        sidechain_instance_id: &str,
        reason: &str,
    ) -> Result<()> {
        let sidechain = i16::from(sidechain);
        let client = self.client.lock().await;
        client
            .execute(
                "UPDATE history_coverage
                    SET status = 'superseded', last_error = $5, updated_at = now()
                  WHERE dataset_id = $1::text::uuid
                    AND event_contract_version = $6
                    AND source = $2 AND stream = $3 AND sidechain = $4
                    AND sidechain_instance_id = $7
                    AND status IN ('running', 'error')",
                &[
                    &self.dataset_id,
                    &self.source,
                    &stream,
                    &sidechain,
                    &reason,
                    &self.event_contract_version,
                    &sidechain_instance_id,
                ],
            )
            .await
            .with_context(|| format!("superseding {stream} history for sidechain {sidechain}"))?;
        Ok(())
    }

    /// Newest recorded block of one kind for one sidechain slot.
    ///
    /// The hash is what a range backfill needs as its starting point; the
    /// height only bounds how much it may ask for.
    pub async fn last_recorded_block(
        &self,
        kind: &str,
        sidechain: u8,
        sidechain_instance_id: &str,
    ) -> Result<Option<(Vec<u8>, u32)>> {
        let sidechain = i16::from(sidechain);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT block_hash, height FROM event
                 WHERE dataset_id = $1::text::uuid AND source = $2 AND kind = $3 AND sidechain = $4
                   AND sidechain_instance_id = $5
                   AND event_contract_version = $6
                   AND block_hash IS NOT NULL AND height IS NOT NULL
                 ORDER BY height DESC
                 LIMIT 1",
                &[
                    &self.dataset_id,
                    &self.source,
                    &kind,
                    &sidechain,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| {
                format!("reading the last recorded {kind} block for sidechain {sidechain}")
            })?;

        let Some(row) = row else {
            return Ok(None);
        };
        let hash: Vec<u8> = row.get(0);
        let height: i32 = row.get(1);
        let height = u32::try_from(height)
            .with_context(|| format!("recorded height {height} is negative"))?;

        Ok(Some((hash, height)))
    }

    /// Height of the newest recorded event of one kind for one sidechain slot.
    ///
    /// This is the backfill checkpoint: because the record is written before
    /// anything is published, it cannot claim an event that was not stored.
    pub async fn last_recorded_height(
        &self,
        kind: &str,
        sidechain: u8,
        sidechain_instance_id: &str,
    ) -> Result<Option<u32>> {
        let sidechain = i16::from(sidechain);
        let client = self.client.lock().await;
        let row = client
            .query_one(
                "SELECT max(height) FROM event
                 WHERE dataset_id = $1::text::uuid AND source = $2 AND kind = $3 AND sidechain = $4
                   AND sidechain_instance_id = $5
                   AND event_contract_version = $6",
                &[
                    &self.dataset_id,
                    &self.source,
                    &kind,
                    &sidechain,
                    &sidechain_instance_id,
                    &self.event_contract_version,
                ],
            )
            .await
            .with_context(|| {
                format!("reading the last recorded {kind} height for sidechain {sidechain}")
            })?;

        let height: Option<i32> = row.get(0);
        height
            .map(|height| {
                u32::try_from(height)
                    .with_context(|| format!("recorded height {height} is negative"))
            })
            .transpose()
    }
}

fn history_coverage_from_row(row: Row) -> Result<HistoryCoverage> {
    let sidechain: Option<i16> = row.get(1);
    let sidechain = sidechain
        .map(|slot| u8::try_from(slot).with_context(|| format!("invalid history sidechain {slot}")))
        .transpose()?;
    let covered_tip_hash: Option<Vec<u8>> = row.get(4);
    let covered_tip_height: Option<i32> = row.get(5);
    let covered_tip = block_from_parts(covered_tip_hash, covered_tip_height, "covered tip")?;
    let target_tip_hash: Vec<u8> = row.get(6);
    let target_tip_height: i32 = row.get(7);
    let target_tip =
        block_from_parts(Some(target_tip_hash), Some(target_tip_height), "target tip")?
            .context("history target tip is missing")?;
    let next_hash: Option<Vec<u8>> = row.get(10);
    let next_height: Option<i32> = row.get(11);
    let status: String = row.get(12);
    let rows_recorded: i64 = row.get(13);
    let effective_page_blocks: i32 = row.get(14);

    Ok(HistoryCoverage {
        stream: row.get(0),
        sidechain,
        sidechain_instance_id: row.get(2),
        event_contract_version: u32::try_from(row.get::<_, i32>(16))
            .context("history event contract version is not positive")?,
        coverage_start_height: height_from_i32(row.get(3), "coverage start")?,
        covered_tip,
        target_tip,
        floor_hash: row.get(8),
        floor_height: row
            .get::<_, Option<i32>>(9)
            .map(|height| height_from_i32(height, "history floor"))
            .transpose()?,
        next: block_from_parts(next_hash, next_height, "next history cursor")?,
        status: HistoryStatus::parse(&status)?,
        rows_recorded: u64::try_from(rows_recorded).context("history rows_recorded is negative")?,
        effective_page_blocks: u32::try_from(effective_page_blocks)
            .context("history page size is not positive")?,
        last_error: row.get(15),
    })
}

fn validate_history_scope(
    sidechain: Option<u8>,
    sidechain_instance_id: Option<&str>,
) -> Result<()> {
    if sidechain.is_some() != sidechain_instance_id.is_some() {
        bail!("history sidechain and sidechain instance must be present together");
    }
    Ok(())
}

fn required_height(block: &ObservedBlock, name: &str) -> Result<u32> {
    block
        .height
        .with_context(|| format!("{name} has no height"))
}

fn optional_block_parts(block: Option<&ObservedBlock>) -> Result<(Option<Vec<u8>>, Option<i32>)> {
    match block {
        Some(block) => Ok((
            Some(block.hash.clone()),
            Some(height_to_i32(required_height(block, "history block")?)?),
        )),
        None => Ok((None, None)),
    }
}

fn block_from_parts(
    hash: Option<Vec<u8>>,
    height: Option<i32>,
    name: &str,
) -> Result<Option<ObservedBlock>> {
    match (hash, height) {
        (Some(hash), Some(height)) => Ok(Some(ObservedBlock::at_height(
            hash,
            height_from_i32(height, name)?,
        ))),
        (None, None) => Ok(None),
        _ => bail!("record contains an incomplete {name}"),
    }
}

fn height_to_i32(height: u32) -> Result<i32> {
    i32::try_from(height).with_context(|| format!("block height {height} does not fit in an i32"))
}

fn height_from_i32(height: i32, name: &str) -> Result<u32> {
    u32::try_from(height).with_context(|| format!("{name} height {height} is negative"))
}

#[allow(clippy::too_many_arguments)]
async fn insert(
    transaction: &Transaction<'_>,
    source: &'static str,
    dataset_id: &str,
    run_id: &str,
    event_contract_version: i32,
    method: CaptureMethod,
    snapshot_group_id: Option<&str>,
    explicit_sidechain: Option<i16>,
    explicit_sidechain_instance_id: Option<&str>,
    capture_seq: i64,
    event: &Event,
    instance_cache: &mut BTreeMap<i16, String>,
) -> Result<u64> {
    let facts = facts(event).context("describing an event for the record")?;
    let payload = json::render(event).context("rendering an event payload as JSON")?;
    let envelope = event.encode_to_vec();
    let envelope_sha256 = Sha256::digest(&envelope).to_vec();
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_millis(event.timestamp);
    update_sidechain_instances(transaction, dataset_id, event, observed_at, instance_cache).await?;
    let sidechain_instance_id = match (
        facts.sidechain,
        explicit_sidechain,
        explicit_sidechain_instance_id,
    ) {
        (None, None, None) => None,
        (None, Some(_), _) | (None, _, Some(_)) => bail!(
            "a global {} event was given a sidechain instance",
            facts.kind
        ),
        (Some(sidechain), Some(instance_sidechain), Some(instance_id)) => {
            if sidechain != instance_sidechain {
                bail!(
                    "a slot {sidechain} {} event was assigned to slot {instance_sidechain}",
                    facts.kind
                );
            }
            Some(instance_id.to_owned())
        }
        (Some(_), Some(_), None) | (Some(_), None, Some(_)) => {
            bail!("an explicit sidechain instance is incomplete")
        }
        (Some(sidechain), None, None) => {
            if let Some(instance_id) = instance_cache.get(&sidechain) {
                Some(instance_id.clone())
            } else {
                let instance_id =
                    current_sidechain_instance(transaction, dataset_id, Some(sidechain))
                        .await?
                        .with_context(|| {
                            format!(
                                "no active sidechain instance is registered for slot {sidechain}"
                            )
                        })?;
                instance_cache.insert(sidechain, instance_id.clone());
                Some(instance_id)
            }
        }
    };

    let row = transaction
        .query_one(
            "WITH inserted_fact AS (
                 INSERT INTO event
                     (dataset_id, observed_at, source, kind, sidechain, block_hash,
                      height, envelope, payload, envelope_sha256,
                      sidechain_instance_id, event_contract_version)
                 VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $9,
                         $10, $11, $12)
                 ON CONFLICT ON CONSTRAINT event_identity DO NOTHING
                 RETURNING id
             ), resolved_fact AS MATERIALIZED (
                 SELECT id, TRUE AS inserted FROM inserted_fact
                 UNION ALL
                 SELECT id, FALSE AS inserted
                   FROM event
                  WHERE dataset_id = $1::text::uuid
                    AND event_contract_version = $12
                    AND source = $3 AND kind = $4
                    AND sidechain IS NOT DISTINCT FROM $5
                    AND block_hash IS NOT DISTINCT FROM $6
                    AND sidechain_instance_id IS NOT DISTINCT FROM $11
                    AND NOT EXISTS (SELECT 1 FROM inserted_fact)
             ), inserted_observation AS (
                 INSERT INTO event_observation
                     (dataset_id, run_id, capture_seq, capture_method, event_id,
                      snapshot_group_id, observed_at)
                 SELECT $1::text::uuid, $13::text::uuid, $14, $15, id,
                        $16::text::uuid, $2
                   FROM resolved_fact
                 RETURNING event_id
             )
             SELECT resolved_fact.inserted
               FROM resolved_fact
               JOIN inserted_observation
                 ON inserted_observation.event_id = resolved_fact.id",
            &[
                &dataset_id,
                &observed_at,
                &source,
                &facts.kind,
                &facts.sidechain,
                &facts.block_hash,
                &facts.height,
                &envelope,
                &payload,
                &envelope_sha256,
                &sidechain_instance_id,
                &event_contract_version,
                &run_id,
                &capture_seq,
                &method.as_str(),
                &snapshot_group_id,
            ],
        )
        .await
        .with_context(|| format!("recording a {} event fact and occurrence", facts.kind))?;
    Ok(u64::from(row.get::<_, bool>(0)))
}

async fn update_sidechain_instances(
    transaction: &Transaction<'_>,
    dataset_id: &str,
    event: &Event,
    observed_at: SystemTime,
    instance_cache: &mut BTreeMap<i16, String>,
) -> Result<()> {
    let Some(MonitorEvent::Enforcer(payload)) = event.monitor_event.as_ref() else {
        return Ok(());
    };
    let Some(crate::protobuf::enforcer_extractor::enforcer_event::Event::ActiveSidechains(
        snapshot,
    )) = payload.event.as_ref()
    else {
        return Ok(());
    };

    transaction
        .execute(
            "DELETE FROM current_sidechain_instance WHERE dataset_id = $1::text::uuid",
            &[&dataset_id],
        )
        .await
        .context("resetting the current sidechain-instance map")?;
    instance_cache.clear();
    for sidechain in &snapshot.sidechains {
        let slot = i16::try_from(sidechain.sidechain_number).with_context(|| {
            format!(
                "active sidechain slot {} does not fit in a smallint",
                sidechain.sidechain_number
            )
        })?;
        if !(0..=255).contains(&slot) {
            bail!("active sidechain slot {slot} is outside 0..=255");
        }
        let proposal_height = height_to_i32(sidechain.proposal_height)?;
        let activation_height = height_to_i32(sidechain.activation_height)?;
        let (instance, description_sha256d) = sidechain_instance_identity(sidechain)?;
        transaction
            .execute(
                "INSERT INTO sidechain_instance
                    (dataset_id, sidechain_instance_id, sidechain, raw_description,
                     description_sha256d, proposal_height, activation_height,
                     first_seen_at, last_seen_at)
                 VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $8)
                 ON CONFLICT (dataset_id, sidechain_instance_id) DO UPDATE SET
                    last_seen_at = EXCLUDED.last_seen_at",
                &[
                    &dataset_id,
                    &instance.sidechain_instance_id,
                    &slot,
                    &sidechain.raw_description,
                    &description_sha256d,
                    &proposal_height,
                    &activation_height,
                    &observed_at,
                ],
            )
            .await
            .context("recording a sidechain instance")?;
        transaction
            .execute(
                "INSERT INTO current_sidechain_instance
                    (dataset_id, sidechain, sidechain_instance_id, observed_at)
                 VALUES ($1::text::uuid, $2, $3, $4)",
                &[
                    &dataset_id,
                    &slot,
                    &instance.sidechain_instance_id,
                    &observed_at,
                ],
            )
            .await
            .context("recording the current sidechain instance")?;
        instance_cache.insert(slot, instance.sidechain_instance_id);
    }
    Ok(())
}

async fn current_sidechain_instance(
    transaction: &Transaction<'_>,
    dataset_id: &str,
    sidechain: Option<i16>,
) -> Result<Option<String>> {
    let Some(sidechain) = sidechain else {
        return Ok(None);
    };
    Ok(transaction
        .query_opt(
            "SELECT sidechain_instance_id
               FROM current_sidechain_instance
              WHERE dataset_id = $1::text::uuid AND sidechain = $2",
            &[&dataset_id, &sidechain],
        )
        .await
        .context("resolving the current sidechain instance")?
        .map(|row| row.get(0)))
}

async fn current_sidechain_instance_client(
    client: &Client,
    dataset_id: &str,
    sidechain: i16,
) -> Result<Option<String>> {
    Ok(client
        .query_opt(
            "SELECT sidechain_instance_id
               FROM current_sidechain_instance
              WHERE dataset_id = $1::text::uuid AND sidechain = $2",
            &[&dataset_id, &sidechain],
        )
        .await
        .context("resolving the current sidechain instance")?
        .map(|row| row.get(0)))
}

async fn reserve_capture_sequences(
    transaction: &Transaction<'_>,
    run_id: &str,
    count: usize,
) -> Result<i64> {
    let count = i64::try_from(count).context("capture sequence count overflow")?;
    if count == 0 {
        bail!("cannot reserve an empty capture sequence range");
    }
    let row = transaction
        .query_opt(
            "UPDATE extractor_run
                SET last_capture_seq = last_capture_seq + $2
              WHERE run_id = $1::text::uuid AND status = 'running'
              RETURNING last_capture_seq - $2 + 1",
            &[&run_id, &count],
        )
        .await
        .context("allocating extractor capture sequences")?
        .context("extractor run is not active")?;
    Ok(row.get(0))
}

async fn validate_sidechain_instance(
    transaction: &Transaction<'_>,
    dataset_id: &str,
    sidechain: u8,
    sidechain_instance_id: &str,
) -> Result<()> {
    let sidechain = i16::from(sidechain);
    let exists: bool = transaction
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM sidechain_instance
                  WHERE dataset_id = $1::text::uuid
                    AND sidechain = $2
                    AND sidechain_instance_id = $3
             )",
            &[&dataset_id, &sidechain, &sidechain_instance_id],
        )
        .await
        .context("validating a sidechain instance")?
        .get(0);
    if !exists {
        bail!(
            "sidechain instance `{sidechain_instance_id}` is not registered for slot {sidechain}"
        );
    }
    Ok(())
}

async fn update_extractor_status(
    transaction: &Transaction<'_>,
    dataset_id: &str,
    source: &str,
    run_id: &str,
    tip: &ObservedBlock,
) -> Result<()> {
    let height = height_to_i32(required_height(tip, "extractor status tip")?)?;
    transaction
        .execute(
            "UPDATE extractor_status
                SET last_tip_hash = $4,
                    last_tip_height = $5,
                    last_error = NULL,
                    updated_at = now()
              WHERE dataset_id = $1::text::uuid AND source = $2 AND run_id = $3::text::uuid",
            &[&dataset_id, &source, &run_id, &tip.hash, &height],
        )
        .await
        .context("updating extractor status")?;
    Ok(())
}

fn require_hash(hash: &[u8], name: &str) -> Result<()> {
    if hash.len() != 32 {
        bail!("{name} hash is {} bytes instead of 32", hash.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{PostgresArgs, escape, facts};
    use crate::protobuf::enforcer_extractor as events;
    use crate::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};

    fn envelope(payload: events::enforcer_event::Event, anchor: Option<ObservedBlock>) -> Event {
        Event {
            timestamp: 1_700_000_000_000,
            observed_at_block: anchor,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(payload),
            })),
        }
    }

    #[test]
    fn facts_are_read_from_the_payload_and_the_anchor() {
        let event = envelope(
            events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                sidechain_number: 98,
                ctip: None,
            }),
            Some(ObservedBlock::at_height(vec![0x11; 32], 996_259)),
        );

        let facts = facts(&event).expect("described event");
        assert_eq!(facts.kind, "ctip");
        assert_eq!(facts.sidechain, Some(98));
        assert_eq!(facts.height, Some(996_259));
        assert_eq!(facts.block_hash, Some([0x11; 32].as_slice()));
    }

    #[test]
    fn an_absent_height_stays_absent_rather_than_becoming_zero() {
        let event = envelope(
            events::enforcer_event::Event::BlockDisconnected(events::BlockDisconnected {
                block_hash: vec![0x22; 32],
                sidechain_number: 9,
            }),
            Some(ObservedBlock::without_height(vec![0x22; 32])),
        );

        let facts = facts(&event).expect("described event");
        assert_eq!(facts.kind, "block_disconnected");
        assert_eq!(facts.height, None);
    }

    #[test]
    fn an_unscoped_event_records_no_sidechain() {
        let event = envelope(
            events::enforcer_event::Event::ChainInfo(events::ChainInfo::default()),
            Some(ObservedBlock::at_height(vec![0x33; 32], 1)),
        );

        assert_eq!(facts(&event).expect("described event").sidechain, None);
    }

    #[test]
    fn an_empty_envelope_is_rejected_rather_than_recorded_as_null() {
        let event = Event {
            timestamp: 1,
            observed_at_block: None,
            monitor_event: None,
        };
        assert!(facts(&event).is_err());
    }

    #[test]
    fn the_connection_string_never_interpolates_a_raw_credential() {
        let args = PostgresArgs {
            postgres_password: Some("pa'ss\\word".to_owned()),
            ..PostgresArgs::default()
        };

        let connection_string = args.connection_string().expect("built connection string");
        assert!(connection_string.contains(r"password='pa\'ss\\word'"));
        assert!(connection_string.contains("application_name='bip300-monitor'"));
    }

    #[test]
    fn a_password_and_a_password_file_cannot_both_be_set() {
        let args = PostgresArgs {
            postgres_password: Some("secret".to_owned()),
            postgres_password_file: Some(PathBuf::from("/run/secrets/postgres-password")),
            ..PostgresArgs::default()
        };

        let error = args
            .connection_string()
            .expect_err("two credentials must be rejected");
        assert!(error.to_string().contains("only one of"));
    }

    #[test]
    fn a_missing_password_file_fails_rather_than_connecting_without_one() {
        let args = PostgresArgs {
            postgres_password_file: Some(PathBuf::from("/nonexistent/postgres-password")),
            ..PostgresArgs::default()
        };

        assert!(args.connection_string().is_err());
    }

    #[test]
    fn escaping_survives_quotes_and_backslashes() {
        assert_eq!(escape("plain"), "'plain'");
        assert_eq!(escape(r"back\slash"), r"'back\\slash'");
        assert_eq!(escape("quo'te"), r"'quo\'te'");
    }
}
