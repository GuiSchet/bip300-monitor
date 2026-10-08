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
/// Each version is applied in one transaction together with its
/// `schema_version` row, so a version is applied exactly once even across a
/// crash; the statements themselves need not be idempotent.
const MIGRATIONS: &[&str] = &[
    include_str!("../schema/0001_event.sql"),
    include_str!("../schema/0002_event_identity_nulls.sql"),
    include_str!("../schema/0003_history_coverage.sql"),
    include_str!("../schema/0004_observation_provenance.sql"),
    include_str!("../schema/0005_event_fact_identity.sql"),
    include_str!("../schema/0006_worker_health_and_observation_order.sql"),
    include_str!("../schema/0007_single_active_run.sql"),
    include_str!("../schema/0008_observation_quality.sql"),
    include_str!("../schema/0009_official_sources.sql"),
    include_str!("../schema/0010_official_contract_9.sql"),
];

/// Advisory-lock key that serializes the migration of one record.
///
/// The check-then-apply below is two statements, so two processes starting at
/// once would both see a version as unapplied and both run it. `ADD CONSTRAINT`
/// is not idempotent, so the loser would fail its startup for no real reason.
const MIGRATION_LOCK_KEY: i64 = 0x6231_3330_305f_6d6f;

/// Advisory-lock key taken first by every transaction that writes the record.
///
/// The enforcer and node workers write through separate connections. Identity
/// ids are allocated when a row is inserted, not when it commits, so two
/// concurrent writers could commit id N+1 before id N, and a consumer paging
/// by `id > cursor` would skip N forever. Holding this lock from before the
/// first insert until commit makes commit order equal id order.
const WRITE_ORDER_LOCK_KEY: i64 = 0x6231_3330_305f_7772;

/// Open a record write that commits in identity-id order (see
/// [`WRITE_ORDER_LOCK_KEY`]).
async fn write_tx(client: &mut Client) -> Result<tokio_postgres::Transaction<'_>> {
    let transaction = client
        .transaction()
        .await
        .context("opening a record write")?;
    transaction
        .execute("SELECT pg_advisory_xact_lock($1)", &[&WRITE_ORDER_LOCK_KEY])
        .await
        .context("ordering the record write")?;
    Ok(transaction)
}

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
    if let Some(MonitorEvent::Node(node)) = event.monitor_event.as_ref() {
        let payload = node.event.as_ref().context("missing node event")?;
        let anchor = event.observed_at_block.as_ref();
        return Ok(Facts {
            kind: payload.kind(),
            sidechain: None,
            block_hash: anchor.map(|a| a.hash.as_slice()),
            height: anchor
                .and_then(|a| a.height)
                .map(i32::try_from)
                .transpose()?,
        });
    }

    let payload = match event.monitor_event.as_ref() {
        Some(MonitorEvent::Enforcer(payload)) => payload,
        _ => bail!("event envelope does not contain a monitor event"),
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
    /// Last tip this source recorded before the current run, if any. A new
    /// subscription cannot replay what happened since: it starts a gap.
    previous_run_tip: Option<ObservedBlock>,
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
                "extractor_status",
                "per_worker_health"
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

/// Independently supervised workers whose health must not overwrite another
/// worker's status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtractorWorker {
    NodeHistory,
    MainchainTip,
    BmmRequests,
    MainchainEvents,
    ConfirmedBmmFees,
    EnforcerState,
    /// Per-slot block history; reported only when a scope is quarantined.
    BlockHistory,
}

impl ExtractorWorker {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NodeHistory => "node_history",
            Self::MainchainTip => "mainchain_tip",
            Self::BmmRequests => "bmm_requests",
            Self::MainchainEvents => "mainchain_events",
            Self::ConfirmedBmmFees => "confirmed_bmm_fees",
            Self::EnforcerState => "enforcer_state",
            Self::BlockHistory => "block_history",
        }
    }
}

/// Whether the mainchain tip stayed fixed around a grouped unary snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotConsistency {
    Stable, // Archived contract 7 only.
    TipMatched,
    Unknown,
    Changed,
}

impl SnapshotConsistency {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::TipMatched => "tip_matched",
            Self::Unknown => "unknown",
            Self::Changed => "changed",
        }
    }
}

/// Provenance shared by every event captured in one unary snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotMetadata {
    pub revision_before: Option<String>,
    pub revision_after: Option<String>,
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
    let description_sha256d = crate::bip300::sidechain_description_hash(&sidechain.raw_description)
        .context("calculating the active sidechain BIP300 description hash")?;
    if sidechain.description_hash != description_sha256d {
        bail!(
            "active sidechain slot {slot} reports description hash {}, calculated {}",
            hex::encode(&sidechain.description_hash),
            hex::encode(&description_sha256d)
        );
    }
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
    /// Last tip this source recorded before the current run started.
    pub const fn previous_run_tip(&self) -> Option<&ObservedBlock> {
        self.previous_run_tip.as_ref()
    }

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
            previous_run_tip: None,
        };
        // Since contract 8 every contract version owns a fresh dataset: facts of
        // different versions are never mixed under one dataset identity.
        if manifest.event_contract_version >= 8 {
            let version = i32::try_from(manifest.event_contract_version)
                .context("event contract version does not fit in an i32")?;
            let client = store.client.lock().await;
            let exists: bool = client
                .query_one("SELECT to_regclass('dataset_manifest') IS NOT NULL", &[])
                .await?
                .get(0);
            if exists {
                let incompatible: bool = client.query_one(
                    "SELECT EXISTS(SELECT 1 FROM dataset_manifest WHERE network_id=$1 AND activation_height=$2
                        AND activation_block_hash=$3 AND initial_event_contract_version<>$4)",
                    &[&manifest.network_id,&height_to_i32(manifest.activation_height)?,&manifest.activation_block_hash,&version],
                ).await?.get(0);
                if incompatible {
                    bail!(
                        "event contract v{version} requires a fresh v{version} dataset; no migrations were applied"
                    );
                }
            }
        }
        store.migrate().await?;
        let (dataset_id, run_id, previous_run_tip) = store.initialize_identity(&manifest).await?;
        Ok(Self {
            dataset_id,
            run_id,
            previous_run_tip,
            event_contract_version: i32::try_from(manifest.event_contract_version)
                .context("event contract version does not fit in an i32")?,
            ..store
        })
    }

    async fn initialize_identity(
        &self,
        manifest: &DatasetManifest,
    ) -> Result<(String, String, Option<ObservedBlock>)> {
        let activation_height = height_to_i32(manifest.activation_height)?;
        let event_contract_version = i32::try_from(manifest.event_contract_version)
            .context("event contract version does not fit in an i32")?;
        let mut client = self.client.lock().await;
        let transaction = write_tx(&mut client).await?;
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
        if manifest.event_contract_version >= 8 {
            let incompatible: bool = transaction
                .query_one(
                    "SELECT initial_event_contract_version <> $2
                       FROM dataset_manifest
                      WHERE dataset_id = $1::text::uuid",
                    &[&dataset_id, &event_contract_version],
                )
                .await
                .context("checking the dataset contract compatibility boundary")?
                .get(0);
            if incompatible {
                bail!(
                    "event contract v{event_contract_version} requires a fresh v{event_contract_version} dataset; create a recoverable record backup and start an empty dataset"
                );
            }
        }
        transaction
            .execute(
                "UPDATE extractor_run
                    SET status = 'failed',
                        finished_at = now(),
                        finish_reason = 'superseded by extractor startup after unclean termination'
                  WHERE dataset_id = $1::text::uuid
                    AND source = $2
                    AND status = 'running'",
                &[&dataset_id, &self.source],
            )
            .await
            .context("closing an orphaned extractor run")?;
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
        // A new subscription starts at the current tip: it cannot replay live
        // transitions missed between processes, even after a clean shutdown.
        if self.source == "enforcer" && manifest.event_contract_version >= 8 {
            transaction
                .execute(
                    "INSERT INTO observation_failure(dataset_id, run_id, worker, error)
                     SELECT $1::text::uuid, $2::text::uuid, 'mainchain_events',
                            'subscription restarted after run ' || run_id::text ||
                            '; offline transitions are unknown (no replay continuity) until the'
                            ' next mainchain_transition subscription boundary'
                       FROM extractor_run
                      WHERE dataset_id = $1::text::uuid AND source = $3
                        AND run_id <> $2::text::uuid
                      ORDER BY started_at DESC, run_id DESC LIMIT 1",
                    &[&dataset_id, &run_id, &self.source],
                )
                .await
                .context("recording the subscription restart gap")?;
        }
        // Read before the status row is claimed by the new run.
        let previous_run_tip = transaction
            .query_opt(
                "SELECT last_tip_hash, last_tip_height FROM extractor_status
                  WHERE dataset_id = $1::text::uuid AND source = $2
                    AND last_tip_hash IS NOT NULL AND last_tip_height IS NOT NULL",
                &[&dataset_id, &self.source],
            )
            .await
            .context("reading the previous run's last tip")?
            .map(|row| -> Result<ObservedBlock> {
                Ok(ObservedBlock::at_height(
                    row.get(0),
                    u32::try_from(row.get::<_, i32>(1)).context("negative previous tip height")?,
                ))
            })
            .transpose()?;
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
        Ok((dataset_id, run_id, previous_run_tip))
    }

    async fn migrate(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        // Held for the whole migration, and released by the session ending even
        // if this returns early: two extractors starting at once must not both
        // decide a version is unapplied.
        client
            .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await
            .context("taking the record migration lock")?;
        let result = Self::apply_migrations(&mut client).await;
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

    async fn apply_migrations(client: &mut Client) -> Result<()> {
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

            // The statements and their version commit together: a crash in
            // between would otherwise re-run DDL that is not idempotent.
            let transaction = client
                .transaction()
                .await
                .with_context(|| format!("opening schema version {version}"))?;
            transaction
                .batch_execute(statements)
                .await
                .with_context(|| format!("applying schema version {version}"))?;
            transaction
                .execute(
                    "INSERT INTO schema_version (version) VALUES ($1)
                     ON CONFLICT (version) DO NOTHING",
                    &[&version],
                )
                .await
                .with_context(|| format!("recording schema version {version}"))?;
            transaction
                .commit()
                .await
                .with_context(|| format!("committing schema version {version}"))?;
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

        let queued_at = std::time::Instant::now();
        let mut client = self.client.lock().await;
        let writer_wait_ms = queued_at.elapsed().as_millis() as u64;
        let transaction_started = std::time::Instant::now();
        let transaction = write_tx(&mut client).await?;
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

        tracing::info!(
            writer_wait_ms,
            transaction_ms = transaction_started.elapsed().as_millis() as u64,
            events = events.len(),
            inserted,
            "record transaction committed"
        );
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
        if metadata.consistency == SnapshotConsistency::TipMatched
            && (metadata.tip_before != metadata.tip_after
                || metadata.revision_before.is_some()
                || metadata.revision_after.is_some())
        {
            bail!("tip-matched snapshots require matching tips and no claimed server revision");
        }
        if self.event_contract_version >= 8 && metadata.consistency == SnapshotConsistency::Stable {
            bail!("official API cannot establish atomic stable snapshots");
        }
        if metadata.consistency == SnapshotConsistency::Stable
            && (metadata.tip_before != metadata.tip_after
                || metadata
                    .revision_before
                    .as_ref()
                    .is_none_or(String::is_empty)
                || metadata.revision_before != metadata.revision_after)
        {
            bail!("stable snapshot requires one tip and one explicit chain revision");
        }
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
        let transaction = write_tx(&mut client).await?;
        let snapshot_group_id: String = transaction
            .query_one(
                "INSERT INTO snapshot_group
                    (dataset_id, run_id, capture_method, started_at, finished_at,
                     tip_before_hash, tip_before_height, tip_after_hash,
                     tip_after_height, consistency, attempts, revision_before, revision_after)
                 VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
                    &metadata.revision_before,
                    &metadata.revision_after,
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
        let transaction = write_tx(&mut client).await?;
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

    /// Create the durable rows for independently supervised workers.
    pub async fn initialize_worker_statuses(&self, workers: &[ExtractorWorker]) -> Result<()> {
        let mut client = self.client.lock().await;
        let transaction = write_tx(&mut client).await?;
        for worker in workers {
            transaction
                .execute(
                    "INSERT INTO extractor_worker_status (run_id, worker)
                     VALUES ($1::text::uuid, $2)
                     ON CONFLICT (run_id, worker) DO NOTHING",
                    &[&self.run_id, &worker.as_str()],
                )
                .await
                .with_context(|| format!("initializing {} worker status", worker.as_str()))?;
        }
        recompute_extractor_error(&transaction, &self.dataset_id, self.source, &self.run_id)
            .await?;
        transaction
            .commit()
            .await
            .context("committing worker-status initialization")?;
        Ok(())
    }

    /// Record one worker failure and expose it after the configured threshold.
    /// Returns the durable consecutive-failure count.
    pub async fn record_worker_failure(
        &self,
        worker: ExtractorWorker,
        error: &str,
        degraded_after: u32,
    ) -> Result<u32> {
        if degraded_after == 0 {
            bail!("worker degradation threshold must be positive");
        }
        let threshold = i32::try_from(degraded_after)
            .context("worker degradation threshold does not fit in an i32")?;
        let mut client = self.client.lock().await;
        let transaction = write_tx(&mut client).await?;
        let row = transaction
            .query_opt(
                "UPDATE extractor_worker_status
                    SET consecutive_failures = consecutive_failures + 1,
                        last_error = CASE
                            WHEN consecutive_failures + 1 >= $3 THEN $4
                            ELSE NULL
                        END,
                        last_failure_at = now(),
                        updated_at = now()
                  WHERE run_id = $1::text::uuid AND worker = $2
                  RETURNING consecutive_failures",
                &[&self.run_id, &worker.as_str(), &threshold, &error],
            )
            .await
            .with_context(|| format!("recording {} worker failure", worker.as_str()))?
            .with_context(|| format!("{} worker status was not initialized", worker.as_str()))?;
        transaction
            .execute(
                "INSERT INTO observation_failure(dataset_id,run_id,worker,error)
             VALUES($1::text::uuid,$2::text::uuid,$3,$4)",
                &[&self.dataset_id, &self.run_id, &worker.as_str(), &error],
            )
            .await
            .context("recording observation failure evidence")?;
        let failures: i32 = row.get(0);
        recompute_extractor_error(&transaction, &self.dataset_id, self.source, &self.run_id)
            .await?;
        transaction
            .commit()
            .await
            .context("committing the worker failure")?;
        u32::try_from(failures).context("worker failure count is negative")
    }

    /// Mark one worker healthy without touching any other worker's error.
    pub async fn record_worker_success(&self, worker: ExtractorWorker) -> Result<()> {
        let mut client = self.client.lock().await;
        let transaction = write_tx(&mut client).await?;
        let updated = transaction
            .execute(
                "UPDATE extractor_worker_status
                    SET consecutive_failures = 0,
                        last_error = NULL,
                        last_success_at = now(),
                        updated_at = now()
                  WHERE run_id = $1::text::uuid AND worker = $2",
                &[&self.run_id, &worker.as_str()],
            )
            .await
            .with_context(|| format!("recording {} worker success", worker.as_str()))?;
        if updated != 1 {
            bail!("{} worker status was not initialized", worker.as_str());
        }
        recompute_extractor_error(&transaction, &self.dataset_id, self.source, &self.run_id)
            .await?;
        transaction
            .commit()
            .await
            .context("committing the worker success")?;
        Ok(())
    }

    /// Close a run cleanly; a row left `running` documents an unclean exit.
    pub async fn finish_run(&self, status: &str, reason: Option<&str>) -> Result<()> {
        if !matches!(status, "completed" | "failed") {
            bail!("invalid terminal extractor run status `{status}`");
        }
        let mut client = self.client.lock().await;
        let transaction = write_tx(&mut client).await?;
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

    /// New facts have priority over daily retries of unavailable historical fees.
    pub async fn next_fee_block(&self) -> Result<Option<(i64, ObservedBlock)>> {
        if self.source != "node" {
            bail!("confirmed BMM fees are node enrichments");
        }
        let client = self.client.lock().await;
        let row = client.query_opt(
            "WITH next AS (SELECT id,block_hash,height FROM event
              WHERE dataset_id=$1::text::uuid AND event_contract_version=$2 AND source=$3 AND kind='mainchain_block'
                AND id>COALESCE((SELECT max(source_event_id) FROM bmm_fee_job
                    WHERE dataset_id=$1::text::uuid AND event_contract_version=$2 AND source=$3),0)
              ORDER BY id LIMIT 1), retry AS (
                SELECT e.id,e.block_hash,e.height FROM bmm_fee_job j JOIN event e ON e.id=j.source_event_id
                 WHERE j.dataset_id=$1::text::uuid AND j.event_contract_version=$2 AND j.source=$3
                   AND j.status='pending' AND j.next_retry_at<=now()
                 ORDER BY j.next_retry_at LIMIT 1)
             SELECT * FROM next UNION ALL SELECT * FROM retry WHERE NOT EXISTS(SELECT 1 FROM next)",
            &[&self.dataset_id,&self.event_contract_version,&self.source],
        ).await?;
        row.map(|r| {
            Ok((
                r.get(0),
                ObservedBlock::at_height(r.get(1), u32::try_from(r.get::<_, i32>(2))?),
            ))
        })
        .transpose()
    }

    pub async fn record_fee_enrichment(&self, source_event_id: i64, event: &Event) -> Result<()> {
        let Some(MonitorEvent::Node(node)) = &event.monitor_event else {
            bail!("confirmed BMM fees are node enrichments");
        };
        self.record_node_fees(source_event_id, event, node).await
    }

    /// Only bids observed through the official API and a matching official
    /// commitment are candidates. Inclusion/transaction fees are checked by the
    /// node worker; missing observations never become a complete BMM history.
    pub async fn observed_bmm_candidates(
        &self,
        block_hash: &[u8],
        parent: &[u8],
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let client = self.client.lock().await;
        let rows=client.query("SELECT DISTINCT (r->>'sidechain_number')::integer,decode(r->>'txid','hex')
            FROM event e CROSS JOIN LATERAL jsonb_array_elements(e.payload #> '{monitor_event,Enforcer,event,BmmRequests,requests}') r
            WHERE e.dataset_id=$1::text::uuid AND e.event_contract_version=$2 AND e.source='enforcer'
            AND e.kind='bmm_requests' AND e.payload #>> '{monitor_event,Enforcer,event,BmmRequests,previous_mainchain_block_hash}'=encode($3::bytea,'hex')
            AND EXISTS(SELECT 1 FROM event_observation o JOIN snapshot_group g USING(snapshot_group_id)
                WHERE o.event_id=e.id AND g.dataset_id=e.dataset_id AND g.consistency='tip_matched')
            AND EXISTS(SELECT 1 FROM event b WHERE b.dataset_id=e.dataset_id AND b.event_contract_version=e.event_contract_version
                AND b.source='enforcer' AND b.kind='block_connected' AND b.block_hash=$4
                AND b.sidechain=(r->>'sidechain_number')::smallint
                AND b.payload #>> '{monitor_event,Enforcer,event,BlockConnected,bmm_commitment}'=r->>'critical_hash'
                AND NOT EXISTS(SELECT 1 FROM event_conflict c WHERE c.dataset_id=b.dataset_id AND (c.first_event_id=b.id OR c.conflicting_event_id=b.id)))",
            &[&self.dataset_id,&self.event_contract_version,&parent,&block_hash]).await?;
        rows.into_iter()
            .map(|r| Ok((u32::try_from(r.get::<_, i32>(0))?, r.get(1))))
            .collect()
    }

    async fn record_node_fees(
        &self,
        source_id: i64,
        event: &Event,
        node: &crate::protobuf::event::NodeEvent,
    ) -> Result<()> {
        use crate::protobuf::event::node_event;
        let Some(node_event::Event::ConfirmedBmmFees(fees)) = &node.event else {
            bail!("expected node fee enrichment");
        };
        let header = fees.header.as_ref().context("missing fee header")?;
        let candidates = self
            .observed_bmm_candidates(&header.hash, &header.previous_hash)
            .await?;
        let mut identities = std::collections::BTreeSet::new();
        for fee in &fees.fees {
            if !candidates.contains(&(fee.sidechain_number, fee.txid.clone()))
                || !identities.insert((fee.sidechain_number, fee.txid.clone()))
                || fee.fee_sats.is_some() != fee.unavailable_reason.is_empty()
            {
                bail!("fee has no unambiguous official evidence");
            }
        }
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let row=tx.query_opt("SELECT envelope FROM event WHERE id=$1 AND dataset_id=$2::text::uuid
            AND event_contract_version=$3 AND source='node' AND kind='mainchain_block' AND block_hash=$4",
            &[&source_id,&self.dataset_id,&self.event_contract_version,&header.hash]).await?.context("missing node source block")?;
        let bytes: Vec<u8> = row.get(0);
        let original = Event::decode(bytes.as_slice())?;
        let Some(MonitorEvent::Node(original)) = original.monitor_event else {
            bail!("invalid node block envelope");
        };
        let Some(node_event::Event::MainchainBlock(block)) = original.event else {
            bail!("invalid node block kind");
        };
        if block.header.as_ref() != Some(header) {
            bail!("fee enrichment changed the block header");
        }
        let block: bitcoin::Block = bitcoin::consensus::deserialize(&block.raw_block)?;
        for fee in &fees.fees {
            if !block
                .txdata
                .iter()
                .any(|t| t.compute_txid().to_string() == hex::encode(&fee.txid))
            {
                bail!("fee transaction is not in the block");
            }
        }
        let sequence = reserve_capture_sequences(&tx, &self.run_id, 1).await?;
        insert(
            &tx,
            self.source,
            &self.dataset_id,
            &self.run_id,
            self.event_contract_version,
            CaptureMethod::Backfill,
            None,
            None,
            None,
            sequence,
            event,
            &mut BTreeMap::new(),
        )
        .await?;
        // The official slot backfill can arrive after the node block, so the
        // candidates are final only once every active slot's block fact exists.
        let final_candidates: bool = tx
            .query_one(
                "SELECT NOT EXISTS(SELECT 1 FROM current_sidechain_instance i
                JOIN sidechain_instance s USING(dataset_id, sidechain_instance_id)
                WHERE i.dataset_id=$1::text::uuid AND s.activation_height<=$4
                  AND NOT EXISTS(SELECT 1 FROM event b
                    WHERE b.dataset_id=i.dataset_id AND b.event_contract_version=$2
                      AND b.source='enforcer' AND b.kind='block_connected'
                      AND b.sidechain=i.sidechain AND b.block_hash=$3))",
                &[
                    &self.dataset_id,
                    &self.event_contract_version,
                    &header.hash,
                    &height_to_i32(header.height)?,
                ],
            )
            .await?
            .get(0);
        schedule_fee_job(&tx, self, source_id, final_candidates, None).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Record that enriching one block failed; it is retried with backoff and
    /// abandoned after a bounded number of attempts.
    pub async fn record_fee_failure(&self, source_event_id: i64, error: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        schedule_fee_job(&tx, self, source_event_id, false, Some(error)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Only certified prefixes can terminate a repair; a live fact cannot.
    pub async fn certified_history_floor(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        instance: Option<&str>,
        hashes: &[Vec<u8>],
    ) -> Result<Option<ObservedBlock>> {
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT block_hash,height FROM history_certified_block
             WHERE dataset_id=$1::text::uuid AND event_contract_version=$2 AND source=$3
               AND stream=$4 AND sidechain IS NOT DISTINCT FROM $5
               AND sidechain_instance_id IS NOT DISTINCT FROM $6
               AND block_hash=ANY($7) ORDER BY height DESC LIMIT 1",
                &[
                    &self.dataset_id,
                    &self.event_contract_version,
                    &self.source,
                    &stream,
                    &sidechain,
                    &instance,
                    &hashes,
                ],
            )
            .await
            .context("finding a certified history prefix")?;
        row.map(|r| {
            Ok(ObservedBlock::at_height(
                r.get(0),
                u32::try_from(r.get::<_, i32>(1))?,
            ))
        })
        .transpose()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_history_floor(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        instance: Option<&str>,
        cursor: &ObservedBlock,
        floor_hash: Option<&[u8]>,
        floor_height: Option<u32>,
    ) -> Result<()> {
        let sidechain = sidechain.map(i16::from);
        let height = floor_height.map(height_to_i32).transpose()?;
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let rows = tx.execute(
            "UPDATE history_coverage SET floor_hash=$8,floor_height=$9,updated_at=now()
              WHERE dataset_id=$1::text::uuid AND event_contract_version=$2 AND source=$3
                AND stream=$4 AND sidechain IS NOT DISTINCT FROM $5
                AND sidechain_instance_id IS NOT DISTINCT FROM $6 AND next_hash=$7 AND status='running'",
            &[&self.dataset_id,&self.event_contract_version,&self.source,&stream,&sidechain,&instance,&cursor.hash,&floor_hash,&height],
        ).await?;
        if rows != 1 {
            bail!("history cursor changed while setting the certified floor");
        }
        tx.commit().await?;
        Ok(())
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

        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let row = tx
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
                    covered_tip_hash = COALESCE(EXCLUDED.covered_tip_hash, history_coverage.covered_tip_hash),
                    covered_tip_height = COALESCE(EXCLUDED.covered_tip_height, history_coverage.covered_tip_height),
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

        tx.commit().await?;
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
        let (mut next_hash, mut next_height) = optional_block_parts(page.next)?;

        let queued_at = std::time::Instant::now();
        let mut client = self.client.lock().await;
        let writer_wait_ms = queued_at.elapsed().as_millis() as u64;
        let transaction_started = std::time::Instant::now();
        let transaction = write_tx(&mut client).await?;
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
        // Only this page's blocks: an old conflict elsewhere in the scope is
        // already recorded and must not fail every later page.
        let page_hashes = events
            .iter()
            .filter_map(|event| event.observed_at_block.as_ref().map(|b| b.hash.clone()))
            .collect::<Vec<_>>();
        let conflict: bool = transaction.query_one(
            "SELECT EXISTS(SELECT 1 FROM event_conflict c JOIN event e ON e.id=c.first_event_id
                WHERE e.dataset_id=$1::text::uuid AND e.event_contract_version=$2 AND e.source=$3
                  AND e.kind=$4 AND e.block_hash=ANY($7)
                  AND e.sidechain IS NOT DISTINCT FROM $5 AND e.sidechain_instance_id IS NOT DISTINCT FROM $6)",
            &[&self.dataset_id,&self.event_contract_version,&self.source,&history_kind(page.stream)?,&sidechain,&page.sidechain_instance_id,&page_hashes],
        ).await?.get(0);
        // Completing certifies the whole scope, so the final page also checks
        // conflicts retained anywhere in it (an operator may have resumed a
        // quarantined cursor without resolving them).
        let conflict = conflict
            || (page.next.is_none()
                && transaction.query_one(
                    "SELECT EXISTS(SELECT 1 FROM event_conflict c JOIN event e ON e.id=c.first_event_id
                        WHERE e.dataset_id=$1::text::uuid AND e.event_contract_version=$2 AND e.source=$3
                          AND e.kind=$4
                          AND e.sidechain IS NOT DISTINCT FROM $5 AND e.sidechain_instance_id IS NOT DISTINCT FROM $6)",
                    &[&self.dataset_id,&self.event_contract_version,&self.source,&history_kind(page.stream)?,&sidechain,&page.sidechain_instance_id],
                ).await?.get::<_, bool>(0));
        let complete = page.next.is_none() && !conflict;
        if conflict {
            (next_hash, next_height) = optional_block_parts(Some(page.expected_next))?;
        }
        let updated = transaction
            .execute(
                "UPDATE history_coverage
                    SET next_hash = $7,
                        next_height = $8,
                        status = CASE WHEN $13 THEN 'error' WHEN $9 THEN 'complete' ELSE 'running' END,
                        covered_tip_hash = CASE WHEN $9 THEN target_tip_hash ELSE covered_tip_hash END,
                        covered_tip_height = CASE WHEN $9 THEN target_tip_height ELSE covered_tip_height END,
                        rows_recorded = rows_recorded + $10,
                        last_error = CASE WHEN $13 THEN $14 ELSE NULL END,
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
                    &conflict,
                    &HISTORY_CONFLICT,
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
        if conflict {
            return Err(HistoryConflict.into());
        }
        tracing::info!(
            writer_wait_ms,
            transaction_ms = transaction_started.elapsed().as_millis() as u64,
            events = events.len(),
            inserted,
            "record transaction committed"
        );
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
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let updated = tx
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
        tx.commit().await?;
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
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let updated = tx
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
        tx.commit().await?;
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
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        let row = tx
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
        tx.commit().await?;
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
        let mut client = self.client.lock().await;
        let tx = write_tx(&mut client).await?;
        tx.execute(
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
        tx.commit().await?;
        Ok(())
    }

    /// Height of the newest recorded event of one kind for one sidechain slot.
    ///
    /// Diagnostic maximum only. Live delivery may leave older holes; the
    /// resumable backfill checkpoint is history_coverage.
    #[cfg(feature = "postgres_integration_tests")]
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

/// `last_error` of a history scope suspended by conflicting immutable facts.
pub const HISTORY_CONFLICT: &str = "conflicting immutable block facts";

/// A history page retained conflicting immutable block facts. The scope is
/// suspended (status `error`, [`HISTORY_CONFLICT`]) until an operator resolves
/// it; retrying the same cursor can only find the same conflict again.
#[derive(Debug)]
pub struct HistoryConflict;

impl std::fmt::Display for HistoryConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("conflicting immutable block facts retained; history certification suspended")
    }
}

impl std::error::Error for HistoryConflict {}

/// Attempts after which an incomplete fee enrichment is abandoned.
const FEE_JOB_MAX_ATTEMPTS: i32 = 10;

/// Close a fee job, or schedule its next attempt with exponential backoff
/// (one hour, doubling, at most a day).
async fn schedule_fee_job(
    tx: &tokio_postgres::Transaction<'_>,
    store: &Store,
    source_event_id: i64,
    done: bool,
    error: Option<&str>,
) -> Result<()> {
    tx.execute(
        "INSERT INTO bmm_fee_job AS j(dataset_id,event_contract_version,source,source_event_id,
                attempts,status,next_retry_at,last_error)
         VALUES($1::text::uuid,$2,$3,$4,1,
                CASE WHEN $5 THEN 'done' ELSE 'pending' END,
                CASE WHEN $5 THEN NULL ELSE now()+interval '1 hour' END,$6)
         ON CONFLICT(dataset_id,event_contract_version,source,source_event_id) DO UPDATE SET
            last_observed_at=now(),
            attempts=j.attempts+1,
            last_error=excluded.last_error,
            status=CASE WHEN $5 THEN 'done' WHEN j.attempts+1>=$7 THEN 'abandoned' ELSE 'pending' END,
            next_retry_at=CASE WHEN $5 OR j.attempts+1>=$7 THEN NULL
                ELSE now()+LEAST(interval '24 hours', interval '1 hour'*power(2,j.attempts)) END",
        &[
            &store.dataset_id,
            &store.event_contract_version,
            &store.source,
            &source_event_id,
            &done,
            &error,
            &FEE_JOB_MAX_ATTEMPTS,
        ],
    )
    .await
    .context("scheduling a confirmed-fee job")?;
    Ok(())
}

/// Event kind whose facts make up a history coverage stream.
fn history_kind(stream: &str) -> Result<&'static str> {
    match stream {
        "block" => Ok("block_connected"),
        "mainchain_block" => Ok("mainchain_block"),
        other => bail!("unknown history stream `{other}`"),
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
    let previous_hash = match event.monitor_event.as_ref() {
        Some(MonitorEvent::Node(node)) => node
            .event
            .as_ref()
            .and_then(|p| p.header())
            .map(|h| &h.previous_hash),
        Some(MonitorEvent::Enforcer(payload)) => match payload.event.as_ref() {
            Some(crate::protobuf::enforcer_extractor::enforcer_event::Event::BlockConnected(b)) => {
                b.header.as_ref().map(|h| &h.previous_hash)
            }
            Some(
                crate::protobuf::enforcer_extractor::enforcer_event::Event::MainchainTransition(t),
            ) => t.header.as_ref().map(|h| &h.previous_hash),
            _ => None,
        },
        _ => None,
    };
    let payload = json::render(event).context("rendering an event payload as JSON")?;
    let envelope = event.encode_to_vec();
    let envelope_sha256 = Sha256::digest(&envelope).to_vec();
    let fact_sha256 = match event.monitor_event.as_ref() {
        Some(MonitorEvent::Enforcer(payload)) => Sha256::digest(payload.encode_to_vec()).to_vec(),
        Some(MonitorEvent::Node(payload)) => Sha256::digest(payload.encode_to_vec()).to_vec(),
        None => bail!("event envelope does not contain a monitor event"),
    };
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_millis(event.timestamp);
    let authoritative = match snapshot_group_id {
        Some(group) => transaction.query_one(
            "SELECT consistency IN ('stable', 'tip_matched') FROM snapshot_group WHERE snapshot_group_id = $1::text::uuid",
            &[&group],
        ).await?.get::<_, bool>(0),
        None => true,
    };
    update_sidechain_instances(
        transaction,
        dataset_id,
        event,
        observed_at,
        instance_cache,
        authoritative,
    )
    .await?;
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
                      sidechain_instance_id, event_contract_version, fact_sha256, previous_hash)
                 VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $9,
                         $10, $11, $12, $17, $18)
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
                    AND fact_sha256 = $17
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
                &fact_sha256,
                &previous_hash,
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
    authoritative: bool,
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

    // Instances are recorded from every reading. Only an authoritative one may
    // change which instance is current, or tag other events of its batch.
    let mut current = Vec::with_capacity(snapshot.sidechains.len());
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
        current.push((slot, instance.sidechain_instance_id));
    }
    if !authoritative {
        return Ok(());
    }
    let slots = current.iter().map(|(slot, _)| *slot).collect::<Vec<_>>();
    let instances = current.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();
    // Change only the rows that differ, so an unchanged reading rewrites
    // nothing and `observed_at` keeps meaning "current since".
    transaction
        .execute(
            "DELETE FROM current_sidechain_instance c
              WHERE c.dataset_id = $1::text::uuid
                AND NOT EXISTS (SELECT 1 FROM unnest($2::smallint[], $3::text[]) n(sidechain, id)
                                 WHERE n.sidechain = c.sidechain AND n.id = c.sidechain_instance_id)",
            &[&dataset_id, &slots, &instances],
        )
        .await
        .context("retiring replaced current sidechain instances")?;
    transaction
        .execute(
            "INSERT INTO current_sidechain_instance
                (dataset_id, sidechain, sidechain_instance_id, observed_at)
             SELECT $1::text::uuid, n.sidechain, n.id, $4
               FROM unnest($2::smallint[], $3::text[]) n(sidechain, id)
             ON CONFLICT (dataset_id, sidechain) DO NOTHING",
            &[&dataset_id, &slots, &instances, &observed_at],
        )
        .await
        .context("recording the current sidechain instances")?;
    instance_cache.clear();
    instance_cache.extend(current);
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
                    updated_at = now()
              WHERE dataset_id = $1::text::uuid AND source = $2 AND run_id = $3::text::uuid",
            &[&dataset_id, &source, &run_id, &tip.hash, &height],
        )
        .await
        .context("updating extractor status")?;
    Ok(())
}

async fn recompute_extractor_error(
    transaction: &Transaction<'_>,
    dataset_id: &str,
    source: &str,
    run_id: &str,
) -> Result<()> {
    transaction
        .execute(
            "UPDATE extractor_status
                SET last_error = (
                        SELECT string_agg(worker || ': ' || last_error, '; ' ORDER BY worker)
                          FROM extractor_worker_status
                         WHERE run_id = $3::text::uuid AND last_error IS NOT NULL
                    ),
                    updated_at = now()
              WHERE dataset_id = $1::text::uuid AND source = $2
                AND run_id = $3::text::uuid",
            &[&dataset_id, &source, &run_id],
        )
        .await
        .context("recomputing aggregate extractor error")?;
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
