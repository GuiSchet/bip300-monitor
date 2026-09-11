//! The authoritative Postgres record of observed monitor events.
//!
//! The `envelope` column holds the raw protobuf bytes, so the record is also
//! the reversibility guarantee: any other store can be rebuilt from
//! `SELECT envelope FROM event ORDER BY id`. The `payload` column is the same
//! event decoded, and exists only so the record can be queried.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use clap::Args as ClapArgs;
use prost::Message as _;
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
}

/// Durable state of one bounded historical import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryCoverage {
    pub stream: String,
    pub sidechain: Option<u8>,
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
}

impl HistoryStatus {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            other => bail!("record contains unknown history status `{other}`"),
        }
    }
}

/// New cursor values committed together with one historical event page.
pub struct HistoryPage<'a> {
    pub stream: &'a str,
    pub sidechain: Option<u8>,
    pub expected_next: &'a ObservedBlock,
    pub next: Option<&'a ObservedBlock>,
}

impl Store {
    /// Connect to the record and apply any outstanding schema statements.
    ///
    /// `source` names the extractor writing the rows.
    pub async fn connect(args: &PostgresArgs, source: &'static str) -> Result<Self> {
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
        };
        store.migrate().await?;
        Ok(store)
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
        if events.is_empty() {
            return Ok(0);
        }

        let mut client = self.client.lock().await;
        let mut transaction = client
            .transaction()
            .await
            .context("opening a record transaction")?;
        let mut inserted = 0;
        for event in events {
            inserted += insert(&mut transaction, self.source, event).await?;
        }
        transaction
            .commit()
            .await
            .context("committing a record transaction")?;

        Ok(inserted)
    }

    /// Read the durable cursor for one historical stream.
    pub async fn history_coverage(
        &self,
        stream: &str,
        sidechain: Option<u8>,
    ) -> Result<Option<HistoryCoverage>> {
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT stream, sidechain, coverage_start_height,
                        covered_tip_hash, covered_tip_height,
                        target_tip_hash, target_tip_height,
                        floor_hash, floor_height, next_hash, next_height,
                        status, rows_recorded, effective_page_blocks, last_error
                   FROM history_coverage
                  WHERE source = $1 AND stream = $2
                    AND sidechain IS NOT DISTINCT FROM $3",
                &[&self.source, &stream, &sidechain],
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
        coverage_start_height: u32,
        covered_tip: Option<&ObservedBlock>,
        target_tip: &ObservedBlock,
        floor_hash: Option<&[u8]>,
        floor_height: Option<u32>,
        effective_page_blocks: u32,
    ) -> Result<HistoryCoverage> {
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
                    (source, stream, sidechain, coverage_start_height,
                     covered_tip_hash, covered_tip_height,
                     target_tip_hash, target_tip_height,
                     floor_hash, floor_height, next_hash, next_height,
                     status, effective_page_blocks, last_error,
                     started_at, updated_at, completed_at)
                 VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $7, $8,
                     'running', $11, NULL, now(), now(), NULL)
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
                 RETURNING stream, sidechain, coverage_start_height,
                           covered_tip_hash, covered_tip_height,
                           target_tip_hash, target_tip_height,
                           floor_hash, floor_height, next_hash, next_height,
                           status, rows_recorded, effective_page_blocks, last_error",
                &[
                    &self.source,
                    &stream,
                    &sidechain,
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
        let sidechain = page.sidechain.map(i16::from);
        let expected_height = required_height(page.expected_next, "history page cursor")?;
        let expected_height = height_to_i32(expected_height)?;
        let (next_hash, next_height) = optional_block_parts(page.next)?;

        let mut client = self.client.lock().await;
        let mut transaction = client
            .transaction()
            .await
            .context("opening a historical page transaction")?;
        let mut inserted = 0;
        for event in events {
            inserted += insert(&mut transaction, self.source, event).await?;
        }
        let inserted_i64 = i64::try_from(inserted).context("history page insert count overflow")?;
        let complete = page.next.is_none();
        let updated = transaction
            .execute(
                "UPDATE history_coverage
                    SET next_hash = $6,
                        next_height = $7,
                        status = CASE WHEN $8 THEN 'complete' ELSE 'running' END,
                        covered_tip_hash = CASE WHEN $8 THEN target_tip_hash ELSE covered_tip_hash END,
                        covered_tip_height = CASE WHEN $8 THEN target_tip_height ELSE covered_tip_height END,
                        rows_recorded = rows_recorded + $9,
                        last_error = NULL,
                        updated_at = now(),
                        completed_at = CASE WHEN $8 THEN now() ELSE NULL END
                  WHERE source = $1 AND stream = $2
                    AND sidechain IS NOT DISTINCT FROM $3
                    AND next_hash = $4 AND next_height = $5",
                &[
                    &self.source,
                    &page.stream,
                    &sidechain,
                    &page.expected_next.hash,
                    &expected_height,
                    &next_hash,
                    &next_height,
                    &complete,
                    &inserted_i64,
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
        effective_page_blocks: u32,
        error: &str,
    ) -> Result<()> {
        let sidechain = sidechain.map(i16::from);
        let page_blocks = height_to_i32(effective_page_blocks)?;
        let client = self.client.lock().await;
        let updated = client
            .execute(
                "UPDATE history_coverage
                    SET effective_page_blocks = $4, last_error = $5, updated_at = now()
                  WHERE source = $1 AND stream = $2
                    AND sidechain IS NOT DISTINCT FROM $3
                    AND status = 'running'",
                &[&self.source, &stream, &sidechain, &page_blocks, &error],
            )
            .await
            .with_context(|| format!("resizing {stream} history pages"))?;
        if updated != 1 {
            bail!("{stream} history was not running while resizing its pages");
        }
        Ok(())
    }

    /// Resume the exact cursor left by a previous fatal page failure.
    pub async fn resume_history(&self, stream: &str, sidechain: Option<u8>) -> Result<()> {
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let updated = client
            .execute(
                "UPDATE history_coverage
                    SET status = 'running', last_error = NULL, updated_at = now()
                  WHERE source = $1 AND stream = $2
                    AND sidechain IS NOT DISTINCT FROM $3
                    AND status = 'error' AND next_hash IS NOT NULL",
                &[&self.source, &stream, &sidechain],
            )
            .await
            .with_context(|| format!("resuming {stream} history"))?;
        if updated != 1 {
            bail!("{stream} history had no failed cursor to resume");
        }
        Ok(())
    }

    /// Mark a failed cursor as resumable without discarding it.
    pub async fn fail_history(
        &self,
        stream: &str,
        sidechain: Option<u8>,
        error: &str,
    ) -> Result<()> {
        let sidechain = sidechain.map(i16::from);
        let client = self.client.lock().await;
        let updated = client
            .execute(
                "UPDATE history_coverage
                    SET status = 'error', last_error = $4, updated_at = now()
                  WHERE source = $1 AND stream = $2
                    AND sidechain IS NOT DISTINCT FROM $3
                    AND status = 'running'",
                &[&self.source, &stream, &sidechain, &error],
            )
            .await
            .with_context(|| format!("marking {stream} history as failed"))?;
        if updated != 1 {
            bail!("{stream} history was not running while marking it failed");
        }
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
    ) -> Result<Option<(Vec<u8>, u32)>> {
        let sidechain = i16::from(sidechain);
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT block_hash, height FROM event
                 WHERE source = $1 AND kind = $2 AND sidechain = $3
                   AND block_hash IS NOT NULL AND height IS NOT NULL
                 ORDER BY height DESC
                 LIMIT 1",
                &[&self.source, &kind, &sidechain],
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
    pub async fn last_recorded_height(&self, kind: &str, sidechain: u8) -> Result<Option<u32>> {
        let sidechain = i16::from(sidechain);
        let client = self.client.lock().await;
        let row = client
            .query_one(
                "SELECT max(height) FROM event
                 WHERE source = $1 AND kind = $2 AND sidechain = $3",
                &[&self.source, &kind, &sidechain],
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
    let covered_tip_hash: Option<Vec<u8>> = row.get(3);
    let covered_tip_height: Option<i32> = row.get(4);
    let covered_tip = block_from_parts(covered_tip_hash, covered_tip_height, "covered tip")?;
    let target_tip_hash: Vec<u8> = row.get(5);
    let target_tip_height: i32 = row.get(6);
    let target_tip =
        block_from_parts(Some(target_tip_hash), Some(target_tip_height), "target tip")?
            .context("history target tip is missing")?;
    let next_hash: Option<Vec<u8>> = row.get(9);
    let next_height: Option<i32> = row.get(10);
    let status: String = row.get(11);
    let rows_recorded: i64 = row.get(12);
    let effective_page_blocks: i32 = row.get(13);

    Ok(HistoryCoverage {
        stream: row.get(0),
        sidechain,
        coverage_start_height: height_from_i32(row.get(2), "coverage start")?,
        covered_tip,
        target_tip,
        floor_hash: row.get(7),
        floor_height: row
            .get::<_, Option<i32>>(8)
            .map(|height| height_from_i32(height, "history floor"))
            .transpose()?,
        next: block_from_parts(next_hash, next_height, "next history cursor")?,
        status: HistoryStatus::parse(&status)?,
        rows_recorded: u64::try_from(rows_recorded).context("history rows_recorded is negative")?,
        effective_page_blocks: u32::try_from(effective_page_blocks)
            .context("history page size is not positive")?,
        last_error: row.get(14),
    })
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

async fn insert(
    transaction: &mut Transaction<'_>,
    source: &'static str,
    event: &Event,
) -> Result<u64> {
    let facts = facts(event).context("describing an event for the record")?;
    let payload = json::render(event).context("rendering an event payload as JSON")?;
    let envelope = event.encode_to_vec();
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_millis(event.timestamp);

    transaction
        .execute(
            "INSERT INTO event
                 (observed_at, source, kind, sidechain, block_hash, height, envelope, payload)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT ON CONSTRAINT event_identity DO NOTHING",
            &[
                &observed_at,
                &source,
                &facts.kind,
                &facts.sidechain,
                &facts.block_hash,
                &facts.height,
                &envelope,
                &payload,
            ],
        )
        .await
        .with_context(|| format!("recording a {} event", facts.kind))
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
