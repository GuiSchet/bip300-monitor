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
use tokio_postgres::{Client, NoTls, Transaction};

use crate::json;
use crate::protobuf::event::{Event, event::MonitorEvent};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;

/// Schema statements applied in order at startup.
///
/// Every statement is idempotent, and `schema_version` records how far the
/// record has been migrated so a future statement is applied exactly once.
const MIGRATIONS: &[&str] = &[include_str!("../schema/0001_event.sql")];

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
