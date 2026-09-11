//! Command-line and environment configuration for the enforcer extractor.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use shared::logging::LogLevel;
use shared::nats::NatsArgs;
use shared::store::PostgresArgs;

/// Runtime configuration for the enforcer extractor.
#[derive(Clone, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Core NATS connection settings, used for best-effort live fan-out.
    #[command(flatten)]
    pub nats: NatsArgs,

    /// Postgres connection settings for the authoritative record.
    #[command(flatten)]
    pub postgres: PostgresArgs,

    /// Default log level when RUST_LOG does not provide a filter.
    #[arg(
        long,
        env = "BIP300_MONITOR_LOG_LEVEL",
        value_enum,
        default_value_t = LogLevel::Info
    )]
    pub log_level: LogLevel,

    /// HTTP URL of the enforcer validator gRPC endpoint.
    #[arg(
        long,
        env = "BIP300_MONITOR_ENFORCER_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
    pub enforcer_endpoint: String,

    /// Sidechain slots to monitor. May be repeated or comma-separated.
    ///
    /// Left unset, active slots are discovered at startup and newly activated
    /// slots are added while the extractor is running. A deployment that pins
    /// an exact observation set may still configure this list.
    #[arg(
        long = "sidechain",
        env = "BIP300_MONITOR_SIDECHAINS",
        value_name = "SLOT",
        value_delimiter = ',',
        num_args = 1..
    )]
    pub sidechains: Vec<u8>,

    /// Maximum number of blocks held by one historical page.
    ///
    /// The complete history is always recovered; this controls only the memory
    /// and transaction size of each resumable step.
    #[arg(
        long,
        env = "BIP300_MONITOR_BACKFILL_PAGE_BLOCKS",
        default_value_t = 128,
        value_parser = clap::value_parser!(u32).range(1..=512)
    )]
    pub backfill_page_blocks: u32,

    /// Delay between historical pages, in milliseconds.
    ///
    /// Live writes can use the Postgres connection between these short pages,
    /// and the enforcer is never hammered by an unbounded catch-up loop.
    #[arg(
        long,
        env = "BIP300_MONITOR_BACKFILL_PAGE_PAUSE_MS",
        default_value_t = 100,
        value_parser = clap::value_parser!(u64).range(0..)
    )]
    pub backfill_page_pause_ms: u64,

    /// How often the mainchain tip is re-read, in seconds.
    ///
    /// The live streams already report every block, so this is not the usual
    /// path. It exists because the state worker has to be woken by something
    /// that does not depend on a slot being observed: with no slot resolved
    /// there is no stream at all, and a stream that wedges without closing
    /// would otherwise stop the refresh silently.
    #[arg(
        long,
        env = "BIP300_MONITOR_TIP_POLL_INTERVAL_SECONDS",
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub tip_poll_interval_seconds: u64,

    /// File whose modification time is refreshed on every successful tip read.
    ///
    /// The extractor exposes no port, so a container healthcheck has nothing to
    /// ask. This gives it something: the time is refreshed by work that only
    /// succeeds when the enforcer is answering, so a wedged process stops
    /// refreshing it. Unset means no file is written.
    #[arg(long, env = "BIP300_MONITOR_LIVENESS_FILE")]
    pub liveness_file: Option<std::path::PathBuf>,

    /// Timeout in seconds for connections, unary requests, and stream setup.
    #[arg(
        long,
        env = "BIP300_MONITOR_REQUEST_TIMEOUT_SECONDS",
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub request_timeout_seconds: u64,

    /// Maximum time allowed for graceful shutdown.
    #[arg(
        long,
        env = "BIP300_MONITOR_SHUTDOWN_TIMEOUT_SECONDS",
        default_value_t = 15,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub shutdown_timeout_seconds: u64,
}

impl Args {
    /// Validate invariants that are not expressible directly through clap.
    pub fn validate(&self) -> Result<()> {
        if std::env::var_os("BIP300_MONITOR_BACKFILL_MAX_BLOCKS").is_some() {
            bail!(
                "BIP300_MONITOR_BACKFILL_MAX_BLOCKS was removed: use \
                 BIP300_MONITOR_BACKFILL_PAGE_BLOCKS to bound each page; \
                 total history is no longer capped"
            );
        }
        if self.shutdown_timeout_seconds <= self.nats.nats_flush_timeout_seconds {
            bail!(
                "shutdown timeout ({}s) must be greater than the NATS flush timeout ({}s)",
                self.shutdown_timeout_seconds,
                self.nats.nats_flush_timeout_seconds
            );
        }

        let mut unique = HashSet::with_capacity(self.sidechains.len());
        for sidechain in &self.sidechains {
            if !unique.insert(sidechain) {
                bail!("sidechain slot {sidechain} was configured more than once");
            }
        }
        Ok(())
    }

    /// Return the configured request timeout.
    pub const fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }

    /// Return the configured interval between tip polls.
    pub const fn tip_poll_interval(&self) -> Duration {
        Duration::from_secs(self.tip_poll_interval_seconds)
    }

    /// Return the configured pause between historical pages.
    pub const fn backfill_page_pause(&self) -> Duration {
        Duration::from_millis(self.backfill_page_pause_ms)
    }

    /// Return the configured graceful-shutdown timeout.
    pub const fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_seconds)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use shared::logging::LogLevel;

    use super::Args;

    #[test]
    fn parses_explicit_sidechains_and_defaults() {
        let args = Args::try_parse_from([
            "enforcer-extractor",
            "--sidechain",
            "9,98",
            "--nats-url",
            "nats://nats:4222",
        ])
        .expect("valid arguments");

        assert_eq!(args.sidechains, vec![9, 98]);
        assert_eq!(args.enforcer_endpoint, "http://127.0.0.1:50051");
        assert_eq!(args.nats.nats_url, "nats://nats:4222");
        assert_eq!(args.log_level, LogLevel::Info);
        assert_eq!(args.request_timeout_seconds, 10);
        assert_eq!(args.backfill_page_blocks, 128);
        assert_eq!(args.backfill_page_pause_ms, 100);
        assert_eq!(args.nats.nats_flush_timeout_seconds, 10);
        assert_eq!(args.shutdown_timeout_seconds, 15);
        args.validate().expect("unique sidechains");
    }

    #[test]
    fn validates_the_shutdown_and_flush_timeout_relationship() {
        let valid = Args::try_parse_from([
            "enforcer-extractor",
            "--sidechain",
            "9",
            "--nats-flush-timeout-seconds",
            "10",
            "--shutdown-timeout-seconds",
            "11",
        ])
        .expect("syntactically valid arguments");
        valid.validate().expect("shutdown has time to flush");

        for shutdown_timeout in ["10", "9"] {
            let invalid = Args::try_parse_from([
                "enforcer-extractor",
                "--sidechain",
                "9",
                "--nats-flush-timeout-seconds",
                "10",
                "--shutdown-timeout-seconds",
                shutdown_timeout,
            ])
            .expect("syntactically valid arguments");
            let error = invalid
                .validate()
                .expect_err("shutdown must outlast a NATS flush");
            assert!(
                error
                    .to_string()
                    .contains("must be greater than the NATS flush timeout")
            );
        }
    }

    #[test]
    fn parses_each_supported_log_level() {
        for level in ["error", "warn", "info", "debug", "trace"] {
            Args::try_parse_from([
                "enforcer-extractor",
                "--sidechain",
                "9",
                "--log-level",
                level,
            ])
            .expect("supported log level");
        }

        assert!(
            Args::try_parse_from(["enforcer-extractor", "--sidechain", "9", "-l", "debug"])
                .is_err(),
            "the undocumented short flag must not be accepted"
        );
    }

    #[test]
    fn omitting_sidechains_defers_to_discovery() {
        let args = Args::try_parse_from(["enforcer-extractor"])
            .expect("sidechains may be discovered instead of configured");
        assert!(args.sidechains.is_empty());
        args.validate().expect("an empty slot list is valid");
    }

    #[test]
    fn rejects_duplicate_sidechains() {
        let args =
            Args::try_parse_from(["enforcer-extractor", "--sidechain", "9", "--sidechain", "9"])
                .expect("syntactically valid arguments");
        let error = args.validate().expect_err("duplicate sidechains must fail");
        assert!(error.to_string().contains("configured more than once"));
    }

    #[test]
    fn rejects_password_without_username() {
        assert!(
            Args::try_parse_from([
                "enforcer-extractor",
                "--sidechain",
                "9",
                "--nats-password",
                "secret",
            ])
            .is_err(),
            "password requires a username"
        );
    }
}
