//! Command-line and environment configuration.

use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use shared::logging::LogLevel;
use shared::nats::NatsArgs;

/// Runtime configuration for the event logger.
#[derive(Clone, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Core NATS connection settings.
    #[command(flatten)]
    pub nats: NatsArgs,

    /// Default log level when RUST_LOG does not provide a filter.
    #[arg(
        long,
        env = "BIP300_MONITOR_LOG_LEVEL",
        value_enum,
        default_value_t = LogLevel::Info
    )]
    pub log_level: LogLevel,

    /// Log the complete normalized event as one JSON object.
    #[arg(long, env = "BIP300_MONITOR_FULL_EVENTS", default_value_t = false)]
    pub full_events: bool,

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
        if self.shutdown_timeout_seconds <= self.nats.nats_flush_timeout_seconds {
            bail!(
                "shutdown timeout ({}s) must be greater than the NATS flush timeout ({}s)",
                self.shutdown_timeout_seconds,
                self.nats.nats_flush_timeout_seconds
            );
        }
        Ok(())
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
    fn parses_defaults_and_full_event_mode() {
        let defaults = Args::try_parse_from(["event-logger"]).expect("default arguments");
        assert_eq!(defaults.nats.nats_url, "nats://127.0.0.1:4222");
        assert_eq!(defaults.log_level, LogLevel::Info);
        assert!(!defaults.full_events);
        assert_eq!(defaults.shutdown_timeout_seconds, 15);
        defaults.validate().expect("valid defaults");

        let full =
            Args::try_parse_from(["event-logger", "--full-events"]).expect("full event arguments");
        assert!(full.full_events);
    }

    #[test]
    fn validates_shutdown_against_flush_timeout() {
        let args = Args::try_parse_from([
            "event-logger",
            "--nats-flush-timeout-seconds",
            "10",
            "--shutdown-timeout-seconds",
            "10",
        ])
        .expect("syntactically valid arguments");

        let error = args
            .validate()
            .expect_err("shutdown must outlast unsubscribe flush");
        assert!(error.to_string().contains("must be greater"));
    }
}
