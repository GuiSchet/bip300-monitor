//! Command-line and environment configuration for the enforcer extractor.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use shared::nats::NatsArgs;

/// Runtime configuration for the enforcer extractor.
#[derive(Clone, Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Core NATS connection settings.
    #[command(flatten)]
    pub nats: NatsArgs,

    /// HTTP URL of the enforcer validator gRPC endpoint.
    #[arg(
        long,
        env = "BIP300_MONITOR_ENFORCER_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
    pub enforcer_endpoint: String,

    /// Sidechain slots to monitor. May be repeated or comma-separated.
    #[arg(
        long = "sidechain",
        env = "BIP300_MONITOR_SIDECHAINS",
        value_name = "SLOT",
        value_delimiter = ',',
        num_args = 1..,
        required = true
    )]
    pub sidechains: Vec<u8>,

    /// Timeout in seconds for connections and unary enforcer requests.
    #[arg(
        long,
        env = "BIP300_MONITOR_REQUEST_TIMEOUT_SECONDS",
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub request_timeout_seconds: u64,
}

impl Args {
    /// Validate invariants that are not expressible directly through clap.
    pub fn validate(&self) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use clap::Parser;

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
        assert_eq!(args.request_timeout_seconds, 10);
        args.validate().expect("unique sidechains");
    }

    #[test]
    fn rejects_missing_and_duplicate_sidechains() {
        assert!(
            Args::try_parse_from(["enforcer-extractor"]).is_err(),
            "at least one sidechain is required"
        );

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
