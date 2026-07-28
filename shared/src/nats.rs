//! Core NATS configuration and protobuf event publication.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use async_nats::{Client, ConnectOptions};
use clap::Args as ClapArgs;
use prost::Message;

use crate::nats_subjects::Subject;
use crate::protobuf::event::Event;

const CLIENT_NAME: &str = "bip300-monitor-enforcer";
const DEFAULT_ADDRESS: &str = "nats://127.0.0.1:4222";

/// Reusable command-line arguments for a Core NATS connection.
#[derive(ClapArgs, Clone)]
pub struct NatsArgs {
    /// Core NATS server URL.
    #[arg(
        long,
        env = "BIP300_MONITOR_NATS_URL",
        default_value = DEFAULT_ADDRESS
    )]
    pub nats_url: String,

    /// Username used for NATS user/password authentication.
    #[arg(long, env = "BIP300_MONITOR_NATS_USERNAME")]
    pub nats_username: Option<String>,

    /// Password used for NATS user/password authentication.
    #[arg(
        long,
        env = "BIP300_MONITOR_NATS_PASSWORD",
        requires = "nats_username",
        conflicts_with = "nats_password_file"
    )]
    pub nats_password: Option<String>,

    /// File containing the NATS password.
    #[arg(
        long,
        env = "BIP300_MONITOR_NATS_PASSWORD_FILE",
        requires = "nats_username",
        conflicts_with = "nats_password"
    )]
    pub nats_password_file: Option<PathBuf>,
}

impl Default for NatsArgs {
    fn default() -> Self {
        Self {
            nats_url: DEFAULT_ADDRESS.to_owned(),
            nats_username: None,
            nats_password: None,
            nats_password_file: None,
        }
    }
}

/// Prepare connection options without exposing credentials in logs.
pub fn connect_options(args: &NatsArgs) -> Result<ConnectOptions> {
    let password = match (&args.nats_password, &args.nats_password_file) {
        (Some(_), Some(_)) => {
            bail!("only one of `nats_password` and `nats_password_file` may be set")
        }
        (Some(password), None) => Some(password.clone()),
        (None, Some(path)) => {
            let password = fs::read_to_string(path)
                .with_context(|| format!("reading NATS password file `{}`", path.display()))?;
            let password = password.trim_end_matches(['\r', '\n']).to_owned();
            if password.is_empty() {
                bail!("NATS password file `{}` is empty", path.display());
            }
            Some(password)
        }
        (None, None) => None,
    };

    let options = ConnectOptions::new().name(CLIENT_NAME);
    match (&args.nats_username, password) {
        (Some(username), Some(password)) => {
            Ok(options.user_and_password(username.clone(), password))
        }
        (Some(_), None) => bail!("a NATS username requires a password or password file"),
        (None, Some(_)) => bail!("a NATS password requires a username"),
        (None, None) => Ok(options),
    }
}

/// Publisher for protobuf envelopes over Core NATS.
#[derive(Clone)]
pub struct EventPublisher {
    client: Client,
}

impl EventPublisher {
    /// Connect a publisher using the supplied NATS configuration.
    pub async fn connect(args: &NatsArgs) -> Result<Self> {
        let client = connect_options(args)?
            .connect(&args.nats_url)
            .await
            .with_context(|| format!("connecting to Core NATS at `{}`", args.nats_url))?;

        Ok(Self { client })
    }

    /// Publish one protobuf event to a stable monitor subject.
    pub async fn publish(&self, subject: Subject, event: &Event) -> Result<()> {
        self.client
            .publish(subject.to_string(), event.encode_to_vec().into())
            .await
            .with_context(|| format!("publishing event to NATS subject `{subject}`"))
    }

    /// Wait until the server has processed all previously sent messages.
    pub async fn flush(&self) -> Result<()> {
        self.client
            .flush()
            .await
            .context("flushing the Core NATS connection")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{NatsArgs, connect_options};

    #[test]
    fn accepts_anonymous_and_password_authentication() {
        connect_options(&NatsArgs::default()).expect("anonymous options");
        connect_options(&NatsArgs {
            nats_username: Some("monitor".to_owned()),
            nats_password: Some("secret".to_owned()),
            ..NatsArgs::default()
        })
        .expect("user/password options");
    }

    #[test]
    fn reads_password_file_without_trailing_newline() {
        let path = std::env::temp_dir().join(format!(
            "bip300-monitor-nats-password-{}",
            std::process::id()
        ));
        fs::write(&path, "secret\r\n").expect("write password fixture");

        let result = connect_options(&NatsArgs {
            nats_username: Some("monitor".to_owned()),
            nats_password_file: Some(path.clone()),
            ..NatsArgs::default()
        });

        fs::remove_file(path).expect("remove password fixture");
        result.expect("password file options");
    }

    #[test]
    fn rejects_incomplete_credentials() {
        let result = connect_options(&NatsArgs {
            nats_username: Some("monitor".to_owned()),
            ..NatsArgs::default()
        });
        let error = match result {
            Ok(_) => panic!("username without password must fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("requires a password or password file")
        );
    }
}
