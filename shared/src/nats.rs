//! Core NATS configuration and protobuf event publication.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_nats::{Client, ConnectOptions, Event as NatsEvent};
use clap::Args as ClapArgs;
use prost::Message;
use tokio::time::timeout;

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

    /// Maximum time to wait for the NATS client transport to flush.
    #[arg(
        long,
        env = "BIP300_MONITOR_NATS_FLUSH_TIMEOUT_SECONDS",
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub nats_flush_timeout_seconds: u64,
}

impl Default for NatsArgs {
    fn default() -> Self {
        Self {
            nats_url: DEFAULT_ADDRESS.to_owned(),
            nats_username: None,
            nats_password: None,
            nats_password_file: None,
            nats_flush_timeout_seconds: 10,
        }
    }
}

impl NatsArgs {
    /// Return the maximum time allowed for a server-confirmed flush.
    pub const fn flush_timeout(&self) -> Duration {
        Duration::from_secs(self.nats_flush_timeout_seconds)
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

    let options = ConnectOptions::new()
        .name(CLIENT_NAME)
        .event_callback(|event| async move {
            match event {
                NatsEvent::Connected => tracing::info!("connected to Core NATS"),
                NatsEvent::Disconnected => tracing::warn!("disconnected from Core NATS"),
                NatsEvent::LameDuckMode => {
                    tracing::warn!("Core NATS server entered lame-duck mode");
                }
                NatsEvent::Draining => tracing::info!("Core NATS connection is draining"),
                NatsEvent::Closed => tracing::warn!("Core NATS connection closed"),
                NatsEvent::SlowConsumer(pending_messages) => {
                    tracing::warn!(pending_messages, "Core NATS reported a slow consumer");
                }
                NatsEvent::ServerError(error) => {
                    tracing::error!(%error, "Core NATS server error");
                }
                NatsEvent::ClientError(error) => {
                    tracing::error!(%error, "Core NATS client error");
                }
            }
        });
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
    flush_timeout: Duration,
}

impl EventPublisher {
    /// Connect a publisher using the supplied NATS configuration.
    pub async fn connect(args: &NatsArgs) -> Result<Self> {
        let client = connect_options(args)?
            .connect(&args.nats_url)
            .await
            .with_context(|| format!("connecting to Core NATS at `{}`", args.nats_url))?;

        Ok(Self {
            client,
            flush_timeout: args.flush_timeout(),
        })
    }

    /// Publish one protobuf event to a stable monitor subject.
    pub async fn publish(&self, subject: Subject, event: &Event) -> Result<()> {
        self.client
            .publish(subject.to_string(), event.encode_to_vec().into())
            .await
            .with_context(|| format!("publishing event to NATS subject `{subject}`"))
    }

    /// Publish one event and wait for the client transport buffer to flush.
    pub async fn publish_and_flush(&self, subject: Subject, event: &Event) -> Result<()> {
        self.publish(subject, event).await?;
        self.flush().await
    }

    /// Wait until the server has processed all previously sent messages.
    pub async fn flush(&self) -> Result<()> {
        timeout(self.flush_timeout, self.client.flush())
            .await
            .with_context(|| {
                format!(
                    "timed out after {}s while flushing the Core NATS connection",
                    self.flush_timeout.as_secs()
                )
            })?
            .context("flushing the Core NATS connection")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

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

    #[test]
    fn uses_a_bounded_flush_timeout() {
        assert_eq!(NatsArgs::default().flush_timeout(), Duration::from_secs(10));
    }
}
