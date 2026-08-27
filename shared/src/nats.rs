//! Core NATS configuration and protobuf event transport.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_nats::connection::State;
use async_nats::{Client, ConnectOptions, Event as NatsEvent, Subscriber};
use clap::Args as ClapArgs;
use futures_util::StreamExt;
use prost::Message;
use tokio::time::timeout;

use crate::nats_subjects::Subject;
use crate::protobuf::event::Event;

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
    /// Return the maximum time allowed for a client transport flush.
    pub const fn flush_timeout(&self) -> Duration {
        Duration::from_secs(self.nats_flush_timeout_seconds)
    }
}

/// Prepare connection options without exposing credentials in logs.
pub fn connect_options(args: &NatsArgs, client_name: &'static str) -> Result<ConnectOptions> {
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
        .name(client_name)
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
    ///
    /// Publication is best-effort, so an unreachable server must not stop the
    /// extractor: `retry_on_initial_connect` makes this return a client that
    /// reconnects in the background, and the event callback reports every
    /// transition. A malformed configuration is still fatal, because
    /// `connect_options` rejects it before any socket is opened.
    pub async fn connect(args: &NatsArgs, client_name: &'static str) -> Result<Self> {
        let client = connect_options(args, client_name)?
            .retry_on_initial_connect()
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

    /// Whether the transport is established right now.
    ///
    /// `Pending` counts as not connected, and that distinction is the whole
    /// point: a client built with `retry_on_initial_connect` that has never
    /// reached a server sits in `Pending`, never in `Disconnected`.
    pub fn is_connected(&self) -> bool {
        self.client.connection_state() == State::Connected
    }

    /// Publish a batch best-effort, bounded by the flush budget.
    ///
    /// `Client::publish` is an `await` on a bounded queue that only the
    /// connection task drains, and that task does not run while it is
    /// reconnecting. So publishing into a disconnected client fills the queue --
    /// 2048 messages by default -- and then blocks the caller for as long as the
    /// server stays away. A first-sight backfill is thousands of events, which
    /// is exactly the burst that reaches the bound, and the caller it would
    /// block is the extractor's startup path.
    ///
    /// Hence two bounds. The state check keeps a known outage from queueing
    /// anything at all, and the timeout covers a disconnect that lands
    /// mid-batch. Skipping a batch does discard messages that a reconnect might
    /// have delivered, but that would only ever be an arbitrary 2048-message
    /// prefix, Core NATS replays nothing for a consumer that was away, and the
    /// record already holds every event. Best-effort has to mean best-effort:
    /// the alternative here is a stalled extractor.
    pub async fn publish_batch(&self, subject: Subject, events: &[Event]) -> FanOut {
        let observed = events.len();
        if observed == 0 {
            return FanOut::default();
        }
        if !self.is_connected() {
            return FanOut {
                observed,
                published: 0,
                failure: Some("the Core NATS connection is not established".to_owned()),
            };
        }

        let mut published = 0;
        let mut failure = None;
        let bounded = timeout(self.flush_timeout, async {
            for event in events {
                // One bad event must not abandon the rest of the batch; they are
                // independent, and the timeout is what bounds a broken transport.
                match self.publish(subject, event).await {
                    Ok(()) => published += 1,
                    Err(error) => {
                        failure.get_or_insert_with(|| format!("{error:#}"));
                    }
                }
            }
        })
        .await;

        if bounded.is_err() {
            failure = Some(format!(
                "timed out after {}s while publishing the batch",
                self.flush_timeout.as_secs()
            ));
        } else if let Err(error) = self.flush().await {
            failure.get_or_insert_with(|| format!("{error:#}"));
        }

        FanOut {
            observed,
            published,
            failure,
        }
    }

    /// Wait until the NATS client transport buffer has been flushed.
    pub async fn flush(&self) -> Result<()> {
        flush_client(&self.client, self.flush_timeout).await
    }
}

/// What one best-effort fan-out managed to do.
#[derive(Debug, Default)]
pub struct FanOut {
    /// Events the batch offered.
    pub observed: usize,
    /// Events handed to the transport.
    pub published: usize,
    /// Why the rest were dropped, when some were.
    pub failure: Option<String>,
}

impl FanOut {
    /// Events the transport never took.
    pub const fn dropped(&self) -> usize {
        self.observed - self.published
    }
}

/// One message received from an event subject.
#[derive(Debug)]
pub enum ReceivedEvent {
    /// A valid protobuf event envelope.
    Decoded(Event),
    /// A message that could not be decoded as the expected protobuf envelope.
    Invalid {
        /// Protobuf decoding failure.
        error: prost::DecodeError,
        /// Size of the rejected NATS payload.
        payload_len: usize,
    },
}

/// Subscriber that decodes monitor protobuf envelopes from one stable subject.
pub struct EventSubscriber {
    client: Client,
    subscriber: Subscriber,
    subject: Subject,
    flush_timeout: Duration,
}

impl EventSubscriber {
    /// Connect and register a subscription before returning.
    ///
    /// Deliberately fail-fast, unlike [`EventPublisher::connect`]: a subscriber
    /// exists only to receive, so an unreachable server is its whole job
    /// failing rather than a degraded side channel. Retrying here would also
    /// hang the flush that confirms the subscription.
    pub async fn connect(
        args: &NatsArgs,
        subject: Subject,
        client_name: &'static str,
    ) -> Result<Self> {
        let client = connect_options(args, client_name)?
            .connect(&args.nats_url)
            .await
            .with_context(|| format!("connecting to Core NATS at `{}`", args.nats_url))?;
        let subscriber = client
            .subscribe(subject.to_string())
            .await
            .with_context(|| format!("subscribing to NATS subject `{subject}`"))?;
        flush_client(&client, args.flush_timeout())
            .await
            .with_context(|| format!("confirming the NATS subscription to `{subject}`"))?;

        Ok(Self {
            client,
            subscriber,
            subject,
            flush_timeout: args.flush_timeout(),
        })
    }

    /// Receive the next message, distinguishing invalid payloads from transport failure.
    pub async fn next_event(&mut self) -> Result<ReceivedEvent> {
        let message = self
            .subscriber
            .next()
            .await
            .with_context(|| format!("NATS subscription to `{}` ended", self.subject))?;

        let payload_len = message.payload.len();
        Ok(match Event::decode(message.payload) {
            Ok(event) => ReceivedEvent::Decoded(event),
            Err(error) => ReceivedEvent::Invalid { error, payload_len },
        })
    }

    /// Round-trip the server to confirm the subscription is still live.
    ///
    /// A subscriber has no other way to tell "the chain is quiet" from "the
    /// transport died": both look like no messages arriving. A flush is answered
    /// by the server, so it fails when the connection is gone.
    pub async fn confirm_connection(&self) -> Result<()> {
        flush_client(&self.client, self.flush_timeout)
            .await
            .with_context(|| format!("confirming the NATS subscription to `{}`", self.subject))
    }

    /// Remove the subscription and flush the client transport.
    pub async fn close(mut self) -> Result<()> {
        timeout(self.flush_timeout, async {
            self.subscriber
                .unsubscribe()
                .await
                .with_context(|| format!("unsubscribing from NATS subject `{}`", self.subject))?;
            self.client
                .flush()
                .await
                .context("flushing the Core NATS connection after unsubscribe")
        })
        .await
        .with_context(|| {
            format!(
                "timed out after {}s while closing the subscription to `{}`",
                self.flush_timeout.as_secs(),
                self.subject
            )
        })?
        .context("closing the event subscription")
    }
}

async fn flush_client(client: &Client, flush_timeout: Duration) -> Result<()> {
    timeout(flush_timeout, client.flush())
        .await
        .with_context(|| {
            format!(
                "timed out after {}s while flushing the Core NATS connection",
                flush_timeout.as_secs()
            )
        })?
        .context("flushing the Core NATS connection")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::TcpListener;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{EventPublisher, NatsArgs, connect_options};
    use crate::nats_subjects::Subject;
    use crate::protobuf::enforcer_extractor as events;
    use crate::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};

    const TEST_CLIENT_NAME: &str = "bip300-monitor-test";

    /// A port nothing is listening on, taken by binding and releasing it.
    fn closed_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind an ephemeral port")
            .local_addr()
            .expect("read the bound port")
            .port()
    }

    fn ctip_event() -> Event {
        Event::new(
            MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                    sidechain_number: 9,
                    ctip: None,
                })),
            }),
            Some(ObservedBlock::at_height(vec![0x11; 32], 996_259)),
        )
        .expect("system clock after the Unix epoch")
    }

    #[tokio::test]
    async fn a_batch_never_blocks_when_the_transport_never_connected() {
        // Regression. `Client::publish` awaits a bounded queue that only the
        // connection task drains, and with retry-on-initial-connect that task
        // stays inside its reconnect loop and never drains anything. A batch
        // larger than the queue used to park the caller until a server appeared
        // -- on the extractor's startup path, that meant a first-sight backfill
        // stalled the process with its rows already committed and no worker
        // running.
        let publisher = EventPublisher::connect(
            &NatsArgs {
                nats_url: format!("nats://127.0.0.1:{}", closed_port()),
                nats_flush_timeout_seconds: 1,
                ..NatsArgs::default()
            },
            TEST_CLIENT_NAME,
        )
        .await
        .expect("retry-on-initial-connect yields a client with no server present");

        assert!(
            !publisher.is_connected(),
            "a client that never reached a server is Pending, which is not connected"
        );

        // Comfortably past the 2048-message default queue.
        let batch = vec![ctip_event(); 3_000];
        let outcome = timeout(
            Duration::from_secs(5),
            publisher.publish_batch(Subject::Enforcer, &batch),
        )
        .await
        .expect("a best-effort fan-out must never block its caller");

        assert_eq!(outcome.published, 0);
        assert_eq!(outcome.dropped(), 3_000);
        assert!(
            outcome.failure.is_some(),
            "a dropped batch has to say why, or the gap reads as nothing happened"
        );
    }

    #[tokio::test]
    async fn an_empty_batch_is_not_reported_as_a_failure() {
        let publisher = EventPublisher::connect(
            &NatsArgs {
                nats_url: format!("nats://127.0.0.1:{}", closed_port()),
                nats_flush_timeout_seconds: 1,
                ..NatsArgs::default()
            },
            TEST_CLIENT_NAME,
        )
        .await
        .expect("retry-on-initial-connect yields a client with no server present");

        let outcome = publisher.publish_batch(Subject::Enforcer, &[]).await;
        assert_eq!(outcome.observed, 0);
        assert!(outcome.failure.is_none());
    }

    #[test]
    fn accepts_anonymous_and_password_authentication() {
        connect_options(&NatsArgs::default(), TEST_CLIENT_NAME).expect("anonymous options");
        connect_options(
            &NatsArgs {
                nats_username: Some("monitor".to_owned()),
                nats_password: Some("secret".to_owned()),
                ..NatsArgs::default()
            },
            TEST_CLIENT_NAME,
        )
        .expect("user/password options");
    }

    #[test]
    fn reads_password_file_without_trailing_newline() {
        let path = std::env::temp_dir().join(format!(
            "bip300-monitor-nats-password-{}",
            std::process::id()
        ));
        fs::write(&path, "secret\r\n").expect("write password fixture");

        let result = connect_options(
            &NatsArgs {
                nats_username: Some("monitor".to_owned()),
                nats_password_file: Some(path.clone()),
                ..NatsArgs::default()
            },
            TEST_CLIENT_NAME,
        );

        fs::remove_file(path).expect("remove password fixture");
        result.expect("password file options");
    }

    #[test]
    fn rejects_incomplete_credentials() {
        let result = connect_options(
            &NatsArgs {
                nats_username: Some("monitor".to_owned()),
                ..NatsArgs::default()
            },
            TEST_CLIENT_NAME,
        );
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
