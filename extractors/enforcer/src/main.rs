#![cfg_attr(feature = "strict", deny(warnings))]

use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use enforcer_extractor::{Args, logging, run};
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(error) = logging::init(args.log_level) {
        eprintln!("failed to initialize diagnostics: {error:#}");
        return ExitCode::FAILURE;
    }

    match run_application(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(
                error = %format!("{error:#}"),
                "enforcer extractor terminated with an error"
            );
            ExitCode::FAILURE
        }
    }
}

async fn run_application(args: Args) -> Result<()> {
    let shutdown_timeout = args.shutdown_timeout();
    #[cfg(unix)]
    let mut signals = ShutdownSignals::new().context("installing shutdown signal handlers")?;
    #[cfg(not(unix))]
    let mut signals = ShutdownSignals::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut extractor = tokio::spawn(run(args, shutdown_rx));

    tokio::select! {
        result = &mut extractor => flatten_join(result),
        signal = signals.recv() => {
            let signal = match signal {
                Ok(signal) => signal,
                Err(error) => {
                    abort_and_join(&mut extractor).await;
                    return Err(error).context("waiting for a shutdown signal");
                }
            };
            tracing::info!(signal = signal.name(), "received shutdown signal");
            let _ = shutdown_tx.send(true);
            finish_graceful_shutdown(
                &mut extractor,
                shutdown_timeout,
                signals.recv(),
            )
            .await
        }
    }
}

async fn finish_graceful_shutdown<F>(
    extractor: &mut JoinHandle<Result<()>>,
    shutdown_timeout: Duration,
    second_signal: F,
) -> Result<()>
where
    F: Future<Output = Result<ShutdownSignal>>,
{
    tokio::select! {
        biased;
        result = &mut *extractor => flatten_join(result),
        signal = second_signal => {
            let signal = match signal {
                Ok(signal) => signal,
                Err(error) => {
                    abort_and_join(extractor).await;
                    return Err(error).context("waiting for a second shutdown signal");
                }
            };
            tracing::warn!(
                signal = signal.name(),
                "received a second shutdown signal; forcing termination"
            );
            abort_and_join(extractor).await;
            bail!("forced shutdown after receiving a second signal");
        }
        () = tokio::time::sleep(shutdown_timeout) => {
            tracing::error!(
                timeout_seconds = shutdown_timeout.as_secs(),
                "graceful shutdown timed out; forcing termination"
            );
            abort_and_join(extractor).await;
            bail!(
                "graceful shutdown timed out after {}s",
                shutdown_timeout.as_secs()
            );
        }
    }
}

fn flatten_join(result: Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    result.context("joining the enforcer extractor task")?
}

async fn abort_and_join(extractor: &mut JoinHandle<Result<()>>) {
    extractor.abort();
    let _ = extractor.await;
}

#[derive(Clone, Copy, Debug)]
enum ShutdownSignal {
    Interrupt,
    #[cfg(unix)]
    Terminate,
}

impl ShutdownSignal {
    const fn name(self) -> &'static str {
        match self {
            Self::Interrupt => "SIGINT",
            #[cfg(unix)]
            Self::Terminate => "SIGTERM",
        }
    }
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: signal::unix::Signal,
    terminate: signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn new() -> Result<Self> {
        use signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("installing SIGINT handler")?,
            terminate: signal(SignalKind::terminate()).context("installing SIGTERM handler")?,
        })
    }

    async fn recv(&mut self) -> Result<ShutdownSignal> {
        tokio::select! {
            signal = self.interrupt.recv() => {
                signal.context("SIGINT signal stream ended")?;
                Ok(ShutdownSignal::Interrupt)
            }
            signal = self.terminate.recv() => {
                signal.context("SIGTERM signal stream ended")?;
                Ok(ShutdownSignal::Terminate)
            }
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    const fn new() -> Self {
        Self
    }

    async fn recv(&mut self) -> Result<ShutdownSignal> {
        signal::ctrl_c().await.context("waiting for Ctrl+C")?;
        Ok(ShutdownSignal::Interrupt)
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::time::Duration;

    use anyhow::Result;

    use super::{ShutdownSignal, finish_graceful_shutdown};

    #[tokio::test]
    async fn graceful_shutdown_waits_for_task_completion() {
        let mut extractor = tokio::spawn(async { Ok(()) });

        finish_graceful_shutdown(&mut extractor, Duration::from_secs(1), future::pending())
            .await
            .expect("extractor completed cleanly");
    }

    #[tokio::test]
    async fn a_second_signal_aborts_shutdown() {
        let mut extractor = tokio::spawn(async { future::pending::<Result<()>>().await });

        let error = finish_graceful_shutdown(
            &mut extractor,
            Duration::from_secs(1),
            future::ready(Ok(ShutdownSignal::Interrupt)),
        )
        .await
        .expect_err("second signal forces shutdown");

        assert!(error.to_string().contains("second signal"));
        assert!(extractor.is_finished());
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_the_task() {
        let mut extractor = tokio::spawn(async { future::pending::<Result<()>>().await });

        let error =
            finish_graceful_shutdown(&mut extractor, Duration::from_millis(10), future::pending())
                .await
                .expect_err("timeout forces shutdown");

        assert!(error.to_string().contains("timed out"));
        assert!(extractor.is_finished());
    }
}
