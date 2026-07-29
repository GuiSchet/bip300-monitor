//! Shared process lifecycle and shutdown handling.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Run one service until it completes or the process receives a shutdown signal.
///
/// The first signal requests graceful shutdown. A second signal or expiry of
/// `shutdown_timeout` aborts the service task.
pub async fn run_until_shutdown<F, Fut>(
    service_name: &'static str,
    shutdown_timeout: Duration,
    run: F,
) -> Result<()>
where
    F: FnOnce(watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    #[cfg(unix)]
    let mut signals = ShutdownSignals::new().context("installing shutdown signal handlers")?;
    #[cfg(not(unix))]
    let mut signals = ShutdownSignals::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut service = tokio::spawn(run(shutdown_rx));

    tokio::select! {
        result = &mut service => flatten_join(result, service_name),
        signal = signals.recv() => {
            let signal = match signal {
                Ok(signal) => signal,
                Err(error) => {
                    abort_and_join(&mut service).await;
                    return Err(error).context("waiting for a shutdown signal");
                }
            };
            tracing::info!(signal = signal.name(), service = service_name, "received shutdown signal");
            let _ = shutdown_tx.send(true);
            finish_graceful_shutdown(
                &mut service,
                service_name,
                shutdown_timeout,
                signals.recv(),
            )
            .await
        }
    }
}

async fn finish_graceful_shutdown<F>(
    service: &mut JoinHandle<Result<()>>,
    service_name: &'static str,
    shutdown_timeout: Duration,
    second_signal: F,
) -> Result<()>
where
    F: Future<Output = Result<ShutdownSignal>>,
{
    tokio::select! {
        biased;
        result = &mut *service => flatten_join(result, service_name),
        signal = second_signal => {
            let signal = match signal {
                Ok(signal) => signal,
                Err(error) => {
                    abort_and_join(service).await;
                    return Err(error).context("waiting for a second shutdown signal");
                }
            };
            tracing::warn!(
                signal = signal.name(),
                service = service_name,
                "received a second shutdown signal; forcing termination"
            );
            abort_and_join(service).await;
            bail!("forced shutdown of {service_name} after receiving a second signal");
        }
        () = tokio::time::sleep(shutdown_timeout) => {
            tracing::error!(
                timeout_seconds = shutdown_timeout.as_secs(),
                service = service_name,
                "graceful shutdown timed out; forcing termination"
            );
            abort_and_join(service).await;
            bail!(
                "graceful shutdown of {service_name} timed out after {}s",
                shutdown_timeout.as_secs()
            );
        }
    }
}

fn flatten_join(
    result: Result<Result<()>, tokio::task::JoinError>,
    service_name: &'static str,
) -> Result<()> {
    result.with_context(|| format!("joining the {service_name} task"))?
}

async fn abort_and_join(service: &mut JoinHandle<Result<()>>) {
    service.abort();
    let _ = service.await;
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
        let mut service = tokio::spawn(async { Ok(()) });

        finish_graceful_shutdown(
            &mut service,
            "test service",
            Duration::from_secs(1),
            future::pending(),
        )
        .await
        .expect("service completed cleanly");
    }

    #[tokio::test]
    async fn a_second_signal_aborts_shutdown() {
        let mut service = tokio::spawn(async { future::pending::<Result<()>>().await });

        let error = finish_graceful_shutdown(
            &mut service,
            "test service",
            Duration::from_secs(1),
            future::ready(Ok(ShutdownSignal::Interrupt)),
        )
        .await
        .expect_err("second signal forces shutdown");

        assert!(error.to_string().contains("second signal"));
        assert!(service.is_finished());
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_the_task() {
        let mut service = tokio::spawn(async { future::pending::<Result<()>>().await });

        let error = finish_graceful_shutdown(
            &mut service,
            "test service",
            Duration::from_millis(10),
            future::pending(),
        )
        .await
        .expect_err("timeout forces shutdown");

        assert!(error.to_string().contains("timed out"));
        assert!(service.is_finished());
    }
}
