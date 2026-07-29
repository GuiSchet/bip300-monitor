//! Process-wide tracing configuration.

use std::io::{self, IsTerminal};

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::config::LogLevel;

/// Initialize structured diagnostics for this process.
pub fn init(default_level: LogLevel) -> Result<()> {
    tracing_log::LogTracer::init().context("installing the log-to-tracing bridge")?;

    let filter = EnvFilter::builder()
        .with_default_directive(default_level.as_filter().into())
        .from_env_lossy();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(io::stderr().is_terminal())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .context("installing the global tracing subscriber")
}
