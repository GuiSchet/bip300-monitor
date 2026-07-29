//! Process-wide tracing configuration.

use std::io::{self, IsTerminal};

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::config::LogLevel;

/// Initialize structured diagnostics for this process.
pub fn init(default_level: LogLevel) -> Result<()> {
    tracing_log::LogTracer::init().context("installing the log-to-tracing bridge")?;

    let rust_log = std::env::var_os("RUST_LOG").map(|value| value.to_string_lossy().into_owned());
    let filter = build_filter(default_level, rust_log.as_deref())?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(io::stderr().is_terminal())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .context("installing the global tracing subscriber")
}

fn build_filter(default_level: LogLevel, rust_log: Option<&str>) -> Result<EnvFilter> {
    let filter = EnvFilter::builder()
        .with_default_directive(default_level.as_filter().into())
        .parse_lossy(rust_log.unwrap_or_default());

    if rust_log.is_none() && default_level == LogLevel::Info {
        return Ok(filter.add_directive(
            "async_nats=warn"
                .parse()
                .context("parsing the default async-nats logging directive")?,
        ));
    }

    Ok(filter)
}

#[cfg(test)]
mod tests {
    use super::build_filter;
    use crate::config::LogLevel;

    #[test]
    fn normal_logging_suppresses_duplicate_async_nats_info_events() {
        let filter = build_filter(LogLevel::Info, None).expect("default filter");
        let filter = filter.to_string();

        assert!(filter.contains("info"));
        assert!(filter.contains("async_nats=warn"));
    }

    #[test]
    fn explicit_rust_log_remains_authoritative() {
        let filter = build_filter(
            LogLevel::Info,
            Some("enforcer_extractor=debug,async_nats=trace"),
        )
        .expect("explicit filter")
        .to_string();

        assert!(filter.contains("enforcer_extractor=debug"));
        assert!(filter.contains("async_nats=trace"));
        assert!(!filter.contains("async_nats=warn"));
    }

    #[test]
    fn verbose_cli_logging_keeps_dependency_diagnostics() {
        for level in [LogLevel::Debug, LogLevel::Trace] {
            let filter = build_filter(level, None)
                .expect("verbose filter")
                .to_string();
            assert!(!filter.contains("async_nats=warn"));
        }
    }
}
