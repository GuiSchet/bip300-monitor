#![cfg_attr(feature = "strict", deny(warnings))]

use std::process::ExitCode;

use clap::Parser;
use event_logger::{Args, run};
use shared::{logging, process};

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(error) = logging::init(args.log_level) {
        eprintln!("failed to initialize diagnostics: {error:#}");
        return ExitCode::FAILURE;
    }

    let shutdown_timeout = args.shutdown_timeout();
    match process::run_until_shutdown("event logger", shutdown_timeout, move |shutdown_rx| {
        run(args, shutdown_rx)
    })
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(
                error = %format!("{error:#}"),
                "event logger terminated with an error"
            );
            ExitCode::FAILURE
        }
    }
}
