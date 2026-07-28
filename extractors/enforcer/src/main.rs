#![cfg_attr(feature = "strict", deny(warnings))]

use anyhow::Result;
use clap::Parser;
use enforcer_extractor::{Args, publish_initial_snapshot};

#[tokio::main]
async fn main() -> Result<()> {
    publish_initial_snapshot(Args::parse()).await
}
