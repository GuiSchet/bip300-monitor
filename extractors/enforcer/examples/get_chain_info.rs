use std::time::Duration;

use anyhow::{Context, Result};
use enforcer_extractor::EnforcerClient;

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50051".to_owned());
    let mut client = EnforcerClient::connect(&endpoint, Duration::from_secs(10))
        .await
        .with_context(|| format!("connecting probe to `{endpoint}`"))?;
    let chain_info = client.get_chain_info().await?;

    println!("{chain_info:#?}");
    Ok(())
}
