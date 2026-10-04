//! Independent, resumable enrichment. RPC failures never become zero fees.
use std::time::Duration;

use anyhow::{Context, Result};
use shared::{
    protobuf::event::{Event, event::MonitorEvent},
    recorder::Recorder,
    store::ExtractorWorker,
};
use tokio::sync::watch;

use crate::{EnforcerClient, convert};

pub(crate) async fn monitor(
    mut client: EnforcerClient,
    recorder: Recorder,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut delay = Duration::ZERO;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return Ok(()),
            _ = tokio::time::sleep(delay) => {},
        }
        let Some((source_id, block)) = recorder.store().next_fee_block().await? else {
            recorder
                .record_worker_success(ExtractorWorker::ConfirmedBmmFees)
                .await?;
            delay = Duration::from_secs(5);
            continue;
        };
        let result: Result<Event> = async {
            let payload =
                convert::confirmed_bmm_fees(client.get_confirmed_bmm_fees(&block.hash).await?)?;
            Ok(Event::new(MonitorEvent::Enforcer(payload), Some(block))?)
        }
        .await;
        match result {
            Ok(event) => {
                recorder
                    .record_fee_enrichment(source_id, event)
                    .await
                    .context("recording confirmed fee enrichment")?;
                recorder
                    .record_worker_success(ExtractorWorker::ConfirmedBmmFees)
                    .await?;
                // Keep historical enrichment from monopolizing node RPC capacity.
                delay = Duration::from_millis(100);
            }
            Err(error) => {
                recorder
                    .record_worker_failure(
                        ExtractorWorker::ConfirmedBmmFees,
                        &format!("{error:#}"),
                        1,
                    )
                    .await?;
                delay = Duration::from_secs(30);
            }
        }
    }
}
