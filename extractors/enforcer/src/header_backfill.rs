//! Slot-independent, bounded and resumable BIP300/301 mainchain history.

use anyhow::{Context, Error, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::{Event, ObservedBlock};
use shared::recorder::Recorder;
use shared::store::{HistoryCoverage, HistoryPage, HistoryStatus};
use tokio::sync::watch;
use tonic::Code;

use crate::EnforcerClient;
use crate::backfill::Settings;
use crate::convert;
use crate::event::envelope;

const HISTORY_STREAM: &str = "bip300_delta";

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    UpToDate,
    Completed { blocks: usize, pages: usize },
    Deferred { blocks: usize, pages: usize },
    Interrupted { blocks: usize, pages: usize },
}

/// Recover canonical BIP300/301 deltas from activation through a fixed tip.
/// Every delta carries its canonical header, including when no slot is active.
pub(crate) async fn run(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    activation_height: u32,
    tip: &ObservedBlock,
    settings: Settings,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<Outcome> {
    let Some(mut progress) =
        prepare_cycle(recorder, activation_height, tip, settings.page_blocks).await?
    else {
        return Ok(Outcome::UpToDate);
    };

    let mut blocks = 0_usize;
    let mut pages = 0_usize;
    let mut restarted_from_activation = progress.floor_hash.is_none();

    loop {
        if *shutdown_rx.borrow() {
            return Ok(Outcome::Interrupted { blocks, pages });
        }
        let cursor = progress
            .next
            .clone()
            .context("a running BIP300-history cursor is missing its next block")?;
        let cursor_height = required_height(&cursor, "BIP300-history cursor")?;
        let remaining = blocks_through_floor(cursor_height, progress.floor_height)?;
        let requested = u32::try_from(remaining.min(u64::from(progress.effective_page_blocks)))
            .expect("a page size fits in u32");

        let response = match client
            .get_bip300_block_delta(hex::encode(&cursor.hash), Some(requested - 1))
            .await
        {
            Ok(response) if response.deltas.is_empty() => {
                let error = anyhow::anyhow!(
                    "BIP300-history cursor {} was not found",
                    hex::encode(&cursor.hash)
                );
                defer(recorder, &error).await?;
                return Ok(Outcome::Deferred { blocks, pages });
            }
            Ok(response) => response,
            Err(error) if retryable_page_error(&error) && progress.effective_page_blocks > 1 => {
                let previous_page_blocks = progress.effective_page_blocks;
                let reduced = (progress.effective_page_blocks / 2).max(1);
                let message = format!("{error:#}");
                recorder
                    .store()
                    .resize_history_page(HISTORY_STREAM, None, reduced, &message)
                    .await
                    .context("persisting the reduced BIP300-history page size")?;
                progress.effective_page_blocks = reduced;
                tracing::warn!(
                    previous_page_blocks,
                    effective_page_blocks = reduced,
                    error = %message,
                    "BIP300-history request was too large; retrying"
                );
                continue;
            }
            Err(error)
                if error_has_code(&error, Code::NotFound)
                    || error_has_code(&error, Code::Unimplemented) =>
            {
                defer(recorder, &error).await?;
                return Ok(Outcome::Deferred { blocks, pages });
            }
            Err(error) => return fail(recorder, error).await,
        };

        let payloads = match convert::bip300_block_deltas(response)
            .and_then(|payloads| verify_page(&payloads, &cursor, requested).map(|()| payloads))
        {
            Ok(payloads) => payloads,
            Err(error) => return fail(recorder, error).await,
        };
        let oldest = header(
            payloads
                .last()
                .context("a verified BIP300 page is not empty")?,
        )?;
        let returned = u32::try_from(payloads.len()).expect("a page length fits in u32");
        let completes_cycle = u64::from(returned) == remaining;

        if completes_cycle
            && let Some(floor_hash) = progress.floor_hash.as_deref()
            && oldest.previous_hash != floor_hash
        {
            if restarted_from_activation {
                return fail(
                    recorder,
                    anyhow::anyhow!(
                        "BIP300 history still failed to reach its floor after a full restart"
                    ),
                )
                .await;
            }
            tracing::warn!(
                expected_floor_hash = %hex::encode(floor_hash),
                actual_floor_hash = %hex::encode(&oldest.previous_hash),
                "BIP300-history target is on another branch; restarting from activation"
            );
            progress = begin_full_cycle(
                recorder,
                activation_height,
                &progress.target_tip,
                progress.effective_page_blocks,
            )
            .await?;
            restarted_from_activation = true;
            continue;
        }

        let next = if completes_cycle {
            None
        } else {
            Some(ObservedBlock::at_height(
                oldest.previous_hash.clone(),
                oldest
                    .height
                    .checked_sub(1)
                    .context("a non-final BIP300 page cannot end at genesis")?,
            ))
        };
        let events = historical_events(payloads)?;
        let inserted = recorder
            .store()
            .record_history_page(
                &events,
                HistoryPage {
                    stream: HISTORY_STREAM,
                    sidechain: None,
                    expected_next: &cursor,
                    next: next.as_ref(),
                },
            )
            .await
            .context("recording a historical BIP300 page")?;

        blocks += events.len();
        pages += 1;
        progress.rows_recorded += inserted;
        progress.next = next;
        if progress.next.is_none() {
            tracing::info!(
                pages,
                blocks,
                rows_recorded = progress.rows_recorded,
                start_height = activation_height,
                target_height = ?progress.target_tip.height,
                "completed contiguous global BIP300/301 history"
            );
            return Ok(Outcome::Completed { blocks, pages });
        }

        if !settings.page_pause.is_zero() {
            tokio::select! {
                biased;
                () = wait_for_shutdown(&mut shutdown_rx) => {
                    return Ok(Outcome::Interrupted { blocks, pages });
                }
                () = tokio::time::sleep(settings.page_pause) => {}
            }
        }
    }
}

async fn prepare_cycle(
    recorder: &Recorder,
    activation_height: u32,
    tip: &ObservedBlock,
    configured_page_blocks: u32,
) -> Result<Option<HistoryCoverage>> {
    let tip_height = required_height(tip, "BIP300-history target")?;
    if tip_height < activation_height {
        bail!("activation height {activation_height} is above target tip {tip_height}");
    }
    let existing = recorder
        .store()
        .history_coverage(HISTORY_STREAM, None)
        .await
        .context("reading global BIP300-history coverage")?;
    let Some(existing) = existing else {
        return begin_full_cycle(recorder, activation_height, tip, configured_page_blocks)
            .await
            .map(Some);
    };
    if existing.coverage_start_height != activation_height {
        bail!(
            "BIP300 history starts at {}, expected {activation_height}",
            existing.coverage_start_height
        );
    }

    match existing.status {
        HistoryStatus::Running => Ok(Some(existing)),
        HistoryStatus::Error if existing.target_tip.hash != tip.hash => begin_full_cycle(
            recorder,
            activation_height,
            tip,
            existing.effective_page_blocks.min(configured_page_blocks),
        )
        .await
        .map(Some),
        HistoryStatus::Error => {
            recorder
                .store()
                .resume_history(HISTORY_STREAM, None)
                .await
                .context("resuming global BIP300 history")?;
            let mut resumed = existing;
            resumed.status = HistoryStatus::Running;
            resumed.last_error = None;
            Ok(Some(resumed))
        }
        HistoryStatus::Complete => {
            let covered = existing
                .covered_tip
                .clone()
                .context("completed BIP300 history has no covered tip")?;
            if covered.hash == tip.hash {
                return Ok(None);
            }
            let page_blocks = existing.effective_page_blocks.min(configured_page_blocks);
            if tip_height > required_height(&covered, "covered BIP300 tip")? {
                recorder
                    .store()
                    .begin_history_cycle(
                        HISTORY_STREAM,
                        None,
                        activation_height,
                        Some(&covered),
                        tip,
                        Some(&covered.hash),
                        covered.height,
                        page_blocks,
                    )
                    .await
                    .context("extending global BIP300 history")
                    .map(Some)
            } else {
                begin_full_cycle(recorder, activation_height, tip, page_blocks)
                    .await
                    .map(Some)
            }
        }
    }
}

async fn begin_full_cycle(
    recorder: &Recorder,
    activation_height: u32,
    tip: &ObservedBlock,
    page_blocks: u32,
) -> Result<HistoryCoverage> {
    recorder
        .store()
        .begin_history_cycle(
            HISTORY_STREAM,
            None,
            activation_height,
            None,
            tip,
            None,
            activation_height.checked_sub(1),
            page_blocks,
        )
        .await
        .context("starting full global BIP300 history")
}

pub(crate) fn verify_page(
    payloads: &[events::EnforcerEvent],
    cursor: &ObservedBlock,
    requested: u32,
) -> Result<()> {
    if requested == 0 || payloads.is_empty() {
        bail!("a BIP300 page must request and return at least one block");
    }
    if payloads.len() > requested as usize {
        bail!("BIP300 page returned more blocks than requested");
    }
    let cursor_height = required_height(cursor, "BIP300 page cursor")?;
    let newest = header(&payloads[0])?;
    if newest.hash != cursor.hash || newest.height != cursor_height {
        bail!("BIP300 page does not begin at its requested cursor");
    }
    for pair in payloads.windows(2) {
        let newer = header(&pair[0])?;
        let older = header(&pair[1])?;
        if newer.previous_hash != older.hash || newer.height != older.height.saturating_add(1) {
            bail!("BIP300 page is not contiguous at height {}", newer.height);
        }
    }
    let oldest = header(payloads.last().expect("non-empty header page"))?;
    let returned = u32::try_from(payloads.len()).expect("page length fits in u32");
    let expected_oldest = cursor_height
        .checked_sub(returned - 1)
        .context("BIP300 page underflows genesis")?;
    if oldest.height != expected_oldest {
        bail!("BIP300 page ends at an unexpected height");
    }
    Ok(())
}

fn header(payload: &events::EnforcerEvent) -> Result<&events::BlockHeader> {
    let Some(events::enforcer_event::Event::Bip300BlockDelta(block)) = payload.event.as_ref()
    else {
        bail!("BIP300 history contains a non-delta payload");
    };
    block
        .header
        .as_ref()
        .context("BIP300 block delta is missing its header")
}

fn historical_events(mut payloads: Vec<events::EnforcerEvent>) -> Result<Vec<Event>> {
    payloads.reverse();
    payloads
        .into_iter()
        .map(|payload| {
            let anchor = header(&payload)?;
            let anchor = ObservedBlock::at_height(anchor.hash.clone(), anchor.height);
            envelope(payload, anchor)
        })
        .collect()
}

fn required_height(block: &ObservedBlock, name: &str) -> Result<u32> {
    block
        .height
        .with_context(|| format!("{name} has no height"))
}

fn blocks_through_floor(cursor_height: u32, floor_height: Option<u32>) -> Result<u64> {
    match floor_height {
        Some(floor) => cursor_height
            .checked_sub(floor)
            .filter(|blocks| *blocks > 0)
            .map(u64::from)
            .with_context(|| format!("BIP300 cursor {cursor_height} is not above floor {floor}")),
        None => Ok(u64::from(cursor_height) + 1),
    }
}

fn retryable_page_error(error: &Error) -> bool {
    error_has_code(error, Code::DeadlineExceeded) || error_has_code(error, Code::ResourceExhausted)
}

fn error_has_code(error: &Error, code: Code) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == code)
    })
}

async fn fail<T>(recorder: &Recorder, error: Error) -> Result<T> {
    let message = format!("{error:#}");
    recorder
        .store()
        .fail_history(HISTORY_STREAM, None, &message)
        .await
        .context("preserving the global BIP300-history failure")?;
    Err(error)
}

async fn defer(recorder: &Recorder, error: &Error) -> Result<()> {
    recorder
        .store()
        .fail_history(HISTORY_STREAM, None, &format!("{error:#}"))
        .await
        .context("preserving the deferred global BIP300-history cursor")
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() || shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::ObservedBlock;

    use super::verify_page;

    fn recovered(hash: u8, previous_hash: u8, height: u32) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::Bip300BlockDelta(
                events::Bip300BlockDelta {
                    header: Some(events::BlockHeader {
                        hash: vec![hash; 32],
                        previous_hash: vec![previous_hash; 32],
                        height,
                        chain_work: vec![0x44; 32],
                        timestamp: 1_750_000_000,
                    }),
                    coinbase_txid: vec![0x55; 32],
                    coinbase_messages: Vec::new(),
                    treasury_transitions: Vec::new(),
                    confirmed_bmm_requests: Vec::new(),
                },
            )),
        }
    }

    #[test]
    fn global_bip300_pages_are_exact_and_contiguous() {
        let payloads = vec![
            recovered(0x14, 0x13, 104),
            recovered(0x13, 0x12, 103),
            recovered(0x12, 0x11, 102),
        ];
        verify_page(&payloads, &ObservedBlock::at_height(vec![0x14; 32], 104), 3)
            .expect("valid global BIP300 page");
        assert!(
            verify_page(
                &[recovered(0x14, 0xaa, 104), recovered(0x13, 0x12, 103)],
                &ObservedBlock::at_height(vec![0x14; 32], 104),
                2,
            )
            .is_err()
        );
    }
}
