//! Bounded, resumable recovery of blocks observed before the extractor started.
//!
//! `GetBlockInfo` is unary and returns every requested ancestor in one message.
//! A total-history request would therefore make both the enforcer and this
//! process allocate the whole result. This module walks backwards in small
//! pages, commits each page with its cursor, and never publishes historical
//! rows to NATS.

use std::time::Duration;

use anyhow::{Context, Error, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::{Event, ObservedBlock};
use shared::recorder::Recorder;
use shared::store::{HistoryCoverage, HistoryPage, HistoryStatus};
use tokio::sync::watch;
use tonic::Code;

use crate::EnforcerClient;
use crate::convert;
use crate::event::envelope;
use crate::state;

const HISTORY_STREAM: &str = "block";

#[derive(Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) page_blocks: u32,
    pub(crate) page_pause: Duration,
}

/// Result of one pass toward a fixed tip.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    UpToDate,
    Completed { blocks: usize, pages: usize },
    Interrupted { blocks: usize, pages: usize },
}

/// Recover one slot from its activation height through `tip`.
///
/// A cursor already in `history_coverage` wins over `tip`, so a restart first
/// finishes the exact branch/page it had begun. The caller then reads the
/// current tip and invokes this again to reconcile blocks that arrived while
/// the earlier target was being filled.
pub(crate) async fn run(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    activation_height: u32,
    tip: &ObservedBlock,
    settings: Settings,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<Outcome> {
    let Some(mut progress) = prepare_cycle(
        recorder,
        sidechain,
        activation_height,
        tip,
        settings.page_blocks,
    )
    .await?
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
            .context("a running history cursor is missing its next block")?;
        let cursor_height = cursor
            .height
            .context("a running history cursor is missing its height")?;
        let remaining = blocks_through_floor(cursor_height, progress.floor_height)?;
        let requested = u32::try_from(remaining.min(u64::from(progress.effective_page_blocks)))
            .expect("a page size fits in a u32");

        let response = match client
            .get_block_info(hex::encode(&cursor.hash), sidechain, Some(requested - 1))
            .await
        {
            Ok(response) => response,
            Err(error) if error_has_code(&error, Code::NotFound) => {
                let current_tip = current_tip(client)
                    .await
                    .context("recovering from an unavailable historical cursor")?;
                if current_tip.hash == progress.target_tip.hash {
                    return fail(recorder, sidechain, error)
                        .await
                        .context("requesting a historical block page");
                }
                tracing::warn!(
                    sidechain,
                    unavailable_cursor = %hex::encode(&cursor.hash),
                    previous_target = %hex::encode(&progress.target_tip.hash),
                    current_target = %hex::encode(&current_tip.hash),
                    "historical cursor left the available branch; restarting this slot from activation"
                );
                progress = begin_full_cycle(
                    recorder,
                    sidechain,
                    activation_height,
                    &current_tip,
                    progress.effective_page_blocks,
                )
                .await?;
                restarted_from_activation = true;
                continue;
            }
            Err(error) if retryable_page_error(&error) && progress.effective_page_blocks > 1 => {
                let reduced = (progress.effective_page_blocks / 2).max(1);
                let message = format!("{error:#}");
                recorder
                    .store()
                    .resize_history_page(HISTORY_STREAM, Some(sidechain), reduced, &message)
                    .await
                    .context("persisting the reduced history page size")?;
                tracing::warn!(
                    sidechain,
                    previous_page_blocks = progress.effective_page_blocks,
                    effective_page_blocks = reduced,
                    error = %message,
                    "history request was too large; retrying the same cursor with a smaller page"
                );
                progress.effective_page_blocks = reduced;
                continue;
            }
            Err(error) => {
                return fail(recorder, sidechain, error)
                    .await
                    .context("requesting a historical block page");
            }
        };

        let payloads = match convert::block_info(sidechain, response)
            .and_then(|payloads| verify_page(&payloads, &cursor, requested).map(|()| payloads))
        {
            Ok(payloads) => payloads,
            Err(error) => {
                return fail(recorder, sidechain, error)
                    .await
                    .context("validating a historical block page");
            }
        };

        let oldest = connected_header(
            payloads
                .last()
                .context("a verified historical page is not empty")?,
        )?;
        let completes_cycle = u64::from(requested) == remaining;

        // Extending a previous continuous range is valid only if this branch
        // actually reaches its covered tip. A mismatch is a reorg: replay the
        // complete canonical history from activation instead of claiming a
        // joined range that does not exist.
        if completes_cycle
            && let Some(floor_hash) = progress.floor_hash.as_deref()
            && oldest.previous_hash != floor_hash
        {
            if restarted_from_activation {
                let error = anyhow::anyhow!(
                    "history branch still failed to reach its floor after restarting from activation"
                );
                return fail(recorder, sidechain, error).await;
            }
            tracing::warn!(
                sidechain,
                expected_floor_hash = %hex::encode(floor_hash),
                actual_floor_hash = %hex::encode(&oldest.previous_hash),
                "covered tip is not an ancestor of the target; restarting this slot from activation"
            );
            progress = begin_full_cycle(
                recorder,
                sidechain,
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
                    .context("a non-final history page cannot end at genesis")?,
            ))
        };
        let events = historical_events(payloads)?;
        let inserted = match recorder
            .store()
            .record_history_page(
                &events,
                HistoryPage {
                    stream: HISTORY_STREAM,
                    sidechain: Some(sidechain),
                    expected_next: &cursor,
                    next: next.as_ref(),
                },
            )
            .await
        {
            Ok(inserted) => inserted,
            Err(error) => {
                return fail(recorder, sidechain, error)
                    .await
                    .context("recording a historical block page");
            }
        };

        pages += 1;
        blocks += events.len();
        progress.rows_recorded += inserted;
        progress.next = next;
        tracing::debug!(
            sidechain,
            pages,
            blocks,
            inserted,
            cursor_height,
            target_height = ?progress.target_tip.height,
            "committed a historical block page"
        );

        if progress.next.is_none() {
            tracing::info!(
                sidechain,
                pages,
                blocks,
                rows_recorded = progress.rows_recorded,
                start_height = activation_height,
                target_height = ?progress.target_tip.height,
                "completed contiguous block history"
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
    sidechain: u8,
    activation_height: u32,
    tip: &ObservedBlock,
    configured_page_blocks: u32,
) -> Result<Option<HistoryCoverage>> {
    let tip_height = tip.height.context("backfill target tip has no height")?;
    if tip_height < activation_height {
        bail!(
            "sidechain {sidechain} activates at {activation_height}, after target tip {tip_height}"
        );
    }

    let existing = recorder
        .store()
        .history_coverage(HISTORY_STREAM, Some(sidechain))
        .await
        .with_context(|| format!("reading block history coverage for sidechain {sidechain}"))?;
    let Some(existing) = existing else {
        return begin_full_cycle(
            recorder,
            sidechain,
            activation_height,
            tip,
            configured_page_blocks,
        )
        .await
        .map(Some);
    };

    if existing.coverage_start_height != activation_height {
        bail!(
            "sidechain {sidechain} history starts at {}, but the enforcer now reports activation at {activation_height}",
            existing.coverage_start_height
        );
    }

    match existing.status {
        HistoryStatus::Running => Ok(Some(existing)),
        HistoryStatus::Error => {
            recorder
                .store()
                .resume_history(HISTORY_STREAM, Some(sidechain))
                .await
                .with_context(|| format!("resuming block history for sidechain {sidechain}"))?;
            let mut resumed = existing;
            resumed.status = HistoryStatus::Running;
            resumed.last_error = None;
            Ok(Some(resumed))
        }
        HistoryStatus::Complete => {
            let covered = existing
                .covered_tip
                .clone()
                .context("completed history coverage has no covered tip")?;
            if covered.hash == tip.hash {
                return Ok(None);
            }

            let page_blocks = existing.effective_page_blocks.min(configured_page_blocks);
            if tip_height > covered.height.context("covered tip has no height")? {
                recorder
                    .store()
                    .begin_history_cycle(
                        HISTORY_STREAM,
                        Some(sidechain),
                        activation_height,
                        Some(&covered),
                        tip,
                        Some(&covered.hash),
                        covered.height,
                        page_blocks,
                    )
                    .await
                    .with_context(|| format!("extending block history for sidechain {sidechain}"))
                    .map(Some)
            } else {
                // Same/lower height with another hash is necessarily a reorg.
                begin_full_cycle(recorder, sidechain, activation_height, tip, page_blocks)
                    .await
                    .map(Some)
            }
        }
    }
}

async fn begin_full_cycle(
    recorder: &Recorder,
    sidechain: u8,
    activation_height: u32,
    tip: &ObservedBlock,
    page_blocks: u32,
) -> Result<HistoryCoverage> {
    recorder
        .store()
        .begin_history_cycle(
            HISTORY_STREAM,
            Some(sidechain),
            activation_height,
            None,
            tip,
            None,
            activation_height.checked_sub(1),
            page_blocks,
        )
        .await
        .with_context(|| format!("starting full block history for sidechain {sidechain}"))
}

fn blocks_through_floor(cursor_height: u32, floor_height: Option<u32>) -> Result<u64> {
    match floor_height {
        Some(floor_height) => {
            let blocks = cursor_height.checked_sub(floor_height).with_context(|| {
                format!(
                    "history cursor {cursor_height} is below its exclusive floor {floor_height}"
                )
            })?;
            if blocks == 0 {
                bail!("history cursor is already at its exclusive floor {floor_height}");
            }
            Ok(u64::from(blocks))
        }
        None => Ok(u64::from(cursor_height) + 1),
    }
}

/// Validate an exact newest-first page before any of it is recorded.
pub(crate) fn verify_page(
    payloads: &[events::EnforcerEvent],
    cursor: &ObservedBlock,
    requested: u32,
) -> Result<()> {
    let expected = requested as usize;
    if payloads.len() != expected {
        bail!(
            "historical page requested {expected} blocks but returned {}",
            payloads.len()
        );
    }
    let cursor_height = cursor.height.context("history page cursor has no height")?;
    let newest = connected_header(
        payloads
            .first()
            .context("a requested history page cannot be empty")?,
    )?;
    if newest.hash != cursor.hash || newest.height != cursor_height {
        bail!(
            "historical page begins at {} height {}, expected {} height {}",
            hex::encode(&newest.hash),
            newest.height,
            hex::encode(&cursor.hash),
            cursor_height
        );
    }

    for pair in payloads.windows(2) {
        let newer = connected_header(&pair[0])?;
        let older = connected_header(&pair[1])?;
        if newer.previous_hash != older.hash || newer.height != older.height.saturating_add(1) {
            bail!(
                "historical page breaks between heights {} and {}",
                newer.height,
                older.height
            );
        }
    }

    let oldest = connected_header(payloads.last().expect("non-empty page"))?;
    let expected_oldest = cursor_height
        .checked_sub(requested - 1)
        .context("history page underflows genesis")?;
    if oldest.height != expected_oldest {
        bail!(
            "historical page ends at height {}, expected {expected_oldest}",
            oldest.height
        );
    }
    Ok(())
}

fn connected_header(payload: &events::EnforcerEvent) -> Result<&events::BlockHeader> {
    let Some(events::enforcer_event::Event::BlockConnected(block)) = payload.event.as_ref() else {
        bail!("a historical block carried an unexpected payload");
    };
    block
        .header
        .as_ref()
        .context("a historical connected block is missing its header")
}

fn historical_events(mut payloads: Vec<events::EnforcerEvent>) -> Result<Vec<Event>> {
    // The RPC is newest-first; durable reads and the original live stream are
    // easier to reason about in chain order.
    payloads.reverse();
    payloads
        .into_iter()
        .map(|payload| {
            let anchor = state::block_anchor(&payload)?;
            envelope(payload, anchor)
        })
        .collect()
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

async fn current_tip(client: &mut EnforcerClient) -> Result<ObservedBlock> {
    let payload = convert::chain_tip(client.get_chain_tip().await?)?;
    state::tip_anchor(&payload)
}

async fn fail<T>(recorder: &Recorder, sidechain: u8, error: Error) -> Result<T> {
    let message = format!("{error:#}");
    if let Err(mark_error) = recorder
        .store()
        .fail_history(HISTORY_STREAM, Some(sidechain), &message)
        .await
    {
        return Err(error).context(format!(
            "also failed to preserve the history error in Postgres: {mark_error:#}"
        ));
    }
    Err(error)
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::ObservedBlock;

    use tonic::Code;

    use super::{blocks_through_floor, error_has_code, retryable_page_error, verify_page};

    fn recovered(hash: u8, previous_hash: u8, height: u32) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockConnected(
                events::BlockConnected {
                    header: Some(events::BlockHeader {
                        hash: vec![hash; 32],
                        previous_hash: vec![previous_hash; 32],
                        height,
                        chain_work: vec![0x44; 32],
                        timestamp: 1_750_000_000,
                    }),
                    sidechain_number: 9,
                    bmm_commitment: None,
                    events: Vec::new(),
                },
            )),
        }
    }

    #[test]
    fn page_is_exact_and_contiguous_newest_first() {
        let payloads = vec![
            recovered(0x14, 0x13, 104),
            recovered(0x13, 0x12, 103),
            recovered(0x12, 0x11, 102),
        ];
        verify_page(&payloads, &ObservedBlock::at_height(vec![0x14; 32], 104), 3)
            .expect("valid page");
    }

    #[test]
    fn short_or_broken_pages_are_rejected() {
        let cursor = ObservedBlock::at_height(vec![0x14; 32], 104);
        let short = vec![recovered(0x14, 0x13, 104)];
        assert!(verify_page(&short, &cursor, 2).is_err());

        let broken = vec![recovered(0x14, 0xaa, 104), recovered(0x13, 0x12, 103)];
        assert!(verify_page(&broken, &cursor, 2).is_err());
    }

    #[test]
    fn page_must_begin_at_the_requested_cursor() {
        let payloads = vec![recovered(0x14, 0x13, 104)];
        assert!(verify_page(&payloads, &ObservedBlock::at_height(vec![0x99; 32], 104), 1).is_err());
    }

    #[test]
    fn floor_is_exclusive_and_genesis_can_be_included() {
        assert_eq!(blocks_through_floor(104, Some(100)).expect("gap"), 4);
        assert_eq!(blocks_through_floor(0, None).expect("genesis"), 1);
        assert!(blocks_through_floor(100, Some(100)).is_err());
        assert!(blocks_through_floor(99, Some(100)).is_err());
    }

    #[test]
    fn grpc_errors_are_classified_without_losing_their_context() {
        let exhausted = anyhow::Error::new(tonic::Status::resource_exhausted("large page"))
            .context("request failed");
        assert!(retryable_page_error(&exhausted));
        assert!(!error_has_code(&exhausted, Code::NotFound));

        let missing = anyhow::Error::new(tonic::Status::not_found("orphaned cursor"))
            .context("request failed");
        assert!(error_has_code(&missing, Code::NotFound));
        assert!(!retryable_page_error(&missing));
    }
}
