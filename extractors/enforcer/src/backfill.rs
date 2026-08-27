//! Recovery of the blocks that passed while the extractor was not watching.
//!
//! `SubscribeEvents` delivers from the moment of subscription and its request
//! carries no cursor, so a restart leaves a real hole that no transport choice
//! can close. The record is what says where to resume: because a row is
//! committed before anything is published, the checkpoint can never claim a
//! block that was not stored.
//!
//! Recovery reads `GetBlockInfo` rather than `GetTwoWayPegData`. The latter
//! reads like the right call and is not: the enforcer drops every block whose
//! `BlockInfo` for the requested slot is empty, which on a quiet slot is almost
//! all of them, while `SubscribeEvents` reports every block. Walking a range
//! with it therefore records a subset of the gap and leaves the checkpoint
//! behind the tip, so the same gap is re-walked on every restart until it
//! outgrows the bound and is dropped. `GetBlockInfo` walks the same blocks
//! unfiltered, and the recovered chain is checked against the checkpoint before
//! anything is recorded, so a short or unrelated answer is a failure rather
//! than a silent hole.

use anyhow::{Context, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::ObservedBlock;
use shared::recorder::Recorder;

use crate::EnforcerClient;
use crate::convert;
use crate::event::envelope;
use crate::state;

/// Kind whose newest recorded block marks how far a slot was observed.
const CHECKPOINT_KIND: &str = "block_connected";

/// What a slot's backfill did, for logging and for tests.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    /// The record already held the tip.
    UpToDate,
    /// Blocks were recovered from a checkpoint by walking the range.
    Ranged { blocks: usize },
    /// A bounded window was recovered instead of a range, because there was no
    /// usable checkpoint to walk from.
    Bounded { blocks: usize, reason: Reason },
}

/// Why a bounded window was used rather than a range.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Reason {
    /// Nothing recorded for this slot yet.
    NoCheckpoint,
    /// The gap is larger than the configured bound.
    GapTooLarge { blocks: u32 },
    /// The range walk failed, which is what a reorg past the checkpoint looks
    /// like: the recorded block is no longer an ancestor of the tip.
    RangeUnavailable,
}

/// How a slot should be recovered, decided before any request is made.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Strategy {
    /// Nothing to do.
    UpToDate,
    /// Walk the range from this exclusive starting block up to the tip, which
    /// is `blocks` blocks further on.
    Ranged { start_hash: Vec<u8>, blocks: u32 },
    /// Ask for a bounded window ending at the tip.
    Bounded { reason: Reason },
}

/// Decide how far back a slot can be recovered from.
///
/// Kept free of I/O because this is where the mistakes are: asking without a
/// start walks to genesis, and asking for an unbounded range returns it in one
/// message.
pub(crate) fn choose(
    checkpoint: Option<(Vec<u8>, u32)>,
    tip: &ObservedBlock,
    max_blocks: u32,
) -> Strategy {
    let Some((hash, height)) = checkpoint else {
        return Strategy::Bounded {
            reason: Reason::NoCheckpoint,
        };
    };
    if hash == tip.hash {
        return Strategy::UpToDate;
    }

    // An anchor without a height cannot size the gap. Treating that as "small"
    // would risk the unbounded response this bound exists to prevent.
    let Some(tip_height) = tip.height else {
        return Strategy::Bounded {
            reason: Reason::GapTooLarge { blocks: max_blocks },
        };
    };
    let gap = tip_height.saturating_sub(height);
    if gap > max_blocks {
        return Strategy::Bounded {
            reason: Reason::GapTooLarge { blocks: gap },
        };
    }
    if gap == 0 {
        // A different hash at or below the tip height is a sibling, not an
        // ancestor: there is no range between them to walk. A reorg is the way
        // the record ends up holding one.
        return Strategy::Bounded {
            reason: Reason::RangeUnavailable,
        };
    }

    Strategy::Ranged {
        start_hash: hash,
        blocks: gap,
    }
}

/// Recover one slot up to `tip`, recording whatever was missed.
pub(crate) async fn run(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    tip: &ObservedBlock,
    max_blocks: u32,
) -> Result<Outcome> {
    let checkpoint = recorder
        .store()
        .last_recorded_block(CHECKPOINT_KIND, sidechain)
        .await
        .with_context(|| format!("reading the backfill checkpoint for sidechain {sidechain}"))?;

    let tip_hash = hex::encode(&tip.hash);
    let outcome = match choose(checkpoint, tip, max_blocks) {
        Strategy::UpToDate => Outcome::UpToDate,
        Strategy::Ranged { start_hash, blocks } => {
            match ranged(client, recorder, sidechain, &start_hash, &tip_hash, blocks).await {
                Ok(blocks) => Outcome::Ranged { blocks },
                Err(error) => {
                    tracing::warn!(
                        sidechain,
                        error = %format!("{error:#}"),
                        "could not walk the range from the checkpoint; \
                         falling back to a bounded window"
                    );
                    let blocks =
                        bounded(client, recorder, sidechain, &tip_hash, max_blocks).await?;
                    Outcome::Bounded {
                        blocks,
                        reason: Reason::RangeUnavailable,
                    }
                }
            }
        }
        Strategy::Bounded { reason } => {
            let blocks = bounded(client, recorder, sidechain, &tip_hash, max_blocks).await?;
            Outcome::Bounded { blocks, reason }
        }
    };

    log(sidechain, &outcome, max_blocks);
    Ok(outcome)
}

/// Recover exactly the `blocks` blocks between the checkpoint and the tip.
///
/// The checkpoint is exclusive, so the walk asks for the tip plus `blocks - 1`
/// ancestors. Whether it really got them is then checked rather than assumed:
/// the enforcer stops walking at the first ancestor whose block info it does not
/// hold, and returns a short answer instead of an error.
async fn ranged(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    start_hash: &[u8],
    tip_hash: &str,
    blocks: u32,
) -> Result<usize> {
    let ancestors = blocks - 1;
    let payloads = walk_back_from(client, sidechain, tip_hash, ancestors).await?;
    verify_range(&payloads, start_hash, blocks)?;
    record(recorder, payloads).await
}

/// Check that a recovered walk really is the range that was asked for.
///
/// Kept free of I/O because this is the assumption the old implementation made
/// silently: the enforcer can answer with fewer blocks than requested, or with a
/// chain that does not reach the checkpoint at all, and neither is an error on
/// the wire. Reporting it as one is what turns a hole into a fallback.
pub(crate) fn verify_range(
    payloads: &[events::EnforcerEvent],
    start_hash: &[u8],
    blocks: u32,
) -> Result<()> {
    let expected = blocks as usize;
    if payloads.len() != expected {
        bail!(
            "recovering {expected} blocks up to the tip returned {} of them",
            payloads.len()
        );
    }

    let oldest = connected_header(
        payloads
            .first()
            .context("a recovered range is never empty")?,
    )?;
    if oldest.previous_hash != start_hash {
        bail!(
            "the oldest recovered block {} follows {} rather than the checkpoint {}",
            hex::encode(&oldest.hash),
            hex::encode(&oldest.previous_hash),
            hex::encode(start_hash)
        );
    }

    // A gap that is contiguous end to end is the only one worth recording: a
    // break in the middle would leave the checkpoint valid while the record
    // skipped blocks under it.
    let mut previous = oldest;
    for payload in payloads.iter().skip(1) {
        let header = connected_header(payload)?;
        if header.previous_hash != previous.hash {
            bail!(
                "the recovered range breaks at {}, which follows {} rather than {}",
                hex::encode(&header.hash),
                hex::encode(&header.previous_hash),
                hex::encode(&previous.hash)
            );
        }
        previous = header;
    }

    Ok(())
}

async fn bounded(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    tip_hash: &str,
    max_blocks: u32,
) -> Result<usize> {
    // The tip counts against the bound, so it asks for one fewer ancestor.
    let payloads = walk_back_from(client, sidechain, tip_hash, max_blocks - 1).await?;
    record(recorder, payloads).await
}

/// Read the tip and `ancestors` of its ancestors, in chain order.
///
/// `GetBlockInfo` reports every walked block, including the ones with nothing in
/// them for this slot, which is what makes it the right call for closing a gap.
/// It answers newest-first; the record and live consumers both read better in
/// chain order.
async fn walk_back_from(
    client: &mut EnforcerClient,
    sidechain: u8,
    block_hash: &str,
    ancestors: u32,
) -> Result<Vec<events::EnforcerEvent>> {
    let response = client
        .get_block_info(block_hash, sidechain, Some(ancestors))
        .await?;
    let mut payloads = convert::block_info(sidechain, response)?;
    payloads.reverse();
    Ok(payloads)
}

/// The header of a recovered block, which is always a connected block.
fn connected_header(payload: &events::EnforcerEvent) -> Result<&events::BlockHeader> {
    let Some(events::enforcer_event::Event::BlockConnected(block)) = payload.event.as_ref() else {
        bail!("a recovered block carried an unexpected payload");
    };
    block
        .header
        .as_ref()
        .context("a recovered connected block is missing its header")
}

async fn record(recorder: &Recorder, payloads: Vec<events::EnforcerEvent>) -> Result<usize> {
    let blocks = payloads.len();
    let events = payloads
        .into_iter()
        .map(|payload| {
            let anchor = state::block_anchor(&payload)?;
            envelope(payload, anchor)
        })
        .collect::<Result<Vec<_>>>()?;
    recorder
        .record_batch(events)
        .await
        .context("recording backfilled blocks")?;

    Ok(blocks)
}

fn log(sidechain: u8, outcome: &Outcome, max_blocks: u32) {
    match outcome {
        Outcome::UpToDate => {
            tracing::info!(sidechain, "the record already holds the enforcer tip");
        }
        Outcome::Ranged { blocks } => {
            tracing::info!(sidechain, blocks, "backfilled the gap since the checkpoint");
        }
        Outcome::Bounded { blocks, reason } => match reason {
            // Zero is not the same story as "a window was recorded": the
            // enforcer had nothing to give, so the record still holds no block
            // for this slot and the next start will ask again.
            Reason::NoCheckpoint if *blocks == 0 => tracing::warn!(
                sidechain,
                max_blocks,
                "no checkpoint yet, and the bounded window came back empty; \
                 nothing was recovered for this slot"
            ),
            Reason::NoCheckpoint => tracing::info!(
                sidechain,
                blocks,
                max_blocks,
                "no checkpoint yet; recorded a bounded window ending at the tip"
            ),
            // Not a detail: the operator has to know history was skipped, or a
            // hole reads as "nothing happened".
            Reason::GapTooLarge { blocks: gap } => tracing::warn!(
                sidechain,
                blocks,
                max_blocks,
                gap,
                "the gap exceeds the backfill bound; \
                 the blocks before this window were not recovered"
            ),
            Reason::RangeUnavailable => tracing::warn!(
                sidechain,
                blocks,
                max_blocks,
                "the checkpoint is not an ancestor of the tip, which is what a reorg \
                 past it looks like; recorded a bounded window instead"
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::ObservedBlock;

    use super::{Reason, Strategy, choose, verify_range};

    fn tip(hash: u8, height: u32) -> ObservedBlock {
        ObservedBlock::at_height(vec![hash; 32], height)
    }

    /// One recovered block, linked to `previous_hash`.
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
    fn a_checkpoint_at_the_tip_needs_no_recovery() {
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x11, 100), 2_000),
            Strategy::UpToDate
        );
    }

    #[test]
    fn a_gap_within_the_bound_walks_the_range() {
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x22, 600), 2_000),
            Strategy::Ranged {
                start_hash: vec![0x11; 32],
                blocks: 500
            }
        );
        // Exactly at the bound is still a range: the bound is a maximum.
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x22, 2_100), 2_000),
            Strategy::Ranged {
                start_hash: vec![0x11; 32],
                blocks: 2_000
            }
        );
    }

    #[test]
    fn a_sibling_at_the_checkpoint_height_is_not_a_range() {
        // Same height, different hash: a reorg left the record holding a block
        // that is not an ancestor of the tip, and there is nothing between them
        // to walk.
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x22, 100), 2_000),
            Strategy::Bounded {
                reason: Reason::RangeUnavailable
            }
        );
    }

    #[test]
    fn a_gap_past_the_bound_takes_a_window_and_names_its_size() {
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x22, 2_101), 2_000),
            Strategy::Bounded {
                reason: Reason::GapTooLarge { blocks: 2_001 }
            }
        );
    }

    #[test]
    fn a_first_sight_is_bounded_rather_than_walked_to_genesis() {
        // Omitting the start makes the enforcer walk back to genesis, so an
        // empty record must never produce a range.
        assert_eq!(
            choose(None, &tip(0x22, 996_259), 2_000),
            Strategy::Bounded {
                reason: Reason::NoCheckpoint
            }
        );
    }

    #[test]
    fn an_unmeasurable_gap_is_bounded_rather_than_assumed_small() {
        assert_eq!(
            choose(
                Some((vec![0x11; 32], 100)),
                &ObservedBlock::without_height(vec![0x22; 32]),
                2_000
            ),
            Strategy::Bounded {
                reason: Reason::GapTooLarge { blocks: 2_000 }
            }
        );
    }

    #[test]
    fn a_checkpoint_ahead_of_the_tip_does_not_underflow_into_a_huge_gap() {
        // A reorg can leave the record holding a block above the current tip.
        // `saturating_sub` makes that a gap of zero, which is not a range: a
        // walk of zero blocks would report success without recovering anything.
        assert_eq!(
            choose(Some((vec![0x11; 32], 500)), &tip(0x22, 100), 2_000),
            Strategy::Bounded {
                reason: Reason::RangeUnavailable
            }
        );
    }

    #[test]
    fn a_contiguous_range_from_the_checkpoint_verifies() {
        let payloads = vec![
            recovered(0x12, 0x11, 101),
            recovered(0x13, 0x12, 102),
            recovered(0x14, 0x13, 103),
        ];
        verify_range(&payloads, &[0x11; 32], 3).expect("a contiguous range");
    }

    #[test]
    fn a_short_range_is_rejected_rather_than_recorded() {
        // What the enforcer answers when it stops walking early: fewer blocks
        // than asked for, and no error on the wire.
        let payloads = vec![recovered(0x13, 0x12, 102), recovered(0x14, 0x13, 103)];
        let error = verify_range(&payloads, &[0x11; 32], 3).expect_err("a short range must fail");
        assert!(
            format!("{error:#}").contains("returned 2 of them"),
            "the error must name what was missing, got: {error:#}"
        );
    }

    #[test]
    fn a_range_that_does_not_reach_the_checkpoint_is_rejected() {
        // The shape a reorg past the checkpoint leaves: the right number of
        // blocks, none of them descended from the recorded one.
        let payloads = vec![
            recovered(0x12, 0xaa, 101),
            recovered(0x13, 0x12, 102),
            recovered(0x14, 0x13, 103),
        ];
        let error =
            verify_range(&payloads, &[0x11; 32], 3).expect_err("an unlinked range must fail");
        assert!(
            format!("{error:#}").contains("rather than the checkpoint"),
            "the error must say the checkpoint was not reached, got: {error:#}"
        );
    }

    #[test]
    fn a_range_with_a_hole_in_the_middle_is_rejected() {
        let payloads = vec![
            recovered(0x12, 0x11, 101),
            recovered(0x14, 0x13, 103),
            recovered(0x15, 0x14, 104),
        ];
        let error = verify_range(&payloads, &[0x11; 32], 3).expect_err("a broken range must fail");
        assert!(
            format!("{error:#}").contains("breaks at"),
            "the error must locate the break, got: {error:#}"
        );
    }
}
