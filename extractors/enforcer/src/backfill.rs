//! Recovery of the blocks that passed while the extractor was not watching.
//!
//! `SubscribeEvents` delivers from the moment of subscription and its request
//! carries no cursor, so a restart leaves a real hole that no transport choice
//! can close. The record is what says where to resume: because a row is
//! committed before anything is published, the checkpoint can never claim a
//! block that was not stored.

use anyhow::{Context, Result};
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
    /// Walk the range from this exclusive starting block up to the tip.
    Ranged { start_hash: Vec<u8> },
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

    Strategy::Ranged { start_hash: hash }
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
        Strategy::Ranged { start_hash } => {
            match ranged(
                client,
                recorder,
                sidechain,
                &hex::encode(&start_hash),
                &tip_hash,
            )
            .await
            {
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

async fn ranged(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    start_hash: &str,
    tip_hash: &str,
) -> Result<usize> {
    // The start is exclusive, so the checkpoint block itself is not re-fetched.
    let response = client
        .get_two_way_peg_data(sidechain, Some(start_hash.to_owned()), tip_hash)
        .await?;
    let payloads = convert::two_way_peg_data(sidechain, response)?;
    record(recorder, payloads).await
}

async fn bounded(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    sidechain: u8,
    tip_hash: &str,
    max_blocks: u32,
) -> Result<usize> {
    let response = client
        .get_block_info(tip_hash, sidechain, Some(max_blocks))
        .await?;
    let mut payloads = convert::block_info(sidechain, response)?;
    // `GetBlockInfo` answers newest-first; the record and live consumers both
    // read better in chain order.
    payloads.reverse();
    record(recorder, payloads).await
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
    use shared::protobuf::event::ObservedBlock;

    use super::{Reason, Strategy, choose};

    fn tip(hash: u8, height: u32) -> ObservedBlock {
        ObservedBlock::at_height(vec![hash; 32], height)
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
                start_hash: vec![0x11; 32]
            }
        );
        // Exactly at the bound is still a range: the bound is a maximum.
        assert_eq!(
            choose(Some((vec![0x11; 32], 100)), &tip(0x22, 2_100), 2_000),
            Strategy::Ranged {
                start_hash: vec![0x11; 32]
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
        assert_eq!(
            choose(Some((vec![0x11; 32], 500)), &tip(0x22, 100), 2_000),
            Strategy::Ranged {
                start_hash: vec![0x11; 32]
            },
            "the range walk is what discovers the checkpoint is not an ancestor"
        );
    }
}
