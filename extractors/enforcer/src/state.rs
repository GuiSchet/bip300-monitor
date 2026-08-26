//! Enforcer state that changes as the mainchain advances.
//!
//! `GetSidechainProposals`, `GetSidechains`, `GetCtip` and
//! `GetWithdrawalBundleProposals` all answer "what is true right now": vote
//! counts accumulate, proposals age out, treasuries move. Reading them only at
//! startup freezes them, so they are re-read whenever a block moves the tip and
//! republished only when their value actually changed.

use anyhow::{Context, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::ObservedBlock;

use crate::EnforcerClient;
use crate::convert;

/// Number of mutable-state payloads collected per configured sidechain slot.
const PAYLOADS_PER_SLOT: usize = 2;
/// Number of mutable-state payloads that are not scoped to a slot.
const GLOBAL_PAYLOADS: usize = 2;

/// One reading of the mutable state, and the tip it is anchored to.
pub(crate) struct Reading {
    pub(crate) anchor: ObservedBlock,
    pub(crate) payloads: Vec<events::EnforcerEvent>,
}

/// Read the enforcer tip and then the mutable state anchored to it.
///
/// The tip is read first so the anchor never claims a block newer than the
/// state it labels.
pub(crate) async fn collect(client: &mut EnforcerClient, sidechains: &[u8]) -> Result<Reading> {
    let anchor = tip_anchor(&convert::chain_tip(client.get_chain_tip().await?)?)?;
    let payloads = collect_payloads(client, sidechains).await?;

    Ok(Reading { anchor, payloads })
}

/// Collect every mutable-state payload in a deterministic order.
///
/// The order is stable for a fixed slot list, which is what lets [`Tracker`]
/// diff two collections positionally.
pub(crate) async fn collect_payloads(
    client: &mut EnforcerClient,
    sidechains: &[u8],
) -> Result<Vec<events::EnforcerEvent>> {
    let mut payloads = Vec::with_capacity(GLOBAL_PAYLOADS + PAYLOADS_PER_SLOT * sidechains.len());
    payloads.push(convert::sidechain_proposals(
        client.get_sidechain_proposals().await?,
    )?);
    payloads.push(convert::active_sidechains(client.get_sidechains().await?)?);
    for sidechain in sidechains {
        payloads.push(convert::ctip(
            *sidechain,
            client.get_ctip(*sidechain).await?,
        )?);
        payloads.push(convert::withdrawal_bundle_proposals(
            *sidechain,
            client.get_withdrawal_bundle_proposals(*sidechain).await?,
        )?);
    }

    Ok(payloads)
}

/// Remembers the last published value of each mutable-state payload.
pub(crate) struct Tracker {
    last: Vec<events::EnforcerEvent>,
}

impl Tracker {
    /// Seed the tracker with the payloads published in the initial snapshot.
    pub(crate) const fn new(published: Vec<events::EnforcerEvent>) -> Self {
        Self { last: published }
    }

    /// Return the payloads that differ from the last published value, and
    /// record the new values as published.
    ///
    /// A length change means the collection itself was rebuilt differently, so
    /// everything is republished rather than diffed against the wrong slot.
    pub(crate) fn take_changed(
        &mut self,
        current: Vec<events::EnforcerEvent>,
    ) -> Result<Vec<events::EnforcerEvent>> {
        if current.len() != self.last.len() {
            bail!(
                "mutable state collection changed shape: expected {} payloads, got {}",
                self.last.len(),
                current.len()
            );
        }

        let changed = current
            .iter()
            .zip(self.last.iter())
            .filter(|(current, last)| current != last)
            .map(|(current, _)| current.clone())
            .collect();
        self.last = current;
        Ok(changed)
    }
}

/// Read the anchor a chain-tip event describes.
pub(crate) fn tip_anchor(payload: &events::EnforcerEvent) -> Result<ObservedBlock> {
    let Some(events::enforcer_event::Event::ChainTip(chain_tip)) = payload.event.as_ref() else {
        bail!("expected a chain-tip event while reading the enforcer tip");
    };
    let header = chain_tip
        .header
        .as_ref()
        .context("chain-tip event is missing its block header")?;

    Ok(ObservedBlock::at_height(header.hash.clone(), header.height))
}

/// Read the anchor a live block event describes.
///
/// A disconnect names only the block being disconnected, so its height is not
/// available here and has to be recovered from the connect that preceded it.
pub(crate) fn block_anchor(payload: &events::EnforcerEvent) -> Result<ObservedBlock> {
    match payload
        .event
        .as_ref()
        .context("live event is missing its concrete event")?
    {
        events::enforcer_event::Event::BlockConnected(block) => {
            let header = block
                .header
                .as_ref()
                .context("connected block is missing its header")?;
            Ok(ObservedBlock::at_height(header.hash.clone(), header.height))
        }
        events::enforcer_event::Event::BlockDisconnected(block) => {
            Ok(ObservedBlock::without_height(block.block_hash.clone()))
        }
        _ => bail!("a live event carried an unexpected payload for a block anchor"),
    }
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;

    use super::{Tracker, block_anchor, tip_anchor};

    fn ctip(sidechain_number: u32, value_sats: u64) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                sidechain_number,
                ctip: Some(events::Ctip {
                    txid: vec![0x11; 32],
                    vout: 0,
                    value_sats,
                    sequence_number: 1,
                }),
            })),
        }
    }

    fn header(hash: u8, height: u32) -> events::BlockHeader {
        events::BlockHeader {
            hash: vec![hash; 32],
            previous_hash: vec![hash.wrapping_sub(1); 32],
            height,
            chain_work: vec![0x44; 32],
            timestamp: 1_750_000_000,
        }
    }

    fn connected(hash: u8, height: u32) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockConnected(
                events::BlockConnected {
                    header: Some(header(hash, height)),
                    sidechain_number: 9,
                    bmm_commitment: None,
                    events: Vec::new(),
                },
            )),
        }
    }

    #[test]
    fn only_changed_payloads_are_republished() {
        let mut tracker = Tracker::new(vec![ctip(9, 100), ctip(98, 200)]);

        assert!(
            tracker
                .take_changed(vec![ctip(9, 100), ctip(98, 200)])
                .expect("same shape")
                .is_empty(),
            "an unchanged snapshot must not be republished"
        );

        let changed = tracker
            .take_changed(vec![ctip(9, 100), ctip(98, 250)])
            .expect("same shape");
        assert_eq!(changed, vec![ctip(98, 250)]);

        assert!(
            tracker
                .take_changed(vec![ctip(9, 100), ctip(98, 250)])
                .expect("same shape")
                .is_empty(),
            "the new value must be recorded as published"
        );
    }

    #[test]
    fn a_reverted_value_is_republished() {
        let mut tracker = Tracker::new(vec![ctip(9, 100)]);

        assert_eq!(
            tracker
                .take_changed(vec![ctip(9, 200)])
                .expect("same shape"),
            vec![ctip(9, 200)]
        );
        assert_eq!(
            tracker
                .take_changed(vec![ctip(9, 100)])
                .expect("same shape"),
            vec![ctip(9, 100)],
            "returning to an older value is still a change"
        );
    }

    #[test]
    fn a_shape_change_is_an_error_rather_than_a_wrong_diff() {
        let mut tracker = Tracker::new(vec![ctip(9, 100), ctip(98, 200)]);

        let error = tracker
            .take_changed(vec![ctip(9, 100)])
            .expect_err("a shorter collection must not be diffed positionally");
        assert!(error.to_string().contains("changed shape"));
    }

    #[test]
    fn anchors_carry_a_height_only_when_the_source_reports_one() {
        let anchor = block_anchor(&connected(0x88, 501)).expect("connected anchor");
        assert_eq!(anchor.hash, vec![0x88; 32]);
        assert_eq!(anchor.height, Some(501));

        let disconnected = events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockDisconnected(
                events::BlockDisconnected {
                    block_hash: vec![0x77; 32],
                    sidechain_number: 9,
                },
            )),
        };
        let anchor = block_anchor(&disconnected).expect("disconnected anchor");
        assert_eq!(anchor.hash, vec![0x77; 32]);
        assert_eq!(
            anchor.height, None,
            "a disconnect must not invent a height of zero"
        );

        let chain_tip = events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::ChainTip(events::ChainTip {
                header: Some(header(0x99, 996_259)),
            })),
        };
        let anchor = tip_anchor(&chain_tip).expect("tip anchor");
        assert_eq!(anchor.hash, vec![0x99; 32]);
        assert_eq!(anchor.height, Some(996_259));
    }

    #[test]
    fn anchors_reject_payloads_they_cannot_describe() {
        assert!(block_anchor(&ctip(9, 100)).is_err());
        assert!(block_anchor(&events::EnforcerEvent { event: None }).is_err());
        assert!(tip_anchor(&connected(0x88, 501)).is_err());
        assert!(
            tip_anchor(&events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::ChainTip(events::ChainTip {
                    header: None,
                })),
            })
            .is_err()
        );
    }
}
