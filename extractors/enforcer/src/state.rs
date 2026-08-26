//! Enforcer state that changes as the mainchain advances.
//!
//! `GetSidechainProposals`, `GetSidechains`, `GetCtip` and
//! `GetWithdrawalBundleProposals` all answer "what is true right now": vote
//! counts accumulate, proposals age out, treasuries move. Reading them only at
//! startup freezes them, so they are re-read whenever a block moves the tip and
//! republished only when their value actually changed.

use anyhow::{Context, Result, bail};
use shared::protobuf::enforcer_extractor as events;

use crate::EnforcerClient;
use crate::convert;

/// Number of mutable-state payloads collected per configured sidechain slot.
const PAYLOADS_PER_SLOT: usize = 2;
/// Number of mutable-state payloads that are not scoped to a slot.
const GLOBAL_PAYLOADS: usize = 2;

/// Collect every mutable-state payload in a deterministic order.
///
/// The order is stable for a fixed slot list, which is what lets [`Tracker`]
/// diff two collections positionally.
pub(crate) async fn collect(
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

/// Extract the mainchain block hash a live block event refers to.
pub(crate) fn block_hash(payload: &events::EnforcerEvent) -> Result<&[u8]> {
    match payload
        .event
        .as_ref()
        .context("live event is missing its concrete event")?
    {
        events::enforcer_event::Event::BlockConnected(block) => Ok(&block
            .header
            .as_ref()
            .context("connected block is missing its header")?
            .hash),
        events::enforcer_event::Event::BlockDisconnected(block) => Ok(&block.block_hash),
        _ => bail!("a live event carried an unexpected payload for a block hash"),
    }
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;

    use super::{Tracker, block_hash};

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

    fn connected(hash: u8) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockConnected(
                events::BlockConnected {
                    header: Some(events::BlockHeader {
                        hash: vec![hash; 32],
                        previous_hash: vec![hash.wrapping_sub(1); 32],
                        height: 10,
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
    fn block_hashes_are_read_from_both_live_variants() {
        assert_eq!(block_hash(&connected(0x88)).expect("hash"), [0x88; 32]);

        let disconnected = events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockDisconnected(
                events::BlockDisconnected {
                    block_hash: vec![0x77; 32],
                    sidechain_number: 9,
                },
            )),
        };
        assert_eq!(block_hash(&disconnected).expect("hash"), [0x77; 32]);

        let unexpected = events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                sidechain_number: 9,
                ctip: None,
            })),
        };
        assert!(block_hash(&unexpected).is_err());
        assert!(block_hash(&events::EnforcerEvent { event: None }).is_err());
    }
}
