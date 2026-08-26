//! Normalized events produced from the enforcer validator API.

include!(concat!(env!("OUT_DIR"), "/enforcer_extractor.rs"));

impl enforcer_event::Event {
    /// Stable name of this event variant.
    ///
    /// One source of truth for the human-readable logger, the deployment
    /// verification queries, and the `kind` column of the record.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ChainInfo(_) => "chain_info",
            Self::ChainTip(_) => "chain_tip",
            Self::SidechainProposals(_) => "sidechain_proposals",
            Self::ActiveSidechains(_) => "active_sidechains",
            Self::Ctip(_) => "ctip",
            Self::BlockConnected(_) => "block_connected",
            Self::BlockDisconnected(_) => "block_disconnected",
            Self::WithdrawalBundleProposals(_) => "withdrawal_bundle_proposals",
        }
    }

    /// The sidechain slot this event is scoped to, if any.
    pub const fn sidechain_number(&self) -> Option<u32> {
        match self {
            Self::ChainInfo(_) | Self::ChainTip(_) => None,
            Self::SidechainProposals(_) | Self::ActiveSidechains(_) => None,
            Self::Ctip(snapshot) => Some(snapshot.sidechain_number),
            Self::BlockConnected(block) => Some(block.sidechain_number),
            Self::BlockDisconnected(block) => Some(block.sidechain_number),
            Self::WithdrawalBundleProposals(snapshot) => Some(snapshot.sidechain_number),
        }
    }
}

#[cfg(test)]
mod kind_tests {
    use super::enforcer_event;

    #[test]
    fn every_variant_has_a_distinct_kind() {
        // Any variant added without a kind fails to compile in `kind` itself;
        // this guards against two variants sharing a name.
        let kinds = [
            enforcer_event::Event::ChainInfo(super::ChainInfo::default()).kind(),
            enforcer_event::Event::ChainTip(super::ChainTip::default()).kind(),
            enforcer_event::Event::SidechainProposals(super::SidechainProposalsSnapshot::default())
                .kind(),
            enforcer_event::Event::ActiveSidechains(super::ActiveSidechainsSnapshot::default())
                .kind(),
            enforcer_event::Event::Ctip(super::CtipSnapshot::default()).kind(),
            enforcer_event::Event::BlockConnected(super::BlockConnected::default()).kind(),
            enforcer_event::Event::BlockDisconnected(super::BlockDisconnected::default()).kind(),
            enforcer_event::Event::WithdrawalBundleProposals(
                super::WithdrawalBundleProposalsSnapshot::default(),
            )
            .kind(),
        ];

        let unique = kinds.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), kinds.len(), "event kinds must be distinct");
    }

    #[test]
    fn only_slot_scoped_variants_report_a_sidechain() {
        assert_eq!(
            enforcer_event::Event::ChainInfo(super::ChainInfo::default()).sidechain_number(),
            None
        );
        assert_eq!(
            enforcer_event::Event::Ctip(super::CtipSnapshot {
                sidechain_number: 98,
                ctip: None,
            })
            .sidechain_number(),
            Some(98)
        );
    }
}
