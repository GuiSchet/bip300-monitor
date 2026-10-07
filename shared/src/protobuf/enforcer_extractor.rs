//! Normalized events produced from the enforcer validator API.

include!(concat!(env!("OUT_DIR"), "/enforcer_extractor.rs"));

/// Version of the normalized enforcer event contract stored with every fact.
pub const EVENT_CONTRACT_VERSION: u32 = 9;

// These fingerprints deliberately live beside the version. Any edit to either
// protobuf contract makes the test below fail until the compatibility review
// records a new version/fingerprint pair here.
#[cfg(test)]
const EVENT_CONTRACT_V9_ENVELOPE_SHA256: &str =
    "71c39ffdf74e64b2729adf50cf4bf06440de75bdb75543f9eb27cd4a5c2eaf14";
#[cfg(test)]
const EVENT_CONTRACT_V9_PAYLOAD_SHA256: &str =
    "9d52c84437a3f9434b3c260afa4d9a0c1dc21c2e2ea919cca91efc7b36456589";

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
            Self::BmmRequests(_) => "bmm_requests",
            Self::MainchainTransition(_) => "mainchain_transition",
        }
    }

    /// The sidechain slot this event is scoped to, if any.
    pub const fn sidechain_number(&self) -> Option<u32> {
        match self {
            Self::ChainInfo(_)
            | Self::ChainTip(_)
            | Self::BmmRequests(_)
            | Self::MainchainTransition(_) => None,
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
    use sha2::{Digest as _, Sha256};

    use super::enforcer_event;

    #[test]
    fn contract_version_matches_the_reviewed_proto_fingerprints() {
        assert_eq!(super::EVENT_CONTRACT_VERSION, 9);
        assert_eq!(
            hex::encode(Sha256::digest(include_bytes!("../../../proto/event.proto"))),
            super::EVENT_CONTRACT_V9_ENVELOPE_SHA256,
            "event.proto changed: review compatibility and bump EVENT_CONTRACT_VERSION"
        );
        assert_eq!(
            hex::encode(Sha256::digest(include_bytes!(
                "../../../proto/enforcer_extractor.proto"
            ))),
            super::EVENT_CONTRACT_V9_PAYLOAD_SHA256,
            "enforcer_extractor.proto changed: review compatibility and bump EVENT_CONTRACT_VERSION"
        );
    }

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
            enforcer_event::Event::BmmRequests(super::BmmRequestsSnapshot::default()).kind(),
            enforcer_event::Event::MainchainTransition(super::MainchainTransition::default())
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
