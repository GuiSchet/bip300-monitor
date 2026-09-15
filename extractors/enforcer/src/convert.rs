//! Fallible conversions from the upstream validator API to monitor events.

use anyhow::{Context, Result, bail};
use shared::protobuf::enforcer_extractor as events;

use crate::proto::{common, mainchain};

/// Convert `GetChainInfo` into a normalized monitor event.
pub fn chain_info(response: mainchain::GetChainInfoResponse) -> Result<events::EnforcerEvent> {
    let constants = required(response.bip300_constants, "chain_info.bip300_constants")?;
    let raw_network = response.network;
    let chain_info = events::ChainInfo {
        network: network(raw_network) as i32,
        bip300_constants: Some(events::Bip300Constants {
            withdrawal_bundle_max_age: constants.withdrawal_bundle_max_age,
            withdrawal_bundle_inclusion_threshold: constants.withdrawal_bundle_inclusion_threshold,
            used_sidechain_slot_proposal_max_age: constants.used_sidechain_slot_proposal_max_age,
            used_sidechain_slot_activation_threshold: constants
                .used_sidechain_slot_activation_threshold,
            unused_sidechain_slot_proposal_max_age: constants
                .unused_sidechain_slot_proposal_max_age,
            unused_sidechain_slot_activation_threshold: constants
                .unused_sidechain_slot_activation_threshold,
            activation_height: constants.activation_height,
        }),
        raw_network,
    };

    Ok(enforcer_event(events::enforcer_event::Event::ChainInfo(
        chain_info,
    )))
}

/// Convert `GetChainTip` into a normalized monitor event.
pub fn chain_tip(response: mainchain::GetChainTipResponse) -> Result<events::EnforcerEvent> {
    let header = block_header(required(
        response.block_header_info,
        "chain_tip.block_header_info",
    )?)?;

    Ok(enforcer_event(events::enforcer_event::Event::ChainTip(
        events::ChainTip {
            header: Some(header),
        },
    )))
}

/// Convert `GetBlockInfo` results, preserving the upstream newest-first order.
pub fn block_info(
    sidechain_number: u8,
    response: mainchain::GetBlockInfoResponse,
) -> Result<Vec<events::EnforcerEvent>> {
    response
        .infos
        .into_iter()
        .enumerate()
        .map(|(index, info)| {
            let header = required(info.header_info, "block_info.infos[].header_info")?;
            let info = required(info.block_info, "block_info.infos[].block_info")?;
            connected_block(sidechain_number, header, info)
                .with_context(|| format!("converting block info at index {index}"))
        })
        .collect()
}

/// Convert observer-oriented BIP300/301 block deltas, preserving the
/// upstream newest-first order and every raw enum number.
pub fn bip300_block_deltas(
    response: mainchain::GetBip300BlockDeltaResponse,
) -> Result<Vec<events::EnforcerEvent>> {
    response
        .deltas
        .into_iter()
        .enumerate()
        .map(|(index, delta)| {
            bip300_block_delta(delta)
                .map(|delta| enforcer_event(events::enforcer_event::Event::Bip300BlockDelta(delta)))
                .with_context(|| format!("converting BIP300 block delta at index {index}"))
        })
        .collect()
}

fn bip300_block_delta(delta: mainchain::Bip300BlockDelta) -> Result<events::Bip300BlockDelta> {
    Ok(events::Bip300BlockDelta {
        header: Some(block_header(required(
            delta.header_info,
            "bip300_block_delta.header_info",
        )?)?),
        coinbase_txid: hash_from_reverse(delta.coinbase_txid, "bip300_block_delta.coinbase_txid")?,
        coinbase_messages: delta
            .coinbase_messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                bip300_coinbase_message(message)
                    .with_context(|| format!("converting BIP300 coinbase message at index {index}"))
            })
            .collect::<Result<Vec<_>>>()?,
        treasury_transitions: delta
            .treasury_transitions
            .into_iter()
            .enumerate()
            .map(|(index, transition)| {
                treasury_transition(transition)
                    .with_context(|| format!("converting treasury transition at index {index}"))
            })
            .collect::<Result<Vec<_>>>()?,
        confirmed_bmm_requests: delta
            .confirmed_bmm_requests
            .into_iter()
            .enumerate()
            .map(|(index, request)| {
                confirmed_bmm_request(request)
                    .with_context(|| format!("converting confirmed M8 at index {index}"))
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn bip300_coinbase_message(
    message: mainchain::Bip300CoinbaseMessage,
) -> Result<events::Bip300CoinbaseMessage> {
    let normalized = match required(message.message, "bip300_coinbase_message.message")? {
        mainchain::bip300_coinbase_message::Message::M1(m1) => {
            events::bip300_coinbase_message::Message::M1(events::M1Delta {
                sidechain_number: m1.sidechain_number,
                description: consensus_hex(m1.description, "bip300_m1.description")?,
                description_hash: hash_from_reverse(
                    m1.description_sha256d_hash,
                    "bip300_m1.description_sha256d_hash",
                )?,
            })
        }
        mainchain::bip300_coinbase_message::Message::M2(m2) => {
            events::bip300_coinbase_message::Message::M2(events::M2Delta {
                sidechain_number: m2.sidechain_number,
                description_hash: hash_from_reverse(
                    m2.description_sha256d_hash,
                    "bip300_m2.description_sha256d_hash",
                )?,
                effect: m2.effect,
            })
        }
        mainchain::bip300_coinbase_message::Message::M3(m3) => {
            events::bip300_coinbase_message::Message::M3(events::M3Delta {
                sidechain_number: m3.sidechain_number,
                m6id: consensus_hex(m3.m6id, "bip300_m3.m6id")?,
            })
        }
        mainchain::bip300_coinbase_message::Message::M4(m4) => {
            events::bip300_coinbase_message::Message::M4(events::M4Delta {
                mode: m4.mode,
                raw_votes: m4.raw_votes,
                effects: m4
                    .effects
                    .into_iter()
                    .map(|effect| {
                        Ok(events::M4Effect {
                            sidechain_number: effect.sidechain_number,
                            action: effect.action,
                            upvoted_m6id: effect
                                .upvoted_m6id
                                .map(|value| consensus_hex(Some(value), "bip300_m4.upvoted_m6id"))
                                .transpose()?,
                            downvoted_m6ids: effect
                                .downvoted_m6ids
                                .into_iter()
                                .map(|value| consensus_hex(Some(value), "bip300_m4.downvoted_m6id"))
                                .collect::<Result<Vec<_>>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        mainchain::bip300_coinbase_message::Message::M7(m7) => {
            events::bip300_coinbase_message::Message::M7(events::M7Delta {
                sidechain_number: m7.sidechain_number,
                hstar: consensus_hex(m7.hstar, "bip300_m7.hstar")?,
            })
        }
    };
    Ok(events::Bip300CoinbaseMessage {
        vout: message.vout,
        raw_script_pubkey: raw_hex(
            message.raw_script_pubkey,
            "bip300_coinbase_message.raw_script_pubkey",
        )?,
        accepted: message.accepted,
        message: Some(normalized),
    })
}

fn treasury_transition(
    transition: mainchain::TreasuryTransition,
) -> Result<events::TreasuryTransition> {
    Ok(events::TreasuryTransition {
        kind: transition.kind,
        sidechain_number: transition.sidechain_number,
        previous_ctip: transition
            .previous_ctip
            .map(treasury_delta_ctip)
            .transpose()?,
        new_ctip: transition.new_ctip.map(treasury_delta_ctip).transpose()?,
        sequence_number: transition.sequence_number,
        delta_sats: transition.delta_sats,
        payout_sats: transition.payout_sats,
        fee_sats: transition.fee_sats,
        m6id: transition
            .m6id
            .map(|value| consensus_hex(Some(value), "treasury_transition.m6id"))
            .transpose()?,
        sidechain_address: transition
            .sidechain_address
            .map(|value| raw_hex(Some(value), "treasury_transition.sidechain_address"))
            .transpose()?,
        transaction: transition
            .transaction
            .map(|value| consensus_hex(Some(value), "treasury_transition.transaction"))
            .transpose()?,
        proposal_height: transition.proposal_height,
        terminal_height: transition.terminal_height,
    })
}

fn treasury_delta_ctip(ctip: mainchain::TreasuryCtip) -> Result<events::TreasuryCtip> {
    Ok(events::TreasuryCtip {
        txid: hash_from_reverse(ctip.txid, "treasury_ctip.txid")?,
        vout: ctip.vout,
        value_sats: ctip.value_sats,
    })
}

fn confirmed_bmm_request(
    request: mainchain::ConfirmedBmmRequest,
) -> Result<events::ConfirmedBmmRequest> {
    Ok(events::ConfirmedBmmRequest {
        sidechain_number: request.sidechain_number,
        txid: hash_from_reverse(request.txid, "confirmed_bmm_request.txid")?,
        transaction: consensus_hex(request.transaction, "confirmed_bmm_request.transaction")?,
        hstar: consensus_hex(request.hstar, "confirmed_bmm_request.hstar")?,
        previous_mainchain_block_hash: hash_from_reverse(
            request.previous_mainchain_block_hash,
            "confirmed_bmm_request.previous_mainchain_block_hash",
        )?,
        fee_sats: request.fee_sats,
    })
}

/// Convert `GetSidechainProposals` into a snapshot event.
pub fn sidechain_proposals(
    response: mainchain::GetSidechainProposalsResponse,
) -> Result<events::EnforcerEvent> {
    let proposals = response
        .sidechain_proposals
        .into_iter()
        .enumerate()
        .map(|(index, proposal)| {
            sidechain_proposal(proposal)
                .with_context(|| format!("converting sidechain proposal at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(enforcer_event(
        events::enforcer_event::Event::SidechainProposals(events::SidechainProposalsSnapshot {
            proposals,
        }),
    ))
}

/// Convert `GetSidechains` into an active-sidechains snapshot event.
pub fn active_sidechains(
    response: mainchain::GetSidechainsResponse,
) -> Result<events::EnforcerEvent> {
    let sidechains = response
        .sidechains
        .into_iter()
        .enumerate()
        .map(|(index, sidechain)| {
            active_sidechain(sidechain)
                .with_context(|| format!("converting active sidechain at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(enforcer_event(
        events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
            sidechains,
        }),
    ))
}

/// Convert `GetCtip` into a snapshot scoped to the requested sidechain slot.
pub fn ctip(
    sidechain_number: u8,
    response: mainchain::GetCtipResponse,
) -> Result<events::EnforcerEvent> {
    let ctip = response.ctip.map(ctip_value).transpose()?;

    Ok(enforcer_event(events::enforcer_event::Event::Ctip(
        events::CtipSnapshot {
            sidechain_number: u32::from(sidechain_number),
            ctip,
        },
    )))
}

/// Convert `GetWithdrawalBundleProposals` into a snapshot scoped to one slot.
pub fn withdrawal_bundle_proposals(
    sidechain_number: u8,
    response: mainchain::GetWithdrawalBundleProposalsResponse,
) -> Result<events::EnforcerEvent> {
    let proposals = response
        .proposals
        .into_iter()
        .enumerate()
        .map(|(index, proposal)| {
            withdrawal_bundle_proposal(proposal)
                .with_context(|| format!("converting withdrawal bundle proposal at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(enforcer_event(
        events::enforcer_event::Event::WithdrawalBundleProposals(
            events::WithdrawalBundleProposalsSnapshot {
                sidechain_number: u32::from(sidechain_number),
                proposals,
            },
        ),
    ))
}

/// Convert one live subscription item into a sidechain-scoped monitor event.
pub fn subscription_event(
    sidechain_number: u8,
    response: mainchain::SubscribeEventsResponse,
) -> Result<events::EnforcerEvent> {
    let event = required(response.event, "subscribe_events.event")?;
    match required(event.event, "subscribe_events.event.event")? {
        mainchain::subscribe_events_response::event::Event::ConnectBlock(block) => {
            let header = required(block.header_info, "connect_block.header_info")?;
            let info = required(block.block_info, "connect_block.block_info")?;
            connected_block(sidechain_number, header, info)
        }
        mainchain::subscribe_events_response::event::Event::DisconnectBlock(block) => {
            let block_hash = hash_from_reverse(block.block_hash, "disconnect_block.block_hash")?;
            Ok(enforcer_event(
                events::enforcer_event::Event::BlockDisconnected(events::BlockDisconnected {
                    block_hash,
                    sidechain_number: u32::from(sidechain_number),
                }),
            ))
        }
    }
}

fn connected_block(
    sidechain_number: u8,
    header: mainchain::BlockHeaderInfo,
    info: mainchain::BlockInfo,
) -> Result<events::EnforcerEvent> {
    let bmm_commitment = info
        .bmm_commitment
        .map(|value| consensus_hex(Some(value), "block_info.bmm_commitment"))
        .transpose()?;
    let sidechain_events = info
        .events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            sidechain_event(event)
                .with_context(|| format!("converting sidechain event at index {index}"))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(enforcer_event(
        events::enforcer_event::Event::BlockConnected(events::BlockConnected {
            header: Some(block_header(header)?),
            sidechain_number: u32::from(sidechain_number),
            bmm_commitment,
            events: sidechain_events,
        }),
    ))
}

fn block_header(header: mainchain::BlockHeaderInfo) -> Result<events::BlockHeader> {
    Ok(events::BlockHeader {
        hash: hash_from_reverse(header.block_hash, "block_header.block_hash")?,
        previous_hash: hash_from_reverse(header.prev_block_hash, "block_header.prev_block_hash")?,
        height: header.height,
        chain_work: fixed_consensus_hex(header.work, "block_header.work")?,
        timestamp: header.timestamp,
    })
}

fn sidechain_proposal(
    proposal: mainchain::get_sidechain_proposals_response::SidechainProposal,
) -> Result<events::SidechainProposal> {
    Ok(events::SidechainProposal {
        sidechain_number: required(
            proposal.sidechain_number,
            "sidechain_proposal.sidechain_number",
        )?,
        raw_description: consensus_hex(proposal.description, "sidechain_proposal.description")?,
        description_hash: hash_from_reverse(
            proposal.description_sha256d_hash,
            "sidechain_proposal.description_sha256d_hash",
        )?,
        vote_count: required(proposal.vote_count, "sidechain_proposal.vote_count")?,
        proposal_height: required(
            proposal.proposal_height,
            "sidechain_proposal.proposal_height",
        )?,
        proposal_age: required(proposal.proposal_age, "sidechain_proposal.proposal_age")?,
        declaration: proposal
            .declaration
            .map(sidechain_declaration)
            .transpose()?,
    })
}

fn active_sidechain(
    sidechain: mainchain::get_sidechains_response::SidechainInfo,
) -> Result<events::ActiveSidechain> {
    Ok(events::ActiveSidechain {
        sidechain_number: required(
            sidechain.sidechain_number,
            "active_sidechain.sidechain_number",
        )?,
        raw_description: consensus_hex(sidechain.description, "active_sidechain.description")?,
        vote_count: required(sidechain.vote_count, "active_sidechain.vote_count")?,
        proposal_height: required(
            sidechain.proposal_height,
            "active_sidechain.proposal_height",
        )?,
        activation_height: required(
            sidechain.activation_height,
            "active_sidechain.activation_height",
        )?,
        declaration: sidechain
            .declaration
            .map(sidechain_declaration)
            .transpose()?,
    })
}

fn sidechain_declaration(
    declaration: mainchain::SidechainDeclaration,
) -> Result<events::SidechainDeclaration> {
    let declaration = required(
        declaration.sidechain_declaration,
        "sidechain_declaration.sidechain_declaration",
    )?;
    let declaration = match declaration {
        mainchain::sidechain_declaration::SidechainDeclaration::V0(v0) => {
            events::sidechain_declaration::Declaration::V0(events::SidechainDeclarationV0 {
                title: required(v0.title, "sidechain_declaration.v0.title")?,
                description: required(v0.description, "sidechain_declaration.v0.description")?,
                hash_id_1: consensus_hex(v0.hash_id_1, "sidechain_declaration.v0.hash_id_1")?,
                hash_id_2: raw_hex(v0.hash_id_2, "sidechain_declaration.v0.hash_id_2")?,
            })
        }
    };

    Ok(events::SidechainDeclaration {
        declaration: Some(declaration),
    })
}

fn withdrawal_bundle_proposal(
    proposal: mainchain::get_withdrawal_bundle_proposals_response::ResponseItem,
) -> Result<events::WithdrawalBundleProposal> {
    Ok(events::WithdrawalBundleProposal {
        m6id: consensus_hex(proposal.m6id, "withdrawal_bundle_proposal.m6id")?,
        vote_count: required(proposal.vote_count, "withdrawal_bundle_proposal.vote_count")?,
        proposal_height: required(
            proposal.proposal_height,
            "withdrawal_bundle_proposal.proposal_height",
        )?,
    })
}

fn ctip_value(ctip: mainchain::get_ctip_response::Ctip) -> Result<events::Ctip> {
    Ok(events::Ctip {
        txid: hash_from_reverse(ctip.txid, "ctip.txid")?,
        vout: ctip.vout,
        value_sats: ctip.value,
        sequence_number: ctip.sequence_number,
    })
}

fn sidechain_event(event: mainchain::block_info::Event) -> Result<events::SidechainEvent> {
    let event = match required(event.event, "block_info.event.event")? {
        mainchain::block_info::event::Event::Deposit(deposit) => {
            events::sidechain_event::Event::Deposit(deposit_event(deposit)?)
        }
        mainchain::block_info::event::Event::WithdrawalBundle(withdrawal) => {
            events::sidechain_event::Event::WithdrawalBundle(withdrawal_event(withdrawal)?)
        }
    };

    Ok(events::SidechainEvent { event: Some(event) })
}

fn deposit_event(deposit: mainchain::Deposit) -> Result<events::Deposit> {
    let outpoint = required(deposit.outpoint, "deposit.outpoint")?;
    let output = required(deposit.output, "deposit.output")?;

    Ok(events::Deposit {
        sequence_number: required(deposit.sequence_number, "deposit.sequence_number")?,
        outpoint: Some(events::OutPoint {
            txid: hash_from_reverse(outpoint.txid, "deposit.outpoint.txid")?,
            vout: required(outpoint.vout, "deposit.outpoint.vout")?,
        }),
        address: raw_hex(output.address, "deposit.output.address")?,
        value_sats: required(output.value_sats, "deposit.output.value_sats")?,
    })
}

fn withdrawal_event(
    withdrawal: mainchain::WithdrawalBundleEvent,
) -> Result<events::WithdrawalBundleEvent> {
    let event = required(withdrawal.event, "withdrawal_bundle.event")?;
    let state = match required(event.event, "withdrawal_bundle.event.event")? {
        mainchain::withdrawal_bundle_event::event::Event::Submitted(_) => {
            events::withdrawal_bundle_event::State::Submitted(events::WithdrawalBundleSubmitted {})
        }
        mainchain::withdrawal_bundle_event::event::Event::Failed(_) => {
            events::withdrawal_bundle_event::State::Failed(events::WithdrawalBundleFailed {})
        }
        mainchain::withdrawal_bundle_event::event::Event::Succeeded(succeeded) => {
            events::withdrawal_bundle_event::State::Succeeded(events::WithdrawalBundleSucceeded {
                sequence_number: required(
                    succeeded.sequence_number,
                    "withdrawal_bundle.succeeded.sequence_number",
                )?,
                transaction: consensus_hex(
                    succeeded.transaction,
                    "withdrawal_bundle.succeeded.transaction",
                )?,
            })
        }
    };

    Ok(events::WithdrawalBundleEvent {
        m6id: consensus_hex(withdrawal.m6id, "withdrawal_bundle.m6id")?,
        state: Some(state),
    })
}

fn enforcer_event(event: events::enforcer_event::Event) -> events::EnforcerEvent {
    events::EnforcerEvent { event: Some(event) }
}

fn network(value: i32) -> events::Network {
    // The vendored API can lag the deployed enforcer, so an unrecognized value
    // is expected to be survivable. Record the raw number before collapsing it,
    // or the evidence that a wider enum exists is lost entirely.
    let Ok(network) = mainchain::Network::try_from(value) else {
        tracing::warn!(
            network = value,
            "enforcer reported a network the vendored API does not define; publishing it as unknown"
        );
        return events::Network::Unknown;
    };

    match network {
        mainchain::Network::Unspecified => events::Network::Unspecified,
        mainchain::Network::Unknown => events::Network::Unknown,
        mainchain::Network::Mainnet => events::Network::Mainnet,
        mainchain::Network::Regtest => events::Network::Regtest,
        mainchain::Network::Signet => events::Network::Signet,
        mainchain::Network::Testnet => events::Network::Testnet,
    }
}

fn fixed_consensus_hex(value: Option<common::ConsensusHex>, field: &str) -> Result<Vec<u8>> {
    let bytes = consensus_hex(value, field)?;
    require_32_bytes(bytes, field)
}

fn hash_from_reverse(value: Option<common::ReverseHex>, field: &str) -> Result<Vec<u8>> {
    let value = required(value, field)?;
    let bytes = decode_hex(required(value.hex, field)?, field)?;
    require_32_bytes(bytes, field)
}

fn consensus_hex(value: Option<common::ConsensusHex>, field: &str) -> Result<Vec<u8>> {
    let value = required(value, field)?;
    decode_hex(required(value.hex, field)?, field)
}

fn raw_hex(value: Option<common::Hex>, field: &str) -> Result<Vec<u8>> {
    let value = required(value, field)?;
    decode_hex(required(value.hex, field)?, field)
}

fn decode_hex(value: String, field: &str) -> Result<Vec<u8>> {
    hex::decode(value).with_context(|| format!("decoding `{field}` as hex"))
}

fn require_32_bytes(value: Vec<u8>, field: &str) -> Result<Vec<u8>> {
    if value.len() != 32 {
        bail!(
            "`{field}` must contain exactly 32 bytes, got {}",
            value.len()
        );
    }
    Ok(value)
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.with_context(|| format!("missing required field `{field}`"))
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;

    use super::{bip300_block_deltas, network};
    use crate::proto::{common, mainchain};

    fn reverse_hex(byte: u8) -> common::ReverseHex {
        common::ReverseHex {
            hex: Some(hex::encode([byte; 32])),
        }
    }

    fn consensus_hex(bytes: &[u8]) -> common::ConsensusHex {
        common::ConsensusHex {
            hex: Some(hex::encode(bytes)),
        }
    }

    #[test]
    fn known_networks_map_and_undefined_values_collapse_to_unknown() {
        for (upstream, expected) in [
            (
                mainchain::Network::Unspecified,
                events::Network::Unspecified,
            ),
            (mainchain::Network::Unknown, events::Network::Unknown),
            (mainchain::Network::Mainnet, events::Network::Mainnet),
            (mainchain::Network::Regtest, events::Network::Regtest),
            (mainchain::Network::Signet, events::Network::Signet),
            (mainchain::Network::Testnet, events::Network::Testnet),
        ] {
            assert_eq!(network(upstream as i32), expected);
        }

        // A wider upstream enum must neither panic nor be mistaken for a
        // defined network. The warning is what preserves the raw value.
        assert_eq!(network(9_999), events::Network::Unknown);
        assert_eq!(network(-1), events::Network::Unknown);
    }

    #[test]
    fn block_delta_preserves_wire_bytes_display_hashes_and_future_enums() {
        let response = mainchain::GetBip300BlockDeltaResponse {
            deltas: vec![mainchain::Bip300BlockDelta {
                header_info: Some(mainchain::BlockHeaderInfo {
                    block_hash: Some(reverse_hex(0x11)),
                    prev_block_hash: Some(reverse_hex(0x10)),
                    height: 963_648,
                    work: Some(consensus_hex(&[0x22; 32])),
                    timestamp: 1_750_000_000,
                }),
                coinbase_txid: Some(reverse_hex(0x33)),
                coinbase_messages: vec![mainchain::Bip300CoinbaseMessage {
                    vout: 7,
                    raw_script_pubkey: Some(common::Hex {
                        hex: Some("6a01ff".to_owned()),
                    }),
                    accepted: false,
                    message: Some(mainchain::bip300_coinbase_message::Message::M2(
                        mainchain::M2Delta {
                            sidechain_number: 9,
                            description_sha256d_hash: Some(reverse_hex(0x44)),
                            effect: 9_999,
                        },
                    )),
                }],
                treasury_transitions: vec![mainchain::TreasuryTransition {
                    kind: -7,
                    sidechain_number: 9,
                    previous_ctip: None,
                    new_ctip: Some(mainchain::TreasuryCtip {
                        txid: Some(reverse_hex(0x55)),
                        vout: 2,
                        value_sats: u64::MAX,
                    }),
                    sequence_number: Some(u64::MAX),
                    delta_sats: Some(1),
                    payout_sats: None,
                    fee_sats: None,
                    m6id: None,
                    sidechain_address: None,
                    transaction: None,
                    proposal_height: None,
                    terminal_height: Some(963_648),
                }],
                confirmed_bmm_requests: Vec::new(),
            }],
        };

        let mut converted = bip300_block_deltas(response).expect("valid block delta");
        let events::enforcer_event::Event::Bip300BlockDelta(delta) = converted
            .remove(0)
            .event
            .expect("concrete normalized event")
        else {
            panic!("wrong normalized event variant")
        };
        assert_eq!(delta.header.expect("header").hash, vec![0x11; 32]);
        assert_eq!(delta.coinbase_txid, vec![0x33; 32]);
        assert_eq!(
            delta.coinbase_messages[0].raw_script_pubkey,
            [0x6a, 1, 0xff]
        );
        let events::bip300_coinbase_message::Message::M2(m2) = delta.coinbase_messages[0]
            .message
            .as_ref()
            .expect("typed message")
        else {
            panic!("wrong typed coinbase message")
        };
        assert_eq!(m2.effect, 9_999, "future enum numbers must survive");
        assert_eq!(delta.treasury_transitions[0].kind, -7);
        assert_eq!(
            delta.treasury_transitions[0]
                .new_ctip
                .as_ref()
                .expect("new ctip")
                .value_sats,
            u64::MAX
        );
        assert_eq!(
            delta.treasury_transitions[0].sequence_number,
            Some(u64::MAX)
        );
    }
}
