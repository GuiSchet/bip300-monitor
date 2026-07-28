//! Fallible conversions from the upstream validator API to monitor events.

use anyhow::{Context, Result, bail};
use shared::protobuf::enforcer_extractor as events;

use crate::proto::{common, mainchain};

/// Convert `GetChainInfo` into a normalized monitor event.
pub fn chain_info(response: mainchain::GetChainInfoResponse) -> Result<events::EnforcerEvent> {
    let constants = required(response.bip300_constants, "chain_info.bip300_constants")?;
    let chain_info = events::ChainInfo {
        network: network(response.network) as i32,
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
    match mainchain::Network::try_from(value).unwrap_or(mainchain::Network::Unknown) {
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
