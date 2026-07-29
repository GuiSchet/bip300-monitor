//! Human-readable and complete event rendering.

use anyhow::{Context, Result, bail};
use serde_json::Value;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::Event;
use shared::protobuf::event::event::MonitorEvent;

const BYTE_FIELDS: &[&str] = &[
    "address",
    "block_hash",
    "bmm_commitment",
    "chain_work",
    "description_hash",
    "hash",
    "hash_id_1",
    "hash_id_2",
    "m6id",
    "previous_hash",
    "raw_description",
    "transaction",
    "txid",
];

/// Rendered forms of one validated event.
#[derive(Debug)]
pub struct RenderedEvent {
    /// Stable event variant name.
    pub kind: &'static str,
    /// Compact human-readable description.
    pub summary: String,
    /// Complete one-line JSON representation with byte fields in hexadecimal.
    pub full_json: String,
}

/// Validate and render one monitor event.
pub fn render(event: &Event) -> Result<RenderedEvent> {
    let payload = match event.monitor_event.as_ref() {
        Some(MonitorEvent::Enforcer(payload)) => payload,
        None => bail!("event envelope does not contain a monitor event"),
    };
    let payload = payload
        .event
        .as_ref()
        .context("enforcer event does not contain a concrete event")?;
    let (kind, summary) = summarize(payload)?;

    let mut full = serde_json::to_value(event).context("serializing the event as JSON")?;
    encode_byte_fields(&mut full)?;
    let full_json = serde_json::to_string(&full).context("encoding the event JSON")?;

    Ok(RenderedEvent {
        kind,
        summary,
        full_json,
    })
}

fn summarize(payload: &events::enforcer_event::Event) -> Result<(&'static str, String)> {
    match payload {
        events::enforcer_event::Event::ChainInfo(chain_info) => {
            let network = events::Network::try_from(chain_info.network)
                .context("chain info contains an unknown network value")?;
            let constants = chain_info
                .bip300_constants
                .as_ref()
                .context("chain info is missing BIP300 constants")?;
            Ok((
                "chain_info",
                format!(
                    "network={} activation_height={} withdrawal_bundle_max_age={}",
                    network.as_str_name().to_ascii_lowercase(),
                    constants.activation_height,
                    constants.withdrawal_bundle_max_age
                ),
            ))
        }
        events::enforcer_event::Event::ChainTip(chain_tip) => {
            let header = chain_tip
                .header
                .as_ref()
                .context("chain tip is missing its block header")?;
            validate_header(header)?;
            Ok((
                "chain_tip",
                format!(
                    "height={} block_hash={}",
                    header.height,
                    hex::encode(&header.hash)
                ),
            ))
        }
        events::enforcer_event::Event::SidechainProposals(snapshot) => {
            for proposal in &snapshot.proposals {
                validate_proposal(proposal)?;
            }
            Ok((
                "sidechain_proposals",
                format!("proposal_count={}", snapshot.proposals.len()),
            ))
        }
        events::enforcer_event::Event::ActiveSidechains(snapshot) => {
            for sidechain in &snapshot.sidechains {
                validate_active_sidechain(sidechain)?;
            }
            Ok((
                "active_sidechains",
                format!("sidechain_count={}", snapshot.sidechains.len()),
            ))
        }
        events::enforcer_event::Event::Ctip(snapshot) => {
            if let Some(ctip) = snapshot.ctip.as_ref() {
                require_32_bytes(&ctip.txid, "ctip.txid")?;
                Ok((
                    "ctip",
                    format!(
                        "sidechain={} present=true txid={} vout={} value_sats={} sequence_number={}",
                        snapshot.sidechain_number,
                        hex::encode(&ctip.txid),
                        ctip.vout,
                        ctip.value_sats,
                        ctip.sequence_number
                    ),
                ))
            } else {
                Ok((
                    "ctip",
                    format!("sidechain={} present=false", snapshot.sidechain_number),
                ))
            }
        }
        events::enforcer_event::Event::BlockConnected(block) => {
            let header = block
                .header
                .as_ref()
                .context("connected block is missing its block header")?;
            validate_header(header)?;
            let (deposits, withdrawal_bundles) = validate_sidechain_events(&block.events)?;
            Ok((
                "block_connected",
                format!(
                    "sidechain={} height={} block_hash={} deposits={} withdrawal_bundles={} \
                     bmm_commitment={}",
                    block.sidechain_number,
                    header.height,
                    hex::encode(&header.hash),
                    deposits,
                    withdrawal_bundles,
                    block.bmm_commitment.is_some()
                ),
            ))
        }
        events::enforcer_event::Event::BlockDisconnected(block) => {
            require_32_bytes(&block.block_hash, "block_disconnected.block_hash")?;
            Ok((
                "block_disconnected",
                format!(
                    "sidechain={} block_hash={}",
                    block.sidechain_number,
                    hex::encode(&block.block_hash)
                ),
            ))
        }
    }
}

fn validate_header(header: &events::BlockHeader) -> Result<()> {
    require_32_bytes(&header.hash, "block_header.hash")?;
    require_32_bytes(&header.previous_hash, "block_header.previous_hash")?;
    require_32_bytes(&header.chain_work, "block_header.chain_work")
}

fn validate_proposal(proposal: &events::SidechainProposal) -> Result<()> {
    require_32_bytes(
        &proposal.description_hash,
        "sidechain_proposal.description_hash",
    )?;
    validate_declaration(proposal.declaration.as_ref())
}

fn validate_active_sidechain(sidechain: &events::ActiveSidechain) -> Result<()> {
    validate_declaration(sidechain.declaration.as_ref())
}

fn validate_declaration(declaration: Option<&events::SidechainDeclaration>) -> Result<()> {
    if let Some(declaration) = declaration {
        declaration
            .declaration
            .as_ref()
            .context("sidechain declaration is missing its concrete version")?;
    }
    Ok(())
}

fn validate_sidechain_events(
    sidechain_events: &[events::SidechainEvent],
) -> Result<(usize, usize)> {
    let mut deposits = 0;
    let mut withdrawal_bundles = 0;

    for sidechain_event in sidechain_events {
        match sidechain_event
            .event
            .as_ref()
            .context("sidechain event is missing its concrete event")?
        {
            events::sidechain_event::Event::Deposit(deposit) => {
                let outpoint = deposit
                    .outpoint
                    .as_ref()
                    .context("deposit is missing its outpoint")?;
                require_32_bytes(&outpoint.txid, "deposit.outpoint.txid")?;
                deposits += 1;
            }
            events::sidechain_event::Event::WithdrawalBundle(withdrawal) => {
                withdrawal
                    .state
                    .as_ref()
                    .context("withdrawal bundle is missing its state")?;
                withdrawal_bundles += 1;
            }
        }
    }

    Ok((deposits, withdrawal_bundles))
}

fn require_32_bytes(value: &[u8], field: &str) -> Result<()> {
    if value.len() != 32 {
        bail!("{field} must contain 32 bytes, got {}", value.len());
    }
    Ok(())
}

fn encode_byte_fields(value: &mut Value) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                encode_byte_fields(value)?;
            }
        }
        Value::Object(fields) => {
            for (name, value) in fields {
                if BYTE_FIELDS.contains(&name.as_str()) {
                    if value.is_null() {
                        continue;
                    }
                    let bytes = value
                        .as_array()
                        .with_context(|| format!("serialized byte field `{name}` is not an array"))?
                        .iter()
                        .map(|byte| {
                            let byte = byte.as_u64().with_context(|| {
                                format!("serialized byte field `{name}` contains a non-integer")
                            })?;
                            u8::try_from(byte).with_context(|| {
                                format!("serialized byte field `{name}` contains {byte}")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    *value = Value::String(hex::encode(bytes));
                } else {
                    encode_byte_fields(value)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::Event;
    use shared::protobuf::event::event::MonitorEvent;

    use super::render;

    fn envelope(payload: events::enforcer_event::Event) -> Event {
        Event {
            timestamp: 1_700_000_000_000,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(payload),
            })),
        }
    }

    fn header(byte: u8) -> events::BlockHeader {
        events::BlockHeader {
            hash: vec![byte; 32],
            previous_hash: vec![byte.wrapping_sub(1); 32],
            height: 42,
            chain_work: vec![2; 32],
            timestamp: 1_700_000_000,
        }
    }

    #[test]
    fn summarizes_every_top_level_variant() {
        let variants = [
            (
                events::enforcer_event::Event::ChainInfo(events::ChainInfo {
                    network: events::Network::Regtest as i32,
                    bip300_constants: Some(events::Bip300Constants {
                        activation_height: 100,
                        withdrawal_bundle_max_age: 200,
                        ..Default::default()
                    }),
                }),
                "chain_info",
            ),
            (
                events::enforcer_event::Event::ChainTip(events::ChainTip {
                    header: Some(header(1)),
                }),
                "chain_tip",
            ),
            (
                events::enforcer_event::Event::SidechainProposals(
                    events::SidechainProposalsSnapshot { proposals: vec![] },
                ),
                "sidechain_proposals",
            ),
            (
                events::enforcer_event::Event::ActiveSidechains(events::ActiveSidechainsSnapshot {
                    sidechains: vec![],
                }),
                "active_sidechains",
            ),
            (
                events::enforcer_event::Event::Ctip(events::CtipSnapshot {
                    sidechain_number: 9,
                    ctip: None,
                }),
                "ctip",
            ),
            (
                events::enforcer_event::Event::BlockConnected(events::BlockConnected {
                    header: Some(header(3)),
                    sidechain_number: 9,
                    bmm_commitment: None,
                    events: vec![],
                }),
                "block_connected",
            ),
            (
                events::enforcer_event::Event::BlockDisconnected(events::BlockDisconnected {
                    block_hash: vec![4; 32],
                    sidechain_number: 9,
                }),
                "block_disconnected",
            ),
        ];

        for (payload, expected_kind) in variants {
            let rendered = render(&envelope(payload)).expect("valid event");
            assert_eq!(rendered.kind, expected_kind);
            assert!(!rendered.summary.is_empty());
            assert!(!rendered.full_json.is_empty());
        }
    }

    #[test]
    fn full_output_is_complete_json_with_hexadecimal_bytes() {
        let event = envelope(events::enforcer_event::Event::BlockConnected(
            events::BlockConnected {
                header: Some(header(0xab)),
                sidechain_number: 9,
                bmm_commitment: Some(vec![0xcd, 0xef]),
                events: vec![events::SidechainEvent {
                    event: Some(events::sidechain_event::Event::Deposit(events::Deposit {
                        sequence_number: 7,
                        outpoint: Some(events::OutPoint {
                            txid: vec![0x11; 32],
                            vout: 2,
                        }),
                        address: vec![0x22, 0x33],
                        value_sats: 50,
                    })),
                }],
            },
        ));

        let rendered = render(&event).expect("valid connected block");
        let json: serde_json::Value =
            serde_json::from_str(&rendered.full_json).expect("valid JSON");
        assert_eq!(json["timestamp"], 1_700_000_000_000_u64);
        assert!(rendered.full_json.contains(&"ab".repeat(32)));
        assert!(rendered.full_json.contains("\"bmm_commitment\":\"cdef\""));
        assert!(rendered.full_json.contains(&"11".repeat(32)));
        assert!(rendered.full_json.contains("\"address\":\"2233\""));
    }

    #[test]
    fn full_output_preserves_proposals_and_every_withdrawal_state() {
        let proposal = envelope(events::enforcer_event::Event::SidechainProposals(
            events::SidechainProposalsSnapshot {
                proposals: vec![events::SidechainProposal {
                    sidechain_number: 9,
                    raw_description: vec![0x01, 0x02],
                    description_hash: vec![0x03; 32],
                    vote_count: 4,
                    proposal_height: 5,
                    proposal_age: 6,
                    declaration: Some(events::SidechainDeclaration {
                        declaration: Some(events::sidechain_declaration::Declaration::V0(
                            events::SidechainDeclarationV0 {
                                title: "sidechain".to_owned(),
                                description: "description".to_owned(),
                                hash_id_1: vec![0x07],
                                hash_id_2: vec![0x08],
                            },
                        )),
                    }),
                }],
            },
        ));
        let proposal = render(&proposal)
            .expect("valid proposal snapshot")
            .full_json;
        assert!(proposal.contains("\"raw_description\":\"0102\""));
        assert!(proposal.contains(&"03".repeat(32)));
        assert!(proposal.contains("\"hash_id_1\":\"07\""));
        assert!(proposal.contains("\"hash_id_2\":\"08\""));

        let states = [
            events::withdrawal_bundle_event::State::Submitted(events::WithdrawalBundleSubmitted {}),
            events::withdrawal_bundle_event::State::Failed(events::WithdrawalBundleFailed {}),
            events::withdrawal_bundle_event::State::Succeeded(events::WithdrawalBundleSucceeded {
                sequence_number: 10,
                transaction: vec![0xaa, 0xbb],
            }),
        ];
        for state in states {
            let event = envelope(events::enforcer_event::Event::BlockConnected(
                events::BlockConnected {
                    header: Some(header(9)),
                    sidechain_number: 9,
                    bmm_commitment: None,
                    events: vec![events::SidechainEvent {
                        event: Some(events::sidechain_event::Event::WithdrawalBundle(
                            events::WithdrawalBundleEvent {
                                m6id: vec![0xcc, 0xdd],
                                state: Some(state),
                            },
                        )),
                    }],
                },
            ));

            let rendered = render(&event).expect("valid withdrawal event");
            assert!(rendered.summary.contains("withdrawal_bundles=1"));
            assert!(rendered.full_json.contains("\"m6id\":\"ccdd\""));
            if rendered.full_json.contains("Succeeded") {
                assert!(rendered.full_json.contains("\"transaction\":\"aabb\""));
            }
        }
    }

    #[test]
    fn rejects_empty_and_incomplete_envelopes() {
        let empty = Event {
            timestamp: 1,
            monitor_event: None,
        };
        assert!(
            render(&empty)
                .expect_err("empty envelope")
                .to_string()
                .contains("envelope")
        );

        let empty_enforcer = Event {
            timestamp: 1,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: None,
            })),
        };
        assert!(
            render(&empty_enforcer)
                .expect_err("empty enforcer event")
                .to_string()
                .contains("concrete event")
        );
    }

    #[test]
    fn rejects_invalid_normalized_hashes_and_nested_events() {
        let bad_hash = envelope(events::enforcer_event::Event::BlockDisconnected(
            events::BlockDisconnected {
                block_hash: vec![1; 31],
                sidechain_number: 9,
            },
        ));
        assert!(
            render(&bad_hash)
                .expect_err("31-byte hash")
                .to_string()
                .contains("32 bytes")
        );

        let missing_nested = envelope(events::enforcer_event::Event::BlockConnected(
            events::BlockConnected {
                header: Some(header(5)),
                sidechain_number: 9,
                bmm_commitment: None,
                events: vec![events::SidechainEvent { event: None }],
            },
        ));
        assert!(
            render(&missing_nested)
                .expect_err("missing sidechain event")
                .to_string()
                .contains("concrete event")
        );
    }
}
