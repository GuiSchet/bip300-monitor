use enforcer_extractor::{
    convert,
    proto::{common, mainchain},
};
use shared::protobuf::enforcer_extractor as events;

fn reverse_hex(byte: u8) -> Option<common::ReverseHex> {
    Some(common::ReverseHex {
        hex: Some(hex::encode([byte; 32])),
    })
}

fn consensus_hex(bytes: &[u8]) -> Option<common::ConsensusHex> {
    Some(common::ConsensusHex {
        hex: Some(hex::encode(bytes)),
    })
}

fn raw_hex(bytes: &[u8]) -> Option<common::Hex> {
    Some(common::Hex {
        hex: Some(hex::encode(bytes)),
    })
}

fn header(hash_byte: u8, height: u32) -> mainchain::BlockHeaderInfo {
    mainchain::BlockHeaderInfo {
        block_hash: reverse_hex(hash_byte),
        prev_block_hash: reverse_hex(hash_byte.saturating_sub(1)),
        height,
        work: consensus_hex(&[0x44; 32]),
        timestamp: 1_750_000_000,
    }
}

fn declaration() -> mainchain::SidechainDeclaration {
    mainchain::SidechainDeclaration {
        sidechain_declaration: Some(mainchain::sidechain_declaration::SidechainDeclaration::V0(
            mainchain::sidechain_declaration::V0 {
                title: Some("test sidechain".to_owned()),
                description: Some("integration fixture".to_owned()),
                hash_id_1: consensus_hex(&[0xaa, 0xbb]),
                hash_id_2: raw_hex(&[0xcc, 0xdd]),
            },
        )),
    }
}

fn deposit() -> mainchain::block_info::Event {
    mainchain::block_info::Event {
        event: Some(mainchain::block_info::event::Event::Deposit(
            mainchain::Deposit {
                sequence_number: Some(12),
                outpoint: Some(mainchain::OutPoint {
                    txid: reverse_hex(0x55),
                    vout: Some(3),
                }),
                output: Some(mainchain::deposit::Output {
                    address: raw_hex(&[0xde, 0xad, 0xbe, 0xef]),
                    value_sats: Some(42_000),
                }),
            },
        )),
    }
}

fn withdrawal(
    event: mainchain::withdrawal_bundle_event::event::Event,
    m6id_byte: u8,
) -> mainchain::block_info::Event {
    mainchain::block_info::Event {
        event: Some(mainchain::block_info::event::Event::WithdrawalBundle(
            mainchain::WithdrawalBundleEvent {
                m6id: consensus_hex(&[m6id_byte; 32]),
                event: Some(mainchain::withdrawal_bundle_event::Event { event: Some(event) }),
            },
        )),
    }
}

fn block_info() -> mainchain::BlockInfo {
    use mainchain::withdrawal_bundle_event::event::{Event, Failed, Submitted, Succeeded};

    mainchain::BlockInfo {
        bmm_commitment: consensus_hex(&[0x99; 32]),
        events: vec![
            deposit(),
            withdrawal(Event::Submitted(Submitted {}), 1),
            withdrawal(Event::Failed(Failed {}), 2),
            withdrawal(
                Event::Succeeded(Succeeded {
                    sequence_number: Some(13),
                    transaction: consensus_hex(&[0x01, 0x02, 0x03]),
                }),
                3,
            ),
        ],
    }
}

fn event(value: events::EnforcerEvent) -> events::enforcer_event::Event {
    value.event.expect("converted event")
}

#[test]
fn converts_chain_info_and_tip() {
    let chain_info = convert::chain_info(mainchain::GetChainInfoResponse {
        network: mainchain::Network::Regtest as i32,
        bip300_constants: Some(mainchain::get_chain_info_response::Bip300Constants {
            withdrawal_bundle_max_age: 10,
            withdrawal_bundle_inclusion_threshold: 7,
            used_sidechain_slot_proposal_max_age: 20,
            used_sidechain_slot_activation_threshold: 15,
            unused_sidechain_slot_proposal_max_age: 30,
            unused_sidechain_slot_activation_threshold: 25,
            activation_height: 100,
        }),
    })
    .expect("valid chain info");

    let events::enforcer_event::Event::ChainInfo(chain_info) = event(chain_info) else {
        panic!("expected chain info");
    };
    assert_eq!(chain_info.network, events::Network::Regtest as i32);
    let constants = chain_info.bip300_constants.expect("BIP300 constants");
    assert_eq!(constants.withdrawal_bundle_max_age, 10);
    assert_eq!(constants.withdrawal_bundle_inclusion_threshold, 7);
    assert_eq!(constants.activation_height, 100);

    let chain_tip = convert::chain_tip(mainchain::GetChainTipResponse {
        block_header_info: Some(header(0x22, 321)),
    })
    .expect("valid chain tip");
    let events::enforcer_event::Event::ChainTip(chain_tip) = event(chain_tip) else {
        panic!("expected chain tip");
    };
    let header = chain_tip.header.expect("block header");
    assert_eq!(header.hash, vec![0x22; 32]);
    assert_eq!(header.previous_hash, vec![0x21; 32]);
    assert_eq!(header.chain_work, vec![0x44; 32]);
    assert_eq!(header.height, 321);
    assert_eq!(header.timestamp, 1_750_000_000);
}

#[test]
fn converts_sidechain_snapshots() {
    let proposals = convert::sidechain_proposals(mainchain::GetSidechainProposalsResponse {
        sidechain_proposals: vec![
            mainchain::get_sidechain_proposals_response::SidechainProposal {
                sidechain_number: Some(7),
                description: consensus_hex(&[0x01, 0x02]),
                declaration: None,
                description_sha256d_hash: reverse_hex(0x33),
                vote_count: Some(4),
                proposal_height: Some(100),
                proposal_age: Some(6),
            },
        ],
    })
    .expect("valid proposals");
    let events::enforcer_event::Event::SidechainProposals(snapshot) = event(proposals) else {
        panic!("expected sidechain proposals");
    };
    let proposal = &snapshot.proposals[0];
    assert_eq!(proposal.sidechain_number, 7);
    assert_eq!(proposal.raw_description, vec![0x01, 0x02]);
    assert_eq!(proposal.description_hash, vec![0x33; 32]);
    assert!(proposal.declaration.is_none());

    let active = convert::active_sidechains(mainchain::GetSidechainsResponse {
        sidechains: vec![mainchain::get_sidechains_response::SidechainInfo {
            sidechain_number: Some(7),
            description: consensus_hex(&[0x03, 0x04]),
            vote_count: Some(9),
            proposal_height: Some(100),
            activation_height: Some(200),
            declaration: Some(declaration()),
        }],
    })
    .expect("valid active sidechains");
    let events::enforcer_event::Event::ActiveSidechains(snapshot) = event(active) else {
        panic!("expected active sidechains");
    };
    let active = &snapshot.sidechains[0];
    assert_eq!(active.activation_height, 200);
    let declaration = active
        .declaration
        .as_ref()
        .and_then(|declaration| declaration.declaration.as_ref())
        .expect("known declaration");
    let events::sidechain_declaration::Declaration::V0(v0) = declaration;
    assert_eq!(v0.title, "test sidechain");
    assert_eq!(v0.hash_id_1, vec![0xaa, 0xbb]);
    assert_eq!(v0.hash_id_2, vec![0xcc, 0xdd]);
}

#[test]
fn converts_present_and_absent_ctip() {
    let present = convert::ctip(
        7,
        mainchain::GetCtipResponse {
            ctip: Some(mainchain::get_ctip_response::Ctip {
                txid: reverse_hex(0x66),
                vout: 2,
                value: 1_000_000,
                sequence_number: 8,
            }),
        },
    )
    .expect("valid CTIP");
    let events::enforcer_event::Event::Ctip(snapshot) = event(present) else {
        panic!("expected CTIP");
    };
    assert_eq!(snapshot.sidechain_number, 7);
    let ctip = snapshot.ctip.expect("present CTIP");
    assert_eq!(ctip.txid, vec![0x66; 32]);
    assert_eq!(ctip.value_sats, 1_000_000);

    let absent = convert::ctip(8, mainchain::GetCtipResponse { ctip: None })
        .expect("an absent CTIP is valid");
    let events::enforcer_event::Event::Ctip(snapshot) = event(absent) else {
        panic!("expected CTIP");
    };
    assert_eq!(snapshot.sidechain_number, 8);
    assert!(snapshot.ctip.is_none());
}

#[test]
fn converts_block_connections_backfill_and_disconnections() {
    let upstream_header = header(0x77, 500);
    let upstream_info = block_info();
    let live = convert::subscription_event(
        9,
        mainchain::SubscribeEventsResponse {
            event: Some(mainchain::subscribe_events_response::Event {
                event: Some(
                    mainchain::subscribe_events_response::event::Event::ConnectBlock(
                        mainchain::subscribe_events_response::event::ConnectBlock {
                            header_info: Some(upstream_header.clone()),
                            block_info: Some(upstream_info.clone()),
                        },
                    ),
                ),
            }),
        },
    )
    .expect("valid connected block");

    let backfill = convert::block_info(
        9,
        mainchain::GetBlockInfoResponse {
            infos: vec![mainchain::get_block_info_response::Info {
                header_info: Some(upstream_header),
                block_info: Some(upstream_info),
            }],
        },
    )
    .expect("valid backfill");
    assert_eq!(backfill, vec![live.clone()]);

    let events::enforcer_event::Event::BlockConnected(connected) = event(live) else {
        panic!("expected connected block");
    };
    assert_eq!(connected.sidechain_number, 9);
    assert_eq!(connected.bmm_commitment, Some(vec![0x99; 32]));
    assert_eq!(connected.events.len(), 4);

    let events::sidechain_event::Event::Deposit(deposit) =
        connected.events[0].event.as_ref().expect("deposit")
    else {
        panic!("expected deposit");
    };
    assert_eq!(deposit.sequence_number, 12);
    assert_eq!(deposit.address, vec![0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(deposit.value_sats, 42_000);
    assert_eq!(deposit.outpoint.as_ref().expect("outpoint").vout, 3);

    let expected_states = ["submitted", "failed", "succeeded"];
    for (event, expected) in connected.events[1..].iter().zip(expected_states) {
        let events::sidechain_event::Event::WithdrawalBundle(withdrawal) =
            event.event.as_ref().expect("withdrawal")
        else {
            panic!("expected withdrawal");
        };
        let actual = match withdrawal.state.as_ref().expect("withdrawal state") {
            events::withdrawal_bundle_event::State::Submitted(_) => "submitted",
            events::withdrawal_bundle_event::State::Failed(_) => "failed",
            events::withdrawal_bundle_event::State::Succeeded(succeeded) => {
                assert_eq!(succeeded.sequence_number, 13);
                assert_eq!(succeeded.transaction, vec![0x01, 0x02, 0x03]);
                "succeeded"
            }
        };
        assert_eq!(actual, expected);
    }

    let disconnected = convert::subscription_event(
        9,
        mainchain::SubscribeEventsResponse {
            event: Some(mainchain::subscribe_events_response::Event {
                event: Some(
                    mainchain::subscribe_events_response::event::Event::DisconnectBlock(
                        mainchain::subscribe_events_response::event::DisconnectBlock {
                            block_hash: reverse_hex(0x77),
                        },
                    ),
                ),
            }),
        },
    )
    .expect("valid disconnected block");
    let events::enforcer_event::Event::BlockDisconnected(disconnected) = event(disconnected) else {
        panic!("expected disconnected block");
    };
    assert_eq!(disconnected.block_hash, vec![0x77; 32]);
    assert_eq!(disconnected.sidechain_number, 9);
}

#[test]
fn rejects_missing_and_malformed_required_fields() {
    let missing = convert::chain_tip(mainchain::GetChainTipResponse {
        block_header_info: None,
    })
    .expect_err("missing header must fail");
    assert!(
        missing
            .to_string()
            .contains("missing required field `chain_tip.block_header_info`")
    );

    let malformed = convert::chain_tip(mainchain::GetChainTipResponse {
        block_header_info: Some(mainchain::BlockHeaderInfo {
            block_hash: Some(common::ReverseHex {
                hex: Some("not-hex".to_owned()),
            }),
            ..header(0x22, 321)
        }),
    })
    .expect_err("malformed hex must fail");
    assert!(
        malformed
            .to_string()
            .contains("decoding `block_header.block_hash` as hex")
    );

    let wrong_length = convert::chain_tip(mainchain::GetChainTipResponse {
        block_header_info: Some(mainchain::BlockHeaderInfo {
            block_hash: Some(common::ReverseHex {
                hex: Some("00".to_owned()),
            }),
            ..header(0x22, 321)
        }),
    })
    .expect_err("short hash must fail");
    assert!(
        wrong_length
            .to_string()
            .contains("must contain exactly 32 bytes")
    );
}
