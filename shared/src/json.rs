//! JSON rendering of monitor events with byte fields in hexadecimal.
//!
//! Serde renders protobuf `bytes` as an array of numbers, which is unreadable
//! in a log line and unqueryable in a `jsonb` column. Every such field is
//! re-encoded as a hexadecimal string instead.

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::protobuf::event::Event;

/// Every `bytes` field across the monitor's protobuf contract.
///
/// Kept as a flat name list because a walk over the value tree has no protobuf
/// type information to match on. `description` is the one intentional name
/// collision: it is bytes in `Bip300M1` and text in
/// `SidechainDeclarationV0`. Text values are therefore left untouched while
/// byte arrays are encoded below. The
/// `hexadecimal_field_list_covers_every_proto_bytes_field` test keeps the list
/// complete.
pub const BYTE_FIELDS: &[&str] = &[
    "address",
    "block_hash",
    "bmm_commitment",
    "chain_work",
    "coinbase_txid",
    "description",
    "description_hash",
    "downvoted_m6ids",
    "hash",
    "hash_id_1",
    "hash_id_2",
    "hstar",
    "m6id",
    "previous_hash",
    "previous_mainchain_block_hash",
    "raw_description",
    "raw_script_pubkey",
    "sidechain_address",
    "transaction",
    "txid",
    "upvoted_m6id",
];

/// Protobuf fields whose shape is `repeated bytes` rather than one byte string.
///
/// The separate list is required for the empty value: serde renders both an
/// empty `bytes` and an empty `repeated bytes` as `[]`, so the JSON shape alone
/// cannot tell whether the result must be `""` or `[]`.
pub const REPEATED_BYTE_FIELDS: &[&str] = &["downvoted_m6ids"];

/// Render one event as JSON with every byte field in hexadecimal.
pub fn render(event: &Event) -> Result<Value> {
    let mut value = serde_json::to_value(event).context("serializing the event as JSON")?;
    encode_byte_fields(&mut value)?;
    Ok(value)
}

/// Render one event as a single JSON line.
pub fn render_line(event: &Event) -> Result<String> {
    serde_json::to_string(&render(event)?).context("encoding the event JSON")
}

fn encode_byte_fields(value: &mut Value) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                encode_byte_fields(value)?;
            }
        }
        Value::Object(entries) => {
            for (key, value) in entries.iter_mut() {
                if BYTE_FIELDS.contains(&key.as_str()) {
                    // `description` is also a string field in the decoded M1
                    // sidechain declaration. Preserve that text representation
                    // while encoding the consensus-byte form used by the block
                    // delta contract.
                    if key != "description" || !value.is_string() {
                        *value = if REPEATED_BYTE_FIELDS.contains(&key.as_str()) {
                            encode_repeated_bytes(value, key)?
                        } else {
                            encode_bytes(value, key)?
                        };
                    }
                } else {
                    encode_byte_fields(value)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn encode_repeated_bytes(value: &Value, field: &str) -> Result<Value> {
    let Value::Array(values) = value else {
        bail!("`{field}` is not a repeated byte array");
    };
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            encode_bytes(value, field)
                .with_context(|| format!("encoding `{field}` element {index}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

fn encode_bytes(value: &Value, field: &str) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Array(values) => {
            let bytes = values
                .iter()
                .map(|value| {
                    let byte = value
                        .as_u64()
                        .with_context(|| format!("`{field}` contains a non-numeric byte"))?;
                    u8::try_from(byte)
                        .with_context(|| format!("`{field}` contains a byte outside 0..=255"))
                })
                .collect::<Result<Vec<u8>>>()?;
            Ok(Value::String(hex::encode(bytes)))
        }
        _ => bail!("`{field}` is not a byte array"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{BYTE_FIELDS, REPEATED_BYTE_FIELDS, render};
    use crate::protobuf::enforcer_extractor as events;
    use crate::protobuf::event::{Event, event::MonitorEvent};

    #[test]
    fn hexadecimal_field_list_covers_every_proto_bytes_field() {
        // The envelope carries byte fields of its own, so scanning only the
        // extractor contract would let one render as an array of numbers.
        let proto = format!(
            "{}\n{}",
            include_str!("../../proto/enforcer_extractor.proto"),
            include_str!("../../proto/event.proto")
        );
        let proto_fields = proto
            .lines()
            .filter_map(|line| {
                if line.trim_start().starts_with("//") {
                    return None;
                }
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                let bytes_index = tokens.iter().position(|token| *token == "bytes")?;
                tokens
                    .get(bytes_index + 1)
                    .map(|field| field.trim_end_matches(';').to_owned())
            })
            .collect::<BTreeSet<_>>();
        let rendered_fields = BYTE_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(rendered_fields, proto_fields);

        let repeated_proto_fields = proto
            .lines()
            .filter_map(|line| {
                let tokens = line.split_whitespace().collect::<Vec<_>>();
                if tokens.first() == Some(&"repeated") && tokens.get(1) == Some(&"bytes") {
                    tokens
                        .get(2)
                        .map(|field| field.trim_end_matches(';').to_owned())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        let repeated_rendered_fields = REPEATED_BYTE_FIELDS
            .iter()
            .map(|field| (*field).to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(repeated_rendered_fields, repeated_proto_fields);
    }

    #[test]
    fn active_sidechain_fields_have_a_stable_query_path() {
        let event = Event {
            timestamp: 1,
            observed_at_block: None,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::ActiveSidechains(
                    events::ActiveSidechainsSnapshot {
                        sidechains: vec![events::ActiveSidechain {
                            sidechain_number: 9,
                            activation_height: 987_401,
                            ..Default::default()
                        }],
                    },
                )),
            })),
        };

        let value = render(&event).expect("render event");
        let sidechain =
            &value["monitor_event"]["Enforcer"]["event"]["ActiveSidechains"]["sidechains"][0];
        assert_eq!(sidechain["sidechain_number"], 9);
        assert_eq!(sidechain["activation_height"], 987_401);
    }

    #[test]
    fn repeated_byte_fields_render_as_lists_of_hex_strings() {
        let event = m4_event(vec![vec![0x55; 32], vec![0xaa, 0xbb]]);
        let value = render(&event).expect("render M4 downvotes");
        let downvotes = &value["monitor_event"]["Enforcer"]["event"]["Bip300BlockDelta"]["coinbase_messages"]
            [0]["message"]["M4"]["effects"][0]["downvoted_m6ids"];

        assert_eq!(downvotes, &serde_json::json!(["55".repeat(32), "aabb"]));
    }

    #[test]
    fn an_empty_repeated_byte_field_stays_an_empty_list() {
        let value = render(&m4_event(Vec::new())).expect("render empty M4 downvotes");
        let downvotes = &value["monitor_event"]["Enforcer"]["event"]["Bip300BlockDelta"]["coinbase_messages"]
            [0]["message"]["M4"]["effects"][0]["downvoted_m6ids"];

        assert_eq!(downvotes, &serde_json::json!([]));
    }

    fn m4_event(downvoted_m6ids: Vec<Vec<u8>>) -> Event {
        Event {
            timestamp: 1,
            observed_at_block: None,
            monitor_event: Some(MonitorEvent::Enforcer(events::EnforcerEvent {
                event: Some(events::enforcer_event::Event::Bip300BlockDelta(
                    events::Bip300BlockDelta {
                        coinbase_messages: vec![events::Bip300CoinbaseMessage {
                            message: Some(events::bip300_coinbase_message::Message::M4(
                                events::M4Delta {
                                    effects: vec![events::M4Effect {
                                        downvoted_m6ids,
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            })),
        }
    }
}
