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
/// Kept as a flat name list because the field names are unique across messages
/// and a walk over the value tree has no type information to match on. The
/// `hexadecimal_field_list_covers_every_proto_bytes_field` test keeps it
/// complete.
pub const BYTE_FIELDS: &[&str] = &[
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
                    *value = encode_bytes(value, key)?;
                } else {
                    encode_byte_fields(value)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
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

    use super::BYTE_FIELDS;

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
    }
}
