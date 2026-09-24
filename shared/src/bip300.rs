//! BIP300 description decoding and identity helpers.

use anyhow::{Context, Result};
use bitcoin::consensus::deserialize;
use bitcoin::hashes::{Hash as _, sha256d};

/// Decode the exact `ConsensusHex::encode(Vec<u8>)` representation returned by
/// the enforcer.
///
/// Bitcoin consensus decoding rejects truncated, non-minimal, and trailing
/// encodings. Keeping this separate from hashing prevents the CompactSize
/// prefix from accidentally becoming part of the BIP300 description hash.
pub fn decode_sidechain_description(encoded: &[u8]) -> Result<Vec<u8>> {
    deserialize(encoded).context("decoding the consensus-encoded sidechain description")
}

/// Compute the BIP300 SHA256d description hash in conventional display order.
pub fn sidechain_description_hash(encoded: &[u8]) -> Result<Vec<u8>> {
    let description = decode_sidechain_description(encoded)?;
    let mut display_bytes = sha256d::Hash::hash(&description).to_byte_array().to_vec();
    display_bytes.reverse();
    Ok(display_bytes)
}

#[cfg(test)]
mod tests {
    use super::{decode_sidechain_description, sidechain_description_hash};

    #[test]
    fn description_hash_excludes_the_compact_size_prefix() {
        let encoded = [2, 1, 2];
        assert_eq!(decode_sidechain_description(&encoded).unwrap(), [1, 2]);
        assert_eq!(
            hex::encode(sidechain_description_hash(&encoded).unwrap()),
            "db9b643a0e5bacf2a21cb7b532d65caae8b278c3c284cd3d51d215d9ce6aa576"
        );
    }

    #[test]
    fn malformed_or_noncanonical_descriptions_are_rejected() {
        for encoded in [
            vec![2, 1],
            vec![1, 1, 2],
            vec![0xfd, 1, 0, 1],
            vec![0xfe, 1, 0, 0, 0, 1],
        ] {
            assert!(decode_sidechain_description(&encoded).is_err());
        }
    }

    #[test]
    fn multi_byte_compact_size_is_supported() {
        let description = vec![0x42; 253];
        let mut encoded = vec![0xfd, 0xfd, 0x00];
        encoded.extend_from_slice(&description);
        assert_eq!(decode_sidechain_description(&encoded).unwrap(), description);
    }
}
