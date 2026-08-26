//! Top-level monitor event envelope.

#![allow(clippy::module_inception)]

use std::time::{SystemTime, SystemTimeError};

include!(concat!(env!("OUT_DIR"), "/event.rs"));

impl Event {
    /// Wrap an extractor event with the current Unix timestamp in milliseconds
    /// and the block the observation is anchored to.
    pub fn new(
        event: event::MonitorEvent,
        observed_at_block: Option<ObservedBlock>,
    ) -> Result<Self, SystemTimeError> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

        Ok(Self {
            timestamp: now.as_millis() as u64,
            observed_at_block,
            monitor_event: Some(event),
        })
    }
}

impl ObservedBlock {
    /// Anchor to a block whose height is known.
    pub fn at_height(hash: Vec<u8>, height: u32) -> Self {
        Self {
            hash,
            height: Some(height),
        }
    }

    /// Anchor to a block the source named without a height.
    pub fn without_height(hash: Vec<u8>) -> Self {
        Self { hash, height: None }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{Event, ObservedBlock, event::MonitorEvent};
    use crate::protobuf::enforcer_extractor::{ChainInfo, EnforcerEvent, Network, enforcer_event};

    fn payload() -> EnforcerEvent {
        EnforcerEvent {
            event: Some(enforcer_event::Event::ChainInfo(ChainInfo {
                network: Network::Regtest as i32,
                bip300_constants: None,
            })),
        }
    }

    #[test]
    fn event_round_trips_through_protobuf() {
        let event = Event::new(
            MonitorEvent::Enforcer(payload()),
            Some(ObservedBlock::at_height(vec![0x11; 32], 996_259)),
        )
        .expect("system clock after epoch");

        let encoded = event.encode_to_vec();
        let decoded = Event::decode(encoded.as_slice()).expect("valid protobuf");

        assert_eq!(decoded, event);
        assert!(event.timestamp > 0);
        let anchor = decoded.observed_at_block.expect("anchored event");
        assert_eq!(anchor.hash, vec![0x11; 32]);
        assert_eq!(anchor.height, Some(996_259));
    }

    #[test]
    fn a_missing_height_survives_the_round_trip_as_absent() {
        // A disconnect names only the block, so an absent height has to stay
        // absent rather than decode as zero.
        let event = Event::new(
            MonitorEvent::Enforcer(payload()),
            Some(ObservedBlock::without_height(vec![0x22; 32])),
        )
        .expect("system clock after epoch");

        let decoded = Event::decode(event.encode_to_vec().as_slice()).expect("valid protobuf");

        let anchor = decoded.observed_at_block.expect("anchored event");
        assert_eq!(anchor.hash, vec![0x22; 32]);
        assert_eq!(anchor.height, None);
    }

    #[test]
    fn an_unanchored_event_round_trips_without_a_block() {
        let event =
            Event::new(MonitorEvent::Enforcer(payload()), None).expect("system clock after epoch");

        let decoded = Event::decode(event.encode_to_vec().as_slice()).expect("valid protobuf");

        assert!(decoded.observed_at_block.is_none());
    }
}
