//! Top-level monitor event envelope.

#![allow(clippy::module_inception)]

use std::time::{SystemTime, SystemTimeError};

include!(concat!(env!("OUT_DIR"), "/event.rs"));

impl Event {
    /// Wrap an extractor event with the current Unix timestamp in milliseconds.
    pub fn new(event: event::MonitorEvent) -> Result<Self, SystemTimeError> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

        Ok(Self {
            timestamp: now.as_millis() as u64,
            monitor_event: Some(event),
        })
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{Event, event::MonitorEvent};
    use crate::protobuf::enforcer_extractor::{ChainInfo, EnforcerEvent, Network, enforcer_event};

    #[test]
    fn event_round_trips_through_protobuf() {
        let payload = EnforcerEvent {
            event: Some(enforcer_event::Event::ChainInfo(ChainInfo {
                network: Network::Regtest as i32,
                bip300_constants: None,
            })),
        };
        let event = Event::new(MonitorEvent::Enforcer(payload)).expect("system clock after epoch");

        let encoded = event.encode_to_vec();
        let decoded = Event::decode(encoded.as_slice()).expect("valid protobuf");

        assert_eq!(decoded, event);
        assert!(event.timestamp > 0);
    }
}
