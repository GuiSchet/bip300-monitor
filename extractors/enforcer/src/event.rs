//! Construction of the common monitor envelope.

use anyhow::{Context, Result};
use shared::protobuf::enforcer_extractor::EnforcerEvent;
use shared::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};

pub(crate) fn envelope(payload: EnforcerEvent, observed_at_block: ObservedBlock) -> Result<Event> {
    Event::new(MonitorEvent::Enforcer(payload), Some(observed_at_block))
        .context("constructing the monitor event envelope")
}
