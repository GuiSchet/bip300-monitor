//! Construction of the common monitor envelope.

use anyhow::{Context, Result};
use shared::protobuf::enforcer_extractor::EnforcerEvent;
use shared::protobuf::event::{Event, event::MonitorEvent};

pub(crate) fn envelope(payload: EnforcerEvent) -> Result<Event> {
    Event::new(MonitorEvent::Enforcer(payload)).context("constructing the monitor event envelope")
}
