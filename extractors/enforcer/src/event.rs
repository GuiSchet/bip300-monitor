//! Construction of the common monitor envelope.

use anyhow::{Context, Result};
use shared::protobuf::enforcer_extractor::EnforcerEvent;
use shared::protobuf::event::{Event, ObservedBlock, event::MonitorEvent};

pub(crate) fn envelope(payload: EnforcerEvent, observed_at_block: ObservedBlock) -> Result<Event> {
    let state_read = matches!(
        payload.event.as_ref().map(|e| e.kind()),
        Some("sidechain_proposals" | "active_sidechains" | "ctip" | "withdrawal_bundle_proposals")
    );
    Event::new(
        MonitorEvent::Enforcer(payload),
        (!state_read).then_some(observed_at_block),
    )
    .context("constructing the monitor event envelope")
}
