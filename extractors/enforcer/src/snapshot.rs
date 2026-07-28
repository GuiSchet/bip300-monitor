//! One-shot publication of the enforcer's initial observable state.

use anyhow::{Context, Result};
use shared::nats::EventPublisher;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor::EnforcerEvent;
use shared::protobuf::event::{Event, event::MonitorEvent};

use crate::EnforcerClient;
use crate::config::Args;
use crate::convert;

/// Fetch and publish one initial snapshot, then return.
pub async fn publish_initial_snapshot(args: Args) -> Result<()> {
    args.validate()
        .context("validating extractor configuration")?;

    let mut client = EnforcerClient::connect(&args.enforcer_endpoint, args.request_timeout())
        .await
        .context("connecting the initial snapshot client")?;

    let mut events = Vec::with_capacity(4 + args.sidechains.len());
    events.push(envelope(convert::chain_info(
        client.get_chain_info().await?,
    )?)?);
    events.push(envelope(convert::chain_tip(
        client.get_chain_tip().await?,
    )?)?);
    events.push(envelope(convert::sidechain_proposals(
        client.get_sidechain_proposals().await?,
    )?)?);
    events.push(envelope(convert::active_sidechains(
        client.get_sidechains().await?,
    )?)?);
    for sidechain in &args.sidechains {
        events.push(envelope(convert::ctip(
            *sidechain,
            client.get_ctip(*sidechain).await?,
        )?)?);
    }

    let publisher = EventPublisher::connect(&args.nats)
        .await
        .context("connecting the initial snapshot publisher")?;
    for event in events {
        publisher.publish(Subject::Enforcer, &event).await?;
    }
    publisher
        .flush()
        .await
        .context("flushing the initial snapshot")?;

    Ok(())
}

fn envelope(payload: EnforcerEvent) -> Result<Event> {
    Event::new(MonitorEvent::Enforcer(payload)).context("constructing the monitor event envelope")
}
