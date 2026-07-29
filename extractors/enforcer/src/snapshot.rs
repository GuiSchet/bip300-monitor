//! Collection and publication of the enforcer's initial observable state.

use anyhow::{Result, bail};
use shared::nats::EventPublisher;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor::enforcer_event;
use shared::protobuf::event::Event;

use crate::EnforcerClient;
use crate::convert;
use crate::event::envelope;

/// Events collected from a set of unary RPCs and the tip they describe.
pub(crate) struct InitialSnapshot {
    pub(crate) events: Vec<Event>,
    pub(crate) tip_hash: Vec<u8>,
}

/// Fetch the initial observable state without publishing it.
pub(crate) async fn collect_snapshot(
    client: &mut EnforcerClient,
    sidechains: &[u8],
) -> Result<InitialSnapshot> {
    let mut events = Vec::with_capacity(4 + sidechains.len());
    events.push(envelope(convert::chain_info(
        client.get_chain_info().await?,
    )?)?);
    let chain_tip = convert::chain_tip(client.get_chain_tip().await?)?;
    let tip_hash = tip_hash(&chain_tip)?;
    events.push(envelope(chain_tip)?);
    events.push(envelope(convert::sidechain_proposals(
        client.get_sidechain_proposals().await?,
    )?)?);
    events.push(envelope(convert::active_sidechains(
        client.get_sidechains().await?,
    )?)?);
    for sidechain in sidechains {
        events.push(envelope(convert::ctip(
            *sidechain,
            client.get_ctip(*sidechain).await?,
        )?)?);
    }

    Ok(InitialSnapshot { events, tip_hash })
}

/// Fetch only the current mainchain tip hash.
pub(crate) async fn current_tip_hash(client: &mut EnforcerClient) -> Result<Vec<u8>> {
    let chain_tip = convert::chain_tip(client.get_chain_tip().await?)?;
    tip_hash(&chain_tip)
}

/// Publish the complete snapshot and flush the batch with one bounded wait.
pub(crate) async fn publish_snapshot(
    publisher: &EventPublisher,
    snapshot: InitialSnapshot,
) -> Result<()> {
    for event in snapshot.events {
        publisher.publish(Subject::Enforcer, &event).await?;
    }
    publisher.flush().await?;

    Ok(())
}

fn tip_hash(event: &shared::protobuf::enforcer_extractor::EnforcerEvent) -> Result<Vec<u8>> {
    let Some(enforcer_event::Event::ChainTip(chain_tip)) = event.event.as_ref() else {
        bail!("expected a chain-tip event while collecting the initial snapshot");
    };
    let Some(header) = chain_tip.header.as_ref() else {
        bail!("initial chain-tip event is missing its block header");
    };
    Ok(header.hash.clone())
}
