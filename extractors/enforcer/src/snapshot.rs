//! Collection and publication of the enforcer's initial observable state.

use anyhow::{Result, bail};
use shared::nats::EventPublisher;
use shared::nats_subjects::Subject;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::enforcer_extractor::enforcer_event;

use crate::EnforcerClient;
use crate::convert;
use crate::event::envelope;
use crate::state;

/// Events collected from a set of unary RPCs and the tip they describe.
pub(crate) struct InitialSnapshot {
    /// Payloads that are constant for the process lifetime, plus the tip.
    pub(crate) constants: Vec<events::EnforcerEvent>,
    /// Payloads that change as the mainchain advances. Also seeds the state
    /// tracker, so the first refresh diffs against what was published here.
    pub(crate) state: Vec<events::EnforcerEvent>,
    pub(crate) tip_hash: Vec<u8>,
}

/// Fetch the initial observable state without publishing it.
pub(crate) async fn collect_snapshot(
    client: &mut EnforcerClient,
    sidechains: &[u8],
) -> Result<InitialSnapshot> {
    let mut constants = Vec::with_capacity(2);
    constants.push(convert::chain_info(client.get_chain_info().await?)?);
    let chain_tip = convert::chain_tip(client.get_chain_tip().await?)?;
    let tip_hash = tip_hash(&chain_tip)?;
    constants.push(chain_tip);

    let state = state::collect(client, sidechains).await?;

    Ok(InitialSnapshot {
        constants,
        state,
        tip_hash,
    })
}

/// Fetch only the current mainchain tip hash.
pub(crate) async fn current_tip_hash(client: &mut EnforcerClient) -> Result<Vec<u8>> {
    let chain_tip = convert::chain_tip(client.get_chain_tip().await?)?;
    tip_hash(&chain_tip)
}

/// Publish the complete snapshot and flush the batch with one bounded wait.
///
/// Takes the snapshot by reference because the caller keeps its mutable-state
/// payloads to seed the state tracker.
pub(crate) async fn publish_snapshot(
    publisher: &EventPublisher,
    snapshot: &InitialSnapshot,
) -> Result<()> {
    for payload in snapshot.constants.iter().chain(snapshot.state.iter()) {
        publisher
            .publish(Subject::Enforcer, &envelope(payload.clone())?)
            .await?;
    }
    publisher.flush().await?;

    Ok(())
}

fn tip_hash(event: &events::EnforcerEvent) -> Result<Vec<u8>> {
    let Some(enforcer_event::Event::ChainTip(chain_tip)) = event.event.as_ref() else {
        bail!("expected a chain-tip event while collecting the initial snapshot");
    };
    let Some(header) = chain_tip.header.as_ref() else {
        bail!("initial chain-tip event is missing its block header");
    };
    Ok(header.hash.clone())
}
