//! Collection and publication of the enforcer's initial observable state.

use anyhow::Result;
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::ObservedBlock;
use shared::recorder::Recorder;

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
    /// The tip every payload above is anchored to.
    pub(crate) anchor: ObservedBlock,
}

/// Fetch the initial observable state without publishing it.
pub(crate) async fn collect_snapshot(
    client: &mut EnforcerClient,
    sidechains: &[u8],
) -> Result<InitialSnapshot> {
    let chain_tip = convert::chain_tip(client.get_chain_tip().await?)?;
    let anchor = state::tip_anchor(&chain_tip)?;
    let constants = vec![
        convert::chain_info(client.get_chain_info().await?)?,
        chain_tip,
    ];

    let state = state::collect_payloads(client, sidechains).await?;

    Ok(InitialSnapshot {
        constants,
        state,
        anchor,
    })
}

/// Fetch only the current mainchain tip.
pub(crate) async fn current_tip(client: &mut EnforcerClient) -> Result<ObservedBlock> {
    state::tip_anchor(&convert::chain_tip(client.get_chain_tip().await?)?)
}

/// Record the complete snapshot in one transaction.
///
/// Takes the snapshot by reference because the caller keeps its mutable-state
/// payloads to seed the state tracker.
pub(crate) async fn record_snapshot(recorder: &Recorder, snapshot: &InitialSnapshot) -> Result<()> {
    let events = snapshot
        .constants
        .iter()
        .chain(snapshot.state.iter())
        .map(|payload| envelope(payload.clone(), snapshot.anchor.clone()))
        .collect::<Result<Vec<_>>>()?;

    recorder.record_batch(events).await
}
