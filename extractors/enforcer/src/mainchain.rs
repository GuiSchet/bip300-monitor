//! Global observations via the official slot subscription. Slot 0 is a transport
//! selector only: these records never claim a sidechain instance or server revision.
use crate::{EnforcerClient, convert, proto::mainchain};
use anyhow::{Context, Result};
use shared::{
    protobuf::{
        enforcer_extractor as events,
        event::{Event, ObservedBlock, event::MonitorEvent},
    },
    recorder::Recorder,
    store::{CaptureMethod, ExtractorWorker},
};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

pub(crate) async fn monitor(
    mut client: EnforcerClient,
    mut stream: tonic::Streaming<mainchain::SubscribeEventsResponse>,
    recorder: Recorder,
    block_tx: watch::Sender<Vec<u8>>,
    tip_tx: watch::Sender<ObservedBlock>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let failure = loop {
            let item =
                tokio::select! { _=shutdown.changed()=>return Ok(()), item=stream.message()=>item };
            let response = match item {
                Ok(Some(r)) => r,
                Ok(None) => break "official global subscription ended".to_owned(),
                Err(error) => break format!("official global subscription interrupted: {error}"),
            };
            let (payload, anchor) = global_event(response)?;
            recorder
                .record(Event::new(MonitorEvent::Enforcer(payload), Some(anchor))?)
                .await?;
            // Stream delivery has no atomic baseline/revision. Ask for the actual
            // enforcer tip instead of promoting a buffered event to current tip.
            match client.get_chain_tip().await.and_then(convert::chain_tip) {
                Ok(payload) => {
                    let tip = crate::state::tip_anchor(&payload)?;
                    let prior = tip_tx.borrow().clone();
                    recorder
                        .record_tip_observation(
                            &tip,
                            Some(&prior),
                            CaptureMethod::Poll,
                            SystemTime::now(),
                        )
                        .await?;
                    block_tx.send_replace(tip.hash.clone());
                    tip_tx.send_replace(tip);
                    recorder
                        .record_worker_success(ExtractorWorker::MainchainEvents)
                        .await?;
                }
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::MainchainEvents,
                            &format!("tip reconciliation: {error:#}"),
                            1,
                        )
                        .await?;
                }
            }
        };
        recorder
            .record_worker_failure(ExtractorWorker::MainchainEvents, &failure, 1)
            .await?;
        let mut delay = 1;
        loop {
            tokio::select! { _=shutdown.changed()=>return Ok(()), _=tokio::time::sleep(Duration::from_secs(delay))=>{} }
            match client.subscribe_events(0).await {
                Ok(next) => {
                    stream = next;
                    break;
                }
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::MainchainEvents,
                            &format!("resubscription: {error:#}"),
                            1,
                        )
                        .await?;
                    delay = (delay * 2).min(30);
                }
            }
        }
    }
}
fn global_event(
    response: mainchain::SubscribeEventsResponse,
) -> Result<(events::EnforcerEvent, ObservedBlock)> {
    let event = convert::subscription_event(0, response)?;
    let (header, anchor, action) = match event.event.context("missing subscription event")? {
        events::enforcer_event::Event::BlockConnected(block) => {
            let h = block.header.context("connection without header")?;
            let anchor = ObservedBlock::at_height(h.hash.clone(), h.height);
            (Some(h), anchor, 1)
        }
        events::enforcer_event::Event::BlockDisconnected(block) => {
            (None, ObservedBlock::without_height(block.block_hash), 2)
        }
        _ => anyhow::bail!("unexpected subscription event"),
    };
    Ok((
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::MainchainTransition(
                events::MainchainTransition {
                    action,
                    header,
                    gap_start: None,
                },
            )),
        },
        anchor,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common;
    #[test]
    fn empty_slot_transport_still_reports_global_connect_and_disconnect() {
        use mainchain::subscribe_events_response::{Event, event};
        let response = mainchain::SubscribeEventsResponse {
            event: Some(Event {
                event: Some(event::Event::ConnectBlock(event::ConnectBlock {
                    header_info: Some(mainchain::BlockHeaderInfo {
                        block_hash: Some(common::ReverseHex {
                            hex: Some("11".repeat(32)),
                        }),
                        prev_block_hash: Some(common::ReverseHex {
                            hex: Some("22".repeat(32)),
                        }),
                        height: 42,
                        work: Some(common::ConsensusHex {
                            hex: Some("01".repeat(32)),
                        }),
                        timestamp: 1,
                    }),
                    block_info: Some(mainchain::BlockInfo {
                        bmm_commitment: None,
                        events: vec![],
                    }),
                })),
            }),
        };
        let (payload, anchor) = global_event(response).unwrap();
        assert_eq!(anchor.height, Some(42));
        let Some(events::enforcer_event::Event::MainchainTransition(t)) = payload.event else {
            panic!("global event expected")
        };
        assert_eq!(t.action, 1);
        assert!(t.gap_start.is_none());
        let response = mainchain::SubscribeEventsResponse {
            event: Some(Event {
                event: Some(event::Event::DisconnectBlock(event::DisconnectBlock {
                    block_hash: Some(common::ReverseHex {
                        hex: Some("11".repeat(32)),
                    }),
                })),
            }),
        };
        let (payload, anchor) = global_event(response).unwrap();
        assert_eq!(anchor.height, None);
        let Some(events::enforcer_event::Event::MainchainTransition(t)) = payload.event else {
            panic!("global event expected")
        };
        assert_eq!(t.action, 2);
        assert!(t.header.is_none());
    }
}
