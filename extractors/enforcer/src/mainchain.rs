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
    // Upstream never replays: whatever happened since the previous run's last
    // tip is unknown, and so is everything between a lost stream and its
    // replacement. Each (re)subscription records that gap as a boundary.
    let mut gap_start = recorder.previous_run_tip().cloned();
    loop {
        let mut bounded = false;
        let failure = match subscription_boundary(&mut client, gap_start.as_ref()).await {
            Err(error) => format!("subscription boundary: {error:#}"),
            Ok((payload, anchor)) => {
                recorder
                    .record(Event::new(MonitorEvent::Enforcer(payload), Some(anchor))?)
                    .await?;
                bounded = true;
                // A live subscription with its boundary recorded is healthy even
                // before the first block, which can take long on Betanet.
                recorder
                    .record_worker_success(ExtractorWorker::MainchainEvents)
                    .await?;
                loop {
                    let item = tokio::select! { _=shutdown.changed()=>return Ok(()), item=stream.message()=>item };
                    let response = match item {
                        Ok(Some(r)) => r,
                        Ok(None) => break "official global subscription ended".to_owned(),
                        Err(error) => {
                            break format!("official global subscription interrupted: {error}");
                        }
                    };
                    let (payload, anchor) = global_event(response)?;
                    recorder
                        .record(Event::new(MonitorEvent::Enforcer(payload), Some(anchor))?)
                        .await?;
                    reconcile_tip(&mut client, &recorder, &block_tx, &tip_tx).await?;
                }
            }
        };
        // Captured now: the tip poll keeps advancing while the stream is down.
        // A gap whose boundary was never recorded keeps its original start.
        if bounded {
            gap_start = Some(tip_tx.borrow().clone());
        }
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

/// Stream delivery has no atomic baseline/revision. Ask for the actual enforcer
/// tip instead of promoting a buffered event to current tip, and report only a
/// tip that moved: an unchanged tip would wake every tip consumer for nothing.
async fn reconcile_tip(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    block_tx: &watch::Sender<Vec<u8>>,
    tip_tx: &watch::Sender<ObservedBlock>,
) -> Result<()> {
    match client.get_chain_tip().await.and_then(convert::chain_tip) {
        Ok(payload) => {
            let tip = crate::state::tip_anchor(&payload)?;
            let prior = tip_tx.borrow().clone();
            if tip != prior {
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
            }
            recorder
                .record_worker_success(ExtractorWorker::MainchainEvents)
                .await
        }
        Err(error) => {
            recorder
                .record_worker_failure(
                    ExtractorWorker::MainchainEvents,
                    &format!("tip reconciliation: {error:#}"),
                    1,
                )
                .await?;
            Ok(())
        }
    }
}

/// The boundary that opens a subscription: the tip read now ends the gap that
/// began at `gap_start` (absent only when a dataset's evidence begins here).
async fn subscription_boundary(
    client: &mut EnforcerClient,
    gap_start: Option<&ObservedBlock>,
) -> Result<(events::EnforcerEvent, ObservedBlock)> {
    let tip = convert::chain_tip(client.get_chain_tip().await?)?;
    boundary_event(tip, gap_start)
}

fn boundary_event(
    tip: events::EnforcerEvent,
    gap_start: Option<&ObservedBlock>,
) -> Result<(events::EnforcerEvent, ObservedBlock)> {
    let Some(events::enforcer_event::Event::ChainTip(tip)) = tip.event else {
        anyhow::bail!("expected a chain tip for the subscription boundary");
    };
    let header = tip.header.context("chain tip without header")?;
    let anchor = ObservedBlock::at_height(header.hash.clone(), header.height);
    let gap_start = gap_start
        .map(|block| -> Result<events::BlockRef> {
            Ok(events::BlockRef {
                hash: block.hash.clone(),
                height: block.height.context("gap start without height")?,
            })
        })
        .transpose()?;
    Ok((
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::MainchainTransition(
                events::MainchainTransition {
                    action: SUBSCRIPTION_BOUNDARY,
                    header: Some(header),
                    gap_start,
                },
            )),
        },
        anchor,
    ))
}

const SUBSCRIPTION_BOUNDARY: i32 = 3;

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

    fn tip_payload(height: u32, hash: u8) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::ChainTip(events::ChainTip {
                header: Some(events::BlockHeader {
                    hash: vec![hash; 32],
                    previous_hash: vec![hash.wrapping_sub(1); 32],
                    height,
                    ..Default::default()
                }),
            })),
        }
    }

    #[test]
    fn a_subscription_boundary_names_the_gap_it_closes() {
        let start = ObservedBlock::at_height(vec![7; 32], 40);
        let (payload, anchor) = boundary_event(tip_payload(45, 9), Some(&start)).unwrap();
        assert_eq!(anchor, ObservedBlock::at_height(vec![9; 32], 45));
        let Some(events::enforcer_event::Event::MainchainTransition(t)) = payload.event else {
            panic!("boundary transition expected")
        };
        assert_eq!(t.action, SUBSCRIPTION_BOUNDARY);
        assert_eq!(t.header.unwrap().height, 45);
        assert_eq!(
            t.gap_start,
            Some(events::BlockRef {
                hash: vec![7; 32],
                height: 40
            })
        );

        // The first subscription of a dataset starts evidence; it closes no gap.
        let (payload, _) = boundary_event(tip_payload(45, 9), None).unwrap();
        let Some(events::enforcer_event::Event::MainchainTransition(t)) = payload.event else {
            panic!("boundary transition expected")
        };
        assert!(t.gap_start.is_none());

        // A gap start without a height cannot be bounded.
        assert!(
            boundary_event(
                tip_payload(45, 9),
                Some(&ObservedBlock::without_height(vec![7; 32]))
            )
            .is_err()
        );
    }
}
