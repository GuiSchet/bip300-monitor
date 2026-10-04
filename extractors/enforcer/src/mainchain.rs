//! One global stream covers transitions even when no sidechain exists.
use crate::{EnforcerClient, convert, proto::mainchain};
use anyhow::{Context, Result, bail};
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
    mut stream: tonic::Streaming<mainchain::SubscribeMainchainEventsResponse>,
    recorder: Recorder,
    block_tx: watch::Sender<Vec<u8>>,
    tip_tx: watch::Sender<ObservedBlock>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let mut previous: Option<(String, u64)> = None;
        let failure = loop {
            let item = tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                item = stream.message() => item,
            };
            let response = match item {
                Ok(Some(response)) => response,
                Ok(None) => break "global mainchain stream ended".to_owned(),
                Err(error) => break format!("global mainchain stream interrupted: {error}"),
            };
            let payload = match convert::mainchain_transition(response) {
                Ok(payload) => payload,
                Err(error) => break format!("invalid global mainchain event: {error:#}"),
            };
            let Some(events::enforcer_event::Event::MainchainTransition(transition)) =
                &payload.event
            else {
                unreachable!()
            };
            if let Err(error) = check_sequence(previous.as_ref(), transition) {
                break error.to_string();
            }
            previous = Some((transition.observer_session.clone(), transition.sequence));
            let anchor = transition
                .header
                .as_ref()
                .map(|h| ObservedBlock::at_height(h.hash.clone(), h.height));
            let tip = match (&transition.header, transition.action) {
                (Some(h), 1) => Some(ObservedBlock::at_height(h.hash.clone(), h.height)),
                (Some(h), 2) => h
                    .height
                    .checked_sub(1)
                    .map(|height| ObservedBlock::at_height(h.previous_hash.clone(), height)),
                _ => None,
            };
            let event = Event::new(MonitorEvent::Enforcer(payload), anchor)?;
            recorder
                .record(event)
                .await
                .context("recording global chain transition")?;
            if let Some(tip) = tip {
                let prior = tip_tx.borrow().clone();
                recorder
                    .record_tip_observation(
                        &tip,
                        Some(&prior),
                        CaptureMethod::Live,
                        SystemTime::now(),
                    )
                    .await?;
                block_tx.send_replace(tip.hash.clone());
                tip_tx.send_replace(tip);
            }
            recorder
                .record_worker_success(ExtractorWorker::MainchainEvents)
                .await?;
        };
        recorder
            .record_worker_failure(ExtractorWorker::MainchainEvents, &failure, 1)
            .await?;
        let mut delay = 1;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(delay)) => {},
            }
            match client.subscribe_mainchain_events().await {
                Ok(next) => {
                    stream = next;
                    break;
                }
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::MainchainEvents,
                            &format!("global resubscription failed: {error:#}"),
                            1,
                        )
                        .await?;
                    delay = (delay * 2).min(30);
                }
            }
        }
    }
}

fn check_sequence(
    previous: Option<&(String, u64)>,
    event: &events::MainchainTransition,
) -> Result<()> {
    match previous {
        None if event.action == 3 => Ok(()),
        Some((session, sequence))
            if (1..=2).contains(&event.action)
                && session == &event.observer_session
                && sequence.checked_add(1) == Some(event.sequence) =>
        {
            Ok(())
        }
        _ => bail!(
            "mainchain observation gap or missing subscription boundary; reconciliation required"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequence_gaps_and_new_sessions_are_not_silent_reorganizations() {
        let mut event = events::MainchainTransition {
            observer_session: "s".into(),
            action: 3,
            sequence: 100,
            ..Default::default()
        };
        assert!(check_sequence(None, &event).is_ok());
        event.action = 1;
        event.sequence = 101;
        assert!(check_sequence(Some(&("s".into(), 100)), &event).is_ok());
        assert!(check_sequence(Some(&("s".into(), 99)), &event).is_err());
        assert!(check_sequence(Some(&("other".into(), 100)), &event).is_err());
        assert!(check_sequence(None, &event).is_err());
    }
}
