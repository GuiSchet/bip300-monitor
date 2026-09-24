//! Shared bounded, resumable historical-walk engine.
//!
//! Both the slot block stream and the global BIP300-delta stream walk the same
//! newest-first cursor. Stream adapters supply only the RPC fetch and header
//! accessor; persistence, retry, reorg, and shutdown semantics live here once.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Error, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::{Event, ObservedBlock};
use shared::recorder::Recorder;
use shared::store::{HistoryCoverage, HistoryPage, HistoryStatus, SidechainInstanceRef};
use tokio::sync::watch;
use tonic::Code;

use crate::EnforcerClient;
use crate::convert;
use crate::event::envelope;
use crate::snapshot;

const HISTORY_STREAM: &str = "block";

#[derive(Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) page_blocks: u32,
    pub(crate) page_pause: Duration,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    UpToDate {
        target: ObservedBlock,
    },
    Completed {
        target: ObservedBlock,
        blocks: usize,
        pages: usize,
    },
    Deferred {
        target: ObservedBlock,
        blocks: usize,
        pages: usize,
    },
    Superseded {
        target: ObservedBlock,
        blocks: usize,
        pages: usize,
    },
    Interrupted {
        target: ObservedBlock,
        blocks: usize,
        pages: usize,
    },
}

pub(crate) struct HistoryScope<'a> {
    pub(crate) stream: &'static str,
    pub(crate) sidechain: Option<u8>,
    pub(crate) sidechain_instance_id: Option<&'a str>,
    pub(crate) activation_height: u32,
    pub(crate) expected_start_hash: Option<&'a [u8]>,
}

type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<events::EnforcerEvent>>> + Send + 'a>>;

/// The two operations that differ between historical streams.
pub(crate) trait HistoryStream {
    fn scope(&self) -> HistoryScope<'_>;

    fn fetch<'a>(
        &'a self,
        client: &'a mut EnforcerClient,
        cursor: &'a ObservedBlock,
        requested: u32,
    ) -> FetchFuture<'a>;

    fn header<'a>(&self, payload: &'a events::EnforcerEvent) -> Result<&'a events::BlockHeader>;

    fn unavailable_error(&self, error: &Error) -> bool {
        error_has_code(error, Code::NotFound)
    }

    fn inconclusive_probe_error(&self, error: &Error) -> bool {
        retryable_rpc_error(error)
    }
}

struct BlockHistory<'a> {
    instance: &'a SidechainInstanceRef,
}

impl HistoryStream for BlockHistory<'_> {
    fn scope(&self) -> HistoryScope<'_> {
        HistoryScope {
            stream: HISTORY_STREAM,
            sidechain: Some(self.instance.sidechain),
            sidechain_instance_id: Some(&self.instance.sidechain_instance_id),
            activation_height: self.instance.activation_height,
            expected_start_hash: None,
        }
    }

    fn fetch<'a>(
        &'a self,
        client: &'a mut EnforcerClient,
        cursor: &'a ObservedBlock,
        requested: u32,
    ) -> FetchFuture<'a> {
        Box::pin(async move {
            let response = client
                .get_block_info(
                    hex::encode(&cursor.hash),
                    self.instance.sidechain,
                    Some(requested - 1),
                )
                .await?;
            convert::block_info(self.instance.sidechain, response)
        })
    }

    fn header<'a>(&self, payload: &'a events::EnforcerEvent) -> Result<&'a events::BlockHeader> {
        connected_header(payload)
    }
}

/// Recover one slot from its activation height through `tip`.
pub(crate) async fn run(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    instance: &SidechainInstanceRef,
    tip: &ObservedBlock,
    settings: Settings,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Outcome> {
    run_history(
        client,
        recorder,
        &BlockHistory { instance },
        tip,
        settings,
        shutdown_rx,
    )
    .await
}

/// Run the common newest-first historical cursor for one stream adapter.
pub(crate) async fn run_history<S: HistoryStream>(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    stream: &S,
    tip: &ObservedBlock,
    settings: Settings,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<Outcome> {
    let scope = stream.scope();
    if !scope_is_current(recorder, &scope).await? {
        mark_superseded(recorder, &scope).await?;
        return Ok(Outcome::Superseded {
            target: tip.clone(),
            blocks: 0,
            pages: 0,
        });
    }
    let Some(mut progress) = prepare_cycle(recorder, &scope, tip, settings.page_blocks).await?
    else {
        return Ok(Outcome::UpToDate {
            target: tip.clone(),
        });
    };

    let mut blocks = 0_usize;
    let mut pages = 0_usize;
    let mut restarted_from_activation = progress.floor_hash.is_none();

    loop {
        if *shutdown_rx.borrow() {
            return Ok(Outcome::Interrupted {
                target: progress.target_tip.clone(),
                blocks,
                pages,
            });
        }
        if !scope_is_current(recorder, &scope).await? {
            mark_superseded(recorder, &scope).await?;
            return Ok(Outcome::Superseded {
                target: progress.target_tip.clone(),
                blocks,
                pages,
            });
        }

        let cursor = progress
            .next
            .clone()
            .context("a running history cursor is missing its next block")?;
        let cursor_height = required_height(&cursor, "history cursor")?;
        let remaining = blocks_through_floor(cursor_height, progress.floor_height)?;
        let requested = u32::try_from(remaining.min(u64::from(progress.effective_page_blocks)))
            .expect("a page size fits in a u32");

        enum Page {
            Found(Vec<events::EnforcerEvent>),
            Unavailable(Error),
        }
        let page = match stream.fetch(client, &cursor, requested).await {
            Ok(payloads) if payloads.is_empty() => Page::Unavailable(anyhow::anyhow!(
                "history cursor {} was not found",
                hex::encode(&cursor.hash)
            )),
            Ok(payloads) => Page::Found(payloads),
            Err(error) if stream.unavailable_error(&error) => Page::Unavailable(error),
            Err(error) if page_too_large(&error) && progress.effective_page_blocks > 1 => {
                let previous_page_blocks = progress.effective_page_blocks;
                let reduced = (previous_page_blocks / 2).max(1);
                let message = format!("{error:#}");
                if let Err(resize_error) = recorder
                    .store()
                    .resize_history_page(
                        scope.stream,
                        scope.sidechain,
                        scope.sidechain_instance_id,
                        reduced,
                        &message,
                    )
                    .await
                {
                    return settle_failure(
                        recorder,
                        &scope,
                        resize_error.context("persisting the reduced history page size"),
                        &progress.target_tip,
                        blocks,
                        pages,
                        FailureMode::Fatal,
                    )
                    .await;
                }
                progress.effective_page_blocks = reduced;
                tracing::warn!(
                    stream = scope.stream,
                    sidechain = ?scope.sidechain,
                    previous_page_blocks,
                    effective_page_blocks = reduced,
                    error = %message,
                    "history request was too large; retrying the same cursor"
                );
                continue;
            }
            Err(error) if retryable_rpc_error(&error) => {
                return settle_failure(
                    recorder,
                    &scope,
                    error.context("the history page request failed transiently"),
                    &progress.target_tip,
                    blocks,
                    pages,
                    FailureMode::Deferred,
                )
                .await;
            }
            Err(error) => {
                return settle_failure(
                    recorder,
                    &scope,
                    error,
                    &progress.target_tip,
                    blocks,
                    pages,
                    FailureMode::Fatal,
                )
                .await;
            }
        };

        let payloads = match page {
            Page::Found(payloads) => payloads,
            Page::Unavailable(error) => {
                let current_tip = match snapshot::current_tip(client).await {
                    Ok(current_tip) => current_tip,
                    Err(tip_error) if retryable_rpc_error(&tip_error) => {
                        return settle_failure(
                            recorder,
                            &scope,
                            tip_error.context(format!(
                                "reading the current tip after unavailable history cursor: {error:#}"
                            )),
                            &progress.target_tip,
                            blocks,
                            pages,
                            FailureMode::Deferred,
                        )
                        .await;
                    }
                    Err(tip_error) => {
                        return settle_failure(
                            recorder,
                            &scope,
                            tip_error.context(
                                "reading the current tip after an unavailable history cursor",
                            ),
                            &progress.target_tip,
                            blocks,
                            pages,
                            FailureMode::Fatal,
                        )
                        .await;
                    }
                };
                let target_availability = if current_tip.hash == progress.target_tip.hash {
                    Some(true)
                } else {
                    match target_is_available(stream, client, &progress.target_tip).await {
                        Ok(availability) => availability,
                        Err(probe_error) => {
                            tracing::warn!(
                                stream = scope.stream,
                                sidechain = ?scope.sidechain,
                                error = %format!("{probe_error:#}"),
                                "could not classify the persisted target; preserving its cursor"
                            );
                            None
                        }
                    }
                };
                let replacement =
                    replacement_target(&current_tip, &progress.target_tip, target_availability);
                if let Some(current_tip) = replacement {
                    tracing::warn!(
                        stream = scope.stream,
                        sidechain = ?scope.sidechain,
                        unavailable_cursor = %hex::encode(&cursor.hash),
                        previous_target = %hex::encode(&progress.target_tip.hash),
                        current_target = %hex::encode(&current_tip.hash),
                        "historical target left the available branch; restarting from activation"
                    );
                    progress = begin_full_cycle(
                        recorder,
                        &scope,
                        &current_tip,
                        progress.effective_page_blocks,
                    )
                    .await?;
                    restarted_from_activation = true;
                    continue;
                }

                let outcome = settle_failure(
                    recorder,
                    &scope,
                    error,
                    &progress.target_tip,
                    blocks,
                    pages,
                    FailureMode::Deferred,
                )
                .await?;
                if matches!(outcome, Outcome::Deferred { .. }) {
                    tracing::warn!(
                        stream = scope.stream,
                        sidechain = ?scope.sidechain,
                        unavailable_cursor = %hex::encode(&cursor.hash),
                        target = %hex::encode(&progress.target_tip.hash),
                        "historical cursor is unavailable; preserving its exact target for retry"
                    );
                }
                return Ok(outcome);
            }
        };

        if let Err(error) = verify_page_with(stream, &payloads, &cursor, requested) {
            return settle_failure(
                recorder,
                &scope,
                error.context("validating a historical page"),
                &progress.target_tip,
                blocks,
                pages,
                FailureMode::Fatal,
            )
            .await;
        }
        let oldest = stream.header(
            payloads
                .last()
                .context("a verified historical page is not empty")?,
        )?;
        let returned = u32::try_from(payloads.len()).expect("a page length fits in a u32");
        let completes_cycle = u64::from(returned) == remaining;

        if let Err(error) = validate_expected_start_hash(
            &scope,
            progress.floor_hash.as_deref(),
            completes_cycle,
            &oldest.hash,
        ) {
            return settle_failure(
                recorder,
                &scope,
                error,
                &progress.target_tip,
                blocks,
                pages,
                FailureMode::Fatal,
            )
            .await;
        }

        if completes_cycle
            && let Some(floor_hash) = progress.floor_hash.as_deref()
            && oldest.previous_hash != floor_hash
        {
            if restarted_from_activation {
                return settle_failure(
                    recorder,
                    &scope,
                    anyhow::anyhow!(
                        "history branch still failed to reach its floor after restarting from activation"
                    ),
                    &progress.target_tip,
                    blocks,
                    pages,
                    FailureMode::Fatal,
                )
                .await;
            }
            tracing::warn!(
                stream = scope.stream,
                sidechain = ?scope.sidechain,
                expected_floor_hash = %hex::encode(floor_hash),
                actual_floor_hash = %hex::encode(&oldest.previous_hash),
                "covered tip is not an ancestor of the target; restarting from activation"
            );
            progress = begin_full_cycle(
                recorder,
                &scope,
                &progress.target_tip,
                progress.effective_page_blocks,
            )
            .await?;
            restarted_from_activation = true;
            continue;
        }

        let next = if completes_cycle {
            None
        } else {
            Some(ObservedBlock::at_height(
                oldest.previous_hash.clone(),
                oldest
                    .height
                    .checked_sub(1)
                    .context("a non-final history page cannot end at genesis")?,
            ))
        };
        let events = historical_events(stream, payloads)?;
        let inserted = match recorder
            .store()
            .record_history_page(
                &events,
                HistoryPage {
                    stream: scope.stream,
                    sidechain: scope.sidechain,
                    sidechain_instance_id: scope.sidechain_instance_id,
                    expected_next: &cursor,
                    next: next.as_ref(),
                },
            )
            .await
        {
            Ok(inserted) => inserted,
            Err(error) => {
                return settle_failure(
                    recorder,
                    &scope,
                    error.context("recording a historical page"),
                    &progress.target_tip,
                    blocks,
                    pages,
                    FailureMode::Fatal,
                )
                .await;
            }
        };

        pages += 1;
        blocks += events.len();
        progress.rows_recorded += inserted;
        progress.next = next;
        tracing::debug!(
            stream = scope.stream,
            sidechain = ?scope.sidechain,
            pages,
            blocks,
            inserted,
            requested,
            returned,
            cursor_height,
            target_height = ?progress.target_tip.height,
            "committed a historical page"
        );

        if progress.next.is_none() {
            tracing::info!(
                stream = scope.stream,
                sidechain = ?scope.sidechain,
                pages,
                blocks,
                rows_recorded = progress.rows_recorded,
                start_height = scope.activation_height,
                target_height = ?progress.target_tip.height,
                "completed contiguous history"
            );
            return Ok(Outcome::Completed {
                target: progress.target_tip.clone(),
                blocks,
                pages,
            });
        }

        if !settings.page_pause.is_zero() {
            tokio::select! {
                biased;
                () = wait_for_shutdown(&mut shutdown_rx) => {
                    return Ok(Outcome::Interrupted {
                        target: progress.target_tip.clone(),
                        blocks,
                        pages,
                    });
                }
                () = tokio::time::sleep(settings.page_pause) => {}
            }
        }
    }
}

async fn prepare_cycle(
    recorder: &Recorder,
    scope: &HistoryScope<'_>,
    tip: &ObservedBlock,
    configured_page_blocks: u32,
) -> Result<Option<HistoryCoverage>> {
    let tip_height = required_height(tip, "history target")?;
    if tip_height < scope.activation_height {
        bail!(
            "history stream {} starts at {}, after target tip {tip_height}",
            scope.stream,
            scope.activation_height
        );
    }
    let existing = recorder
        .store()
        .history_coverage(scope.stream, scope.sidechain, scope.sidechain_instance_id)
        .await
        .with_context(|| format!("reading {} history coverage", scope.stream))?;
    let Some(existing) = existing else {
        return begin_full_cycle(recorder, scope, tip, configured_page_blocks)
            .await
            .map(Some);
    };
    if existing.coverage_start_height != scope.activation_height {
        bail!(
            "history stream {} starts at {}, expected {}",
            scope.stream,
            existing.coverage_start_height,
            scope.activation_height
        );
    }

    match existing.status {
        HistoryStatus::Running => Ok(Some(existing)),
        HistoryStatus::Error | HistoryStatus::Superseded => {
            recorder
                .store()
                .resume_history(scope.stream, scope.sidechain, scope.sidechain_instance_id)
                .await
                .with_context(|| format!("resuming {} history", scope.stream))?;
            let mut resumed = existing;
            resumed.status = HistoryStatus::Running;
            resumed.last_error = None;
            Ok(Some(resumed))
        }
        HistoryStatus::Complete => {
            let covered = existing
                .covered_tip
                .clone()
                .context("completed history coverage has no covered tip")?;
            if covered.hash == tip.hash {
                return Ok(None);
            }
            let page_blocks = existing.effective_page_blocks.min(configured_page_blocks);
            if tip_height > required_height(&covered, "covered history tip")? {
                recorder
                    .store()
                    .begin_history_cycle(
                        scope.stream,
                        scope.sidechain,
                        scope.sidechain_instance_id,
                        scope.activation_height,
                        Some(&covered),
                        tip,
                        Some(&covered.hash),
                        covered.height,
                        page_blocks,
                    )
                    .await
                    .with_context(|| format!("extending {} history", scope.stream))
                    .map(Some)
            } else {
                begin_full_cycle(recorder, scope, tip, page_blocks)
                    .await
                    .map(Some)
            }
        }
    }
}

async fn begin_full_cycle(
    recorder: &Recorder,
    scope: &HistoryScope<'_>,
    tip: &ObservedBlock,
    page_blocks: u32,
) -> Result<HistoryCoverage> {
    recorder
        .store()
        .begin_history_cycle(
            scope.stream,
            scope.sidechain,
            scope.sidechain_instance_id,
            scope.activation_height,
            None,
            tip,
            None,
            scope.activation_height.checked_sub(1),
            page_blocks,
        )
        .await
        .with_context(|| format!("starting full {} history", scope.stream))
}

async fn target_is_available<S: HistoryStream>(
    stream: &S,
    client: &mut EnforcerClient,
    target: &ObservedBlock,
) -> Result<Option<bool>> {
    match stream.fetch(client, target, 1).await {
        Ok(payloads) => Ok(Some(!payloads.is_empty())),
        Err(error) if stream.inconclusive_probe_error(&error) => Ok(None),
        Err(error) if stream.unavailable_error(&error) => Ok(Some(false)),
        Err(error) => Err(error).context("probing the persisted history target"),
    }
}

fn replacement_target(
    current_tip: &ObservedBlock,
    persisted_target: &ObservedBlock,
    target_availability: Option<bool>,
) -> Option<ObservedBlock> {
    (current_tip.hash != persisted_target.hash && target_availability == Some(false))
        .then(|| current_tip.clone())
}

async fn scope_is_current(recorder: &Recorder, scope: &HistoryScope<'_>) -> Result<bool> {
    let (Some(sidechain), Some(instance_id)) = (scope.sidechain, scope.sidechain_instance_id)
    else {
        return Ok(true);
    };
    Ok(recorder
        .store()
        .current_sidechain_instance_id(sidechain)
        .await?
        .as_deref()
        == Some(instance_id))
}

async fn mark_superseded(recorder: &Recorder, scope: &HistoryScope<'_>) -> Result<()> {
    let (Some(sidechain), Some(instance_id)) = (scope.sidechain, scope.sidechain_instance_id)
    else {
        return Ok(());
    };
    recorder
        .store()
        .supersede_history(
            scope.stream,
            sidechain,
            instance_id,
            "sidechain instance is no longer active",
        )
        .await
}

fn historical_events<S: HistoryStream>(
    stream: &S,
    mut payloads: Vec<events::EnforcerEvent>,
) -> Result<Vec<Event>> {
    payloads.reverse();
    payloads
        .into_iter()
        .map(|payload| {
            let header = stream.header(&payload)?;
            let anchor = ObservedBlock::at_height(header.hash.clone(), header.height);
            envelope(payload, anchor)
        })
        .collect()
}

fn required_height(block: &ObservedBlock, name: &str) -> Result<u32> {
    block
        .height
        .with_context(|| format!("{name} has no height"))
}

fn blocks_through_floor(cursor_height: u32, floor_height: Option<u32>) -> Result<u64> {
    match floor_height {
        Some(floor_height) => cursor_height
            .checked_sub(floor_height)
            .filter(|blocks| *blocks > 0)
            .map(u64::from)
            .with_context(|| {
                format!(
                    "history cursor {cursor_height} is not above its exclusive floor {floor_height}"
                )
            }),
        None => Ok(u64::from(cursor_height) + 1),
    }
}

fn validate_expected_start_hash(
    scope: &HistoryScope<'_>,
    floor_hash: Option<&[u8]>,
    completes_cycle: bool,
    actual_oldest_hash: &[u8],
) -> Result<()> {
    if completes_cycle
        && floor_hash.is_none()
        && let Some(expected_start_hash) = scope.expected_start_hash
        && actual_oldest_hash != expected_start_hash
    {
        bail!(
            "history stream {} reached activation block {}, expected {}",
            scope.stream,
            hex::encode(actual_oldest_hash),
            hex::encode(expected_start_hash)
        );
    }
    Ok(())
}

pub(crate) fn verify_page_with<S: HistoryStream>(
    stream: &S,
    payloads: &[events::EnforcerEvent],
    cursor: &ObservedBlock,
    requested: u32,
) -> Result<()> {
    if requested == 0 || payloads.is_empty() {
        bail!("a historical page must request and return at least one block");
    }
    if payloads.len() > requested as usize {
        bail!(
            "historical page requested at most {requested} blocks but returned {}",
            payloads.len()
        );
    }
    let cursor_height = required_height(cursor, "history page cursor")?;
    let newest = stream.header(&payloads[0])?;
    if newest.hash != cursor.hash || newest.height != cursor_height {
        bail!(
            "historical page begins at {} height {}, expected {} height {}",
            hex::encode(&newest.hash),
            newest.height,
            hex::encode(&cursor.hash),
            cursor_height
        );
    }
    for pair in payloads.windows(2) {
        let newer = stream.header(&pair[0])?;
        let older = stream.header(&pair[1])?;
        if newer.previous_hash != older.hash || newer.height != older.height.saturating_add(1) {
            bail!(
                "historical page breaks between heights {} and {}",
                newer.height,
                older.height
            );
        }
    }
    let oldest = stream.header(
        payloads
            .last()
            .expect("an empty historical page was rejected"),
    )?;
    let returned = u32::try_from(payloads.len()).expect("a page length fits in a u32");
    let expected_oldest = cursor_height
        .checked_sub(returned - 1)
        .context("history page underflows genesis")?;
    if oldest.height != expected_oldest {
        bail!(
            "historical page ends at height {}, expected {expected_oldest}",
            oldest.height
        );
    }
    Ok(())
}

pub(crate) fn retryable_rpc_error(error: &Error) -> bool {
    [
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Unavailable,
    ]
    .into_iter()
    .any(|code| error_has_code(error, code))
}

fn page_too_large(error: &Error) -> bool {
    [Code::DeadlineExceeded, Code::ResourceExhausted]
        .into_iter()
        .any(|code| error_has_code(error, code))
}

pub(crate) fn error_has_code(error: &Error, code: Code) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == code)
    })
}

#[derive(Clone, Copy)]
enum FailureMode {
    Fatal,
    Deferred,
}

async fn settle_failure(
    recorder: &Recorder,
    scope: &HistoryScope<'_>,
    error: Error,
    target: &ObservedBlock,
    blocks: usize,
    pages: usize,
    mode: FailureMode,
) -> Result<Outcome> {
    let message = format!("{error:#}");
    let status = match recorder
        .store()
        .settle_history_failure(
            scope.stream,
            scope.sidechain,
            scope.sidechain_instance_id,
            &message,
        )
        .await
    {
        Ok(status) => status,
        Err(mark_error) => {
            return Err(error).context(format!(
                "also failed to settle the history error in Postgres: {mark_error:#}"
            ));
        }
    };
    match (status, mode) {
        (HistoryStatus::Superseded, _) => Ok(Outcome::Superseded {
            target: target.clone(),
            blocks,
            pages,
        }),
        (HistoryStatus::Error, FailureMode::Deferred) => Ok(Outcome::Deferred {
            target: target.clone(),
            blocks,
            pages,
        }),
        (HistoryStatus::Error, FailureMode::Fatal) => Err(error),
        (status, _) => Err(error).context(format!(
            "history failure settled to unexpected status {status:?}"
        )),
    }
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() || shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

fn connected_header(payload: &events::EnforcerEvent) -> Result<&events::BlockHeader> {
    let Some(events::enforcer_event::Event::BlockConnected(block)) = payload.event.as_ref() else {
        bail!("a historical block carried an unexpected payload");
    };
    block
        .header
        .as_ref()
        .context("a historical connected block is missing its header")
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::ObservedBlock;
    use shared::store::SidechainInstanceRef;
    use tonic::Code;

    use super::{
        BlockHistory, HistoryScope, blocks_through_floor, error_has_code, page_too_large,
        replacement_target, retryable_rpc_error, validate_expected_start_hash, verify_page_with,
    };

    fn recovered(hash: u8, previous_hash: u8, height: u32) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::BlockConnected(
                events::BlockConnected {
                    header: Some(events::BlockHeader {
                        hash: vec![hash; 32],
                        previous_hash: vec![previous_hash; 32],
                        height,
                        chain_work: vec![0x44; 32],
                        timestamp: 1_750_000_000,
                    }),
                    sidechain_number: 9,
                    bmm_commitment: None,
                    events: Vec::new(),
                },
            )),
        }
    }

    fn stream_instance() -> SidechainInstanceRef {
        SidechainInstanceRef {
            sidechain: 9,
            sidechain_instance_id: "test-instance".to_owned(),
            activation_height: 101,
        }
    }

    #[test]
    fn historical_pages_are_exact_and_contiguous() {
        let instance = stream_instance();
        let stream = BlockHistory {
            instance: &instance,
        };
        let cursor = ObservedBlock::at_height(vec![0x14; 32], 104);
        let payloads = vec![
            recovered(0x14, 0x13, 104),
            recovered(0x13, 0x12, 103),
            recovered(0x12, 0x11, 102),
        ];
        verify_page_with(&stream, &payloads, &cursor, 3).expect("valid page");
        assert!(verify_page_with(&stream, &payloads, &cursor, 2).is_err());
        assert!(
            verify_page_with(
                &stream,
                &[recovered(0x14, 0xaa, 104), recovered(0x13, 0x12, 103)],
                &cursor,
                2,
            )
            .is_err()
        );
    }

    #[test]
    fn floor_ranges_are_exclusive_and_never_empty() {
        assert_eq!(blocks_through_floor(104, Some(100)).unwrap(), 4);
        assert!(blocks_through_floor(100, Some(100)).is_err());
        assert_eq!(blocks_through_floor(0, None).unwrap(), 1);
    }

    #[test]
    fn activation_hash_is_checked_only_when_a_full_cycle_reaches_its_start() {
        let expected = vec![0x11; 32];
        let wrong = vec![0x22; 32];
        let floor = vec![0x33; 32];
        let scope = HistoryScope {
            stream: "bip300_delta",
            sidechain: None,
            sidechain_instance_id: None,
            activation_height: 101,
            expected_start_hash: Some(&expected),
        };

        validate_expected_start_hash(&scope, None, true, &expected)
            .expect("full cycle reached the configured activation block");
        assert!(validate_expected_start_hash(&scope, None, true, &wrong).is_err());
        validate_expected_start_hash(&scope, Some(&floor), true, &wrong)
            .expect("an extension ends above activation and checks its exclusive floor instead");
        validate_expected_start_hash(&scope, None, false, &wrong)
            .expect("an intermediate page has not reached activation yet");
    }

    #[test]
    fn grpc_retry_classification_walks_anyhow_context() {
        for (code, status) in [
            (Code::Cancelled, tonic::Status::cancelled("cancelled")),
            (
                Code::DeadlineExceeded,
                tonic::Status::deadline_exceeded("slow"),
            ),
            (
                Code::ResourceExhausted,
                tonic::Status::resource_exhausted("busy"),
            ),
            (Code::Unavailable, tonic::Status::unavailable("offline")),
        ] {
            let error = anyhow::Error::new(status).context("requesting page");
            assert!(retryable_rpc_error(&error));
            assert!(error_has_code(&error, code));
            assert!(!error_has_code(&error, Code::NotFound));
        }
        let invalid = anyhow::Error::new(tonic::Status::invalid_argument("bad request"));
        assert!(!retryable_rpc_error(&invalid));
    }

    #[test]
    fn only_page_pressure_errors_reduce_the_persisted_page_size() {
        for status in [
            tonic::Status::deadline_exceeded("page timed out"),
            tonic::Status::resource_exhausted("page is too large"),
        ] {
            let error = anyhow::Error::new(status).context("requesting history page");
            assert!(page_too_large(&error));
            assert!(retryable_rpc_error(&error));
        }

        for status in [
            tonic::Status::unavailable("enforcer restarting"),
            tonic::Status::cancelled("connection cancelled"),
        ] {
            let error = anyhow::Error::new(status).context("requesting history page");
            assert!(!page_too_large(&error));
            assert!(retryable_rpc_error(&error));
        }
    }

    #[test]
    fn a_moved_tip_does_not_replace_a_valid_or_inconclusive_retry_target() {
        let persisted = ObservedBlock::at_height(vec![0x11; 32], 100);
        let current = ObservedBlock::at_height(vec![0x22; 32], 101);

        assert_eq!(replacement_target(&current, &persisted, Some(true)), None);
        assert_eq!(replacement_target(&current, &persisted, None), None);
        assert_eq!(
            replacement_target(&current, &persisted, Some(false)),
            Some(current)
        );
    }
}
