//! Slot-independent BIP300/301 adapter for the shared backfill engine.

use std::pin::Pin;

use anyhow::{Context, Error, Result, bail};
use shared::protobuf::enforcer_extractor as events;
use shared::protobuf::event::ObservedBlock;
use shared::recorder::Recorder;
use tokio::sync::watch;
use tonic::Code;

use crate::EnforcerClient;
use crate::backfill::{
    HistoryScope, HistoryStream, Settings, error_has_code, retryable_rpc_error, run_history,
};
use crate::convert;

pub(crate) use crate::backfill::Outcome;

const HISTORY_STREAM: &str = "bip300_delta";

struct Bip300History {
    activation_height: u32,
    activation_block_hash: Option<Vec<u8>>,
}

impl HistoryStream for Bip300History {
    fn scope(&self) -> HistoryScope<'_> {
        HistoryScope {
            stream: HISTORY_STREAM,
            sidechain: None,
            sidechain_instance_id: None,
            activation_height: self.activation_height,
            expected_start_hash: self.activation_block_hash.as_deref(),
        }
    }

    fn fetch<'a>(
        &'a self,
        client: &'a mut EnforcerClient,
        cursor: &'a ObservedBlock,
        requested: u32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<events::EnforcerEvent>>> + Send + 'a>>
    {
        Box::pin(async move {
            let response = client
                .get_bip300_block_delta(hex::encode(&cursor.hash), Some(requested - 1))
                .await?;
            convert::bip300_block_deltas(response)
        })
    }

    fn header<'a>(&self, payload: &'a events::EnforcerEvent) -> Result<&'a events::BlockHeader> {
        let Some(events::enforcer_event::Event::Bip300BlockDelta(block)) = payload.event.as_ref()
        else {
            bail!("BIP300 history contains a non-delta payload");
        };
        block
            .header
            .as_ref()
            .context("BIP300 block delta is missing its header")
    }

    fn unavailable_error(&self, error: &Error) -> bool {
        error_has_code(error, Code::NotFound) || error_has_code(error, Code::Unimplemented)
    }

    fn inconclusive_probe_error(&self, error: &Error) -> bool {
        error_has_code(error, Code::Unimplemented) || retryable_rpc_error(error)
    }
}

/// Recover canonical BIP300/301 deltas from activation through a fixed tip.
pub(crate) async fn run(
    client: &mut EnforcerClient,
    recorder: &Recorder,
    activation_height: u32,
    activation_block_hash: Option<&[u8]>,
    tip: &ObservedBlock,
    settings: Settings,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Outcome> {
    run_history(
        client,
        recorder,
        &Bip300History {
            activation_height,
            activation_block_hash: activation_block_hash.map(<[u8]>::to_vec),
        },
        tip,
        settings,
        shutdown_rx,
    )
    .await
}

#[cfg(test)]
mod tests {
    use shared::protobuf::enforcer_extractor as events;
    use shared::protobuf::event::ObservedBlock;

    use super::Bip300History;
    use crate::backfill::{HistoryStream, verify_page_with};

    fn recovered(hash: u8, previous_hash: u8, height: u32) -> events::EnforcerEvent {
        events::EnforcerEvent {
            event: Some(events::enforcer_event::Event::Bip300BlockDelta(
                events::Bip300BlockDelta {
                    header: Some(events::BlockHeader {
                        hash: vec![hash; 32],
                        previous_hash: vec![previous_hash; 32],
                        height,
                        chain_work: vec![0x44; 32],
                        timestamp: 1_750_000_000,
                    }),
                    coinbase_txid: vec![0x55; 32],
                    coinbase_messages: Vec::new(),
                    treasury_transitions: Vec::new(),
                    confirmed_bmm_requests: Vec::new(),
                },
            )),
        }
    }

    #[test]
    fn global_bip300_pages_use_the_shared_contiguity_rules() {
        let stream = Bip300History {
            activation_height: 101,
            activation_block_hash: Some(vec![0x11; 32]),
        };
        let payloads = vec![
            recovered(0x14, 0x13, 104),
            recovered(0x13, 0x12, 103),
            recovered(0x12, 0x11, 102),
        ];
        verify_page_with(
            &stream,
            &payloads,
            &ObservedBlock::at_height(vec![0x14; 32], 104),
            3,
        )
        .expect("valid global BIP300 page");
    }

    #[test]
    fn development_history_has_no_expected_activation_hash() {
        let development = Bip300History {
            activation_height: 0,
            activation_block_hash: None,
        };
        assert_eq!(development.scope().expected_start_hash, None);

        let pinned = Bip300History {
            activation_height: 967_680,
            activation_block_hash: Some(vec![0x11; 32]),
        };
        assert_eq!(pinned.scope().expected_start_hash, Some(&[0x11; 32][..]));
    }
}
