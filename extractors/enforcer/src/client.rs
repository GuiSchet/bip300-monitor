//! Read-only client for the enforcer's validator service.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::proto::{common, mainchain};

const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_KEEP_ALIVE_IDLE: Duration = Duration::from_secs(60);
const TCP_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const TCP_KEEP_ALIVE_RETRIES: u32 = 3;

/// Thin read-only wrapper around the generated validator service client.
#[derive(Clone, Debug)]
pub struct EnforcerClient {
    inner: mainchain::validator_service_client::ValidatorServiceClient<Channel>,
    request_timeout: Duration,
}

impl EnforcerClient {
    /// Connect to an enforcer validator endpoint.
    pub async fn connect(endpoint: &str, request_timeout: Duration) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_owned())
            .with_context(|| format!("parsing enforcer endpoint `{endpoint}`"))?
            .connect_timeout(request_timeout)
            .tcp_keepalive(Some(TCP_KEEP_ALIVE_IDLE))
            .tcp_keepalive_interval(Some(TCP_KEEP_ALIVE_INTERVAL))
            .tcp_keepalive_retries(Some(TCP_KEEP_ALIVE_RETRIES))
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(HTTP2_KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect()
            .await
            .with_context(|| format!("connecting to enforcer at `{endpoint}`"))?;

        Ok(Self {
            inner: mainchain::validator_service_client::ValidatorServiceClient::new(channel),
            request_timeout,
        })
    }

    /// Fetch the network and active BIP300 constants.
    pub async fn get_chain_info(&mut self) -> Result<mainchain::GetChainInfoResponse> {
        self.inner
            .get_chain_info(self.unary_request(mainchain::GetChainInfoRequest {}))
            .await
            .context("calling ValidatorService.GetChainInfo")
            .map(tonic::Response::into_inner)
    }

    /// Fetch the enforcer's current mainchain tip.
    pub async fn get_chain_tip(&mut self) -> Result<mainchain::GetChainTipResponse> {
        self.inner
            .get_chain_tip(self.unary_request(mainchain::GetChainTipRequest {}))
            .await
            .context("calling ValidatorService.GetChainTip")
            .map(tonic::Response::into_inner)
    }

    /// Fetch block information filtered for one sidechain slot.
    pub async fn get_block_info(
        &mut self,
        block_hash: impl Into<String>,
        sidechain_id: u8,
        max_ancestors: Option<u32>,
    ) -> Result<mainchain::GetBlockInfoResponse> {
        let request = mainchain::GetBlockInfoRequest {
            block_hash: Some(reverse_hex(block_hash)),
            sidechain_id: Some(u32::from(sidechain_id)),
            max_ancestors,
        };
        self.inner
            .get_block_info(self.unary_request(request))
            .await
            .with_context(|| {
                format!("calling ValidatorService.GetBlockInfo for sidechain {sidechain_id}")
            })
            .map(tonic::Response::into_inner)
    }

    /// Fetch all current, not-yet-activated sidechain proposals.
    pub async fn get_sidechain_proposals(
        &mut self,
    ) -> Result<mainchain::GetSidechainProposalsResponse> {
        self.inner
            .get_sidechain_proposals(self.unary_request(mainchain::GetSidechainProposalsRequest {}))
            .await
            .context("calling ValidatorService.GetSidechainProposals")
            .map(tonic::Response::into_inner)
    }

    /// Fetch all active sidechains.
    pub async fn get_sidechains(&mut self) -> Result<mainchain::GetSidechainsResponse> {
        self.inner
            .get_sidechains(self.unary_request(mainchain::GetSidechainsRequest {}))
            .await
            .context("calling ValidatorService.GetSidechains")
            .map(tonic::Response::into_inner)
    }

    /// Fetch the current CTIP for one sidechain slot.
    pub async fn get_ctip(&mut self, sidechain_number: u8) -> Result<mainchain::GetCtipResponse> {
        let request = mainchain::GetCtipRequest {
            sidechain_number: Some(u32::from(sidechain_number)),
        };
        self.inner
            .get_ctip(self.unary_request(request))
            .await
            .with_context(|| {
                format!("calling ValidatorService.GetCtip for sidechain {sidechain_number}")
            })
            .map(tonic::Response::into_inner)
    }

    /// Subscribe to live block connect/disconnect events.
    ///
    /// No RPC deadline is attached to this request because it is intentionally
    /// long-lived. Establishing the stream is bounded by the configured
    /// request timeout.
    pub async fn subscribe_events(
        &mut self,
        sidechain_id: u8,
    ) -> Result<Streaming<mainchain::SubscribeEventsResponse>> {
        let request = mainchain::SubscribeEventsRequest {
            sidechain_id: Some(u32::from(sidechain_id)),
        };
        await_stream_setup(
            self.request_timeout,
            sidechain_id,
            self.inner.subscribe_events(Request::new(request)),
        )
        .await
        .map(tonic::Response::into_inner)
    }

    fn unary_request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(self.request_timeout);
        request
    }
}

async fn await_stream_setup<T, F>(
    request_timeout: Duration,
    sidechain_id: u8,
    setup: F,
) -> Result<T>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    tokio::time::timeout(request_timeout, setup)
        .await
        .with_context(|| {
            format!("waiting for ValidatorService.SubscribeEvents for sidechain {sidechain_id}")
        })?
        .with_context(|| {
            format!("calling ValidatorService.SubscribeEvents for sidechain {sidechain_id}")
        })
}

fn reverse_hex(value: impl Into<String>) -> common::ReverseHex {
    common::ReverseHex {
        hex: Some(value.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::time::Duration;

    use super::await_stream_setup;

    #[tokio::test]
    async fn stream_setup_is_bounded_by_the_request_timeout() {
        let error = await_stream_setup(
            Duration::from_millis(1),
            9,
            future::pending::<Result<(), tonic::Status>>(),
        )
        .await
        .expect_err("idle stream setup must time out");

        assert!(
            error
                .to_string()
                .contains("waiting for ValidatorService.SubscribeEvents for sidechain 9")
        );
    }
}
