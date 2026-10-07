//! Official node RPC evidence, independently supervised from the enforcer.
use crate::{
    Args,
    backfill::{self, HistoryScope, HistoryStream},
};
use anyhow::{Context, Result, bail, ensure};
use bitcoin::{Block, Work, consensus::deserialize};
use serde_json::{Value, json};
use shared::{
    nats_subjects::Subject,
    protobuf::{
        enforcer_extractor::{BlockHeader, ChainTip},
        event::{Event, MainchainBlock, NodeEvent, ObservedBlock, event::MonitorEvent, node_event},
    },
    recorder::Recorder,
    store::{CaptureMethod, ExtractorWorker},
};
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::{Duration, SystemTime},
};
use tokio::sync::watch;

#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    cookie: PathBuf,
}

impl NodeClient {
    pub fn new(endpoint: &str, cookie: PathBuf, timeout: Duration) -> Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint).context("invalid node RPC URL")?;
        ensure!(
            endpoint.scheme() == "http"
                && endpoint.username().is_empty()
                && endpoint.password().is_none(),
            "node RPC requires a private HTTP URL without embedded credentials"
        );
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
            cookie,
        })
    }

    pub async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        // A narrow allowlist prevents this observer from issuing wallet/admin calls.
        ensure!(
            matches!(
                method,
                "getblockchaininfo" | "getblockhash" | "getblockheader" | "getblock"
            ),
            "RPC method is not read-only observer API"
        );
        let cookie = std::fs::read_to_string(&self.cookie).context("reading node RPC cookie")?;
        let (user, password) = cookie
            .trim()
            .split_once(':')
            .context("invalid node RPC cookie shape")?;
        let response = self
            .http
            .post(self.endpoint.clone())
            .basic_auth(user, Some(password))
            .json(&json!({"jsonrpc":"2.0", "id":"observer", "method":method,"params":params}))
            .send()
            .await
            .map_err(|_| tonic::Status::unavailable("node RPC transport failed"))?;
        let success = response.status().is_success();
        let value: Value = response
            .json()
            .await
            .context("decoding node RPC response")?;
        if !value["error"].is_null() {
            let code = value["error"]["code"].as_i64().unwrap_or(0);
            // Never include an arbitrary upstream response or credentials in errors.
            if code == -5 {
                bail!(tonic::Status::not_found("node block is unavailable"));
            }
            bail!(tonic::Status::unavailable(format!(
                "node RPC {method} error code {code}"
            )));
        }
        ensure!(success, tonic::Status::unavailable("node RPC HTTP failure"));
        value
            .get("result")
            .cloned()
            .context("node response has no result")
    }

    pub async fn header(&self, hash: &[u8]) -> Result<BlockHeader> {
        ensure!(hash.len() == 32, "invalid requested block hash");
        let value = self
            .rpc("getblockheader", json!([hex::encode(hash), true]))
            .await?;
        ensure!(
            hash_bytes(&value["hash"])? == hash,
            "node returned another block"
        );
        let height = u32::try_from(value["height"].as_u64().context("missing header height")?)?;
        let previous_hash = if height == 0 {
            vec![0; 32]
        } else {
            hash_bytes(&value["previousblockhash"])?
        };
        let work = work(&value["chainwork"])?;
        let parent_work = if height == 0 {
            Work::from_be_bytes([0; 32])
        } else {
            let parent = self
                .rpc("getblockheader", json!([hex::encode(&previous_hash), true]))
                .await?;
            ensure!(
                hash_bytes(&parent["hash"])? == previous_hash
                    && parent["height"].as_u64() == Some(u64::from(height) - 1),
                "invalid parent header"
            );
            work_from_parent(&parent)?
        };
        ensure!(work > parent_work, "nonpositive block work");
        Ok(BlockHeader {
            hash: hash.to_vec(),
            previous_hash,
            height,
            block_work: (work - parent_work).to_le_bytes().to_vec(),
            cumulative_work: work.to_le_bytes().to_vec(),
            timestamp: value["time"].as_u64().context("missing block time")?,
        })
    }

    pub async fn tip(&self) -> Result<ObservedBlock> {
        let info = self.rpc("getblockchaininfo", json!([])).await?;
        ensure!(
            info["initialblockdownload"] == false,
            "node is still in initial download"
        );
        Ok(ObservedBlock::at_height(
            hash_bytes(&info["bestblockhash"])?,
            u32::try_from(info["blocks"].as_u64().context("missing node height")?)?,
        ))
    }

    pub async fn block(&self, hash: &[u8]) -> Result<MainchainBlock> {
        let header = self.header(hash).await?;
        let raw = self.rpc("getblock", json!([hex::encode(hash), 0])).await?;
        let raw_block = hex::decode(raw.as_str().context("node block is not hex")?)?;
        let block: Block = deserialize(&raw_block).context("invalid serialized node block")?;
        ensure!(
            block.block_hash().to_string() == hex::encode(hash),
            "raw block hash mismatch"
        );
        ensure!(
            block.header.prev_blockhash.to_string() == hex::encode(&header.previous_hash)
                && u64::from(block.header.time) == header.timestamp,
            "node header fields differ from raw block"
        );
        ensure!(
            block.check_merkle_root() && block.check_witness_commitment(),
            "invalid block transaction commitment"
        );
        ensure!(
            block.header.work().to_le_bytes().as_slice() == header.block_work.as_slice(),
            "node chainwork increment differs from header proof"
        );
        Ok(MainchainBlock {
            header: Some(header),
            raw_block,
        })
    }
}

fn hash_bytes(v: &Value) -> Result<Vec<u8>> {
    let bytes = hex::decode(v.as_str().context("missing hash")?)?;
    ensure!(bytes.len() == 32, "hash must have 32 bytes");
    Ok(bytes)
}
fn work(v: &Value) -> Result<Work> {
    Ok(Work::from_be_bytes(
        hash_bytes(v)?.try_into().expect("validated length"),
    ))
}
fn work_from_parent(v: &Value) -> Result<Work> {
    work(&v["chainwork"])
}

struct NodeHistory {
    activation: u32,
    activation_hash: Option<Vec<u8>>,
}
impl HistoryStream for NodeHistory {
    type Client = NodeClient;
    type Payload = MainchainBlock;
    fn scope(&self) -> HistoryScope<'_> {
        HistoryScope {
            stream: "mainchain_block",
            sidechain: None,
            sidechain_instance_id: None,
            activation_height: self.activation,
            expected_start_hash: self.activation_hash.as_deref(),
        }
    }
    fn fetch<'a>(
        &'a self,
        client: &'a mut NodeClient,
        cursor: &'a ObservedBlock,
        requested: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MainchainBlock>>> + Send + 'a>> {
        Box::pin(async move {
            let mut hash = cursor.hash.clone();
            let mut blocks = Vec::new();
            for _ in 0..requested {
                let block = client.block(&hash).await?;
                hash = block
                    .header
                    .as_ref()
                    .context("missing header")?
                    .previous_hash
                    .clone();
                blocks.push(block);
            }
            Ok(blocks)
        })
    }
    fn header<'a>(&self, block: &'a MainchainBlock) -> Result<&'a BlockHeader> {
        block.header.as_ref().context("node block missing header")
    }
    fn envelope(&self, block: MainchainBlock, anchor: ObservedBlock) -> Result<Event> {
        node_event(node_event::Event::MainchainBlock(block), anchor)
    }
    fn current_tip<'a>(
        &self,
        client: &'a mut NodeClient,
    ) -> Pin<Box<dyn Future<Output = Result<ObservedBlock>> + Send + 'a>> {
        Box::pin(client.tip())
    }
}

pub fn node_event(event: node_event::Event, anchor: ObservedBlock) -> Result<Event> {
    Ok(Event::new(
        MonitorEvent::Node(NodeEvent { event: Some(event) }),
        Some(anchor),
    )?)
}

async fn monitor(args: Args, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let mut client = NodeClient::new(
        args.node_rpc_endpoint
            .as_deref()
            .context("missing node endpoint")?,
        args.node_rpc_cookie_file
            .clone()
            .context("missing node cookie file")?,
        args.request_timeout(),
    )?;
    let history = NodeHistory {
        activation: args.activation_height,
        activation_hash: args.activation_block_hash_bytes()?,
    };
    if let Some(expected) = &history.activation_hash {
        let actual = client
            .rpc("getblockhash", json!([history.activation]))
            .await?;
        ensure!(
            hash_bytes(&actual)? == *expected,
            "node activation identity mismatch"
        );
    }
    let recorder = Recorder::connect_with_manifest(
        &args.postgres,
        &args.nats,
        Subject::Node,
        "node",
        "bip300-monitor-node",
        args.dataset_manifest(),
    )
    .await?;
    recorder
        .initialize_worker_statuses(&[ExtractorWorker::NodeHistory])
        .await?;
    let history_recorder = recorder.clone();
    let fee_client = client.clone();
    let fee_recorder = recorder.clone();
    let fee_shutdown = shutdown.clone();
    let history_worker = async {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let tip = match client.tip().await {
                Ok(tip) => tip,
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::NodeHistory,
                            &format!("{error:#}"),
                            1,
                        )
                        .await?;
                    tokio::select! { _=shutdown.changed()=>return Ok(()), _=tokio::time::sleep(Duration::from_secs(5))=>{} }
                    continue;
                }
            };
            let header = match client.header(&tip.hash).await {
                Ok(header) => header,
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::NodeHistory,
                            &format!("reading the tip header: {error:#}"),
                            1,
                        )
                        .await?;
                    tokio::select! { _=shutdown.changed()=>return Ok(()), _=tokio::time::sleep(Duration::from_secs(5))=>{} }
                    continue;
                }
            };
            recorder
                .record(node_event(
                    node_event::Event::ChainTip(ChainTip {
                        header: Some(header),
                    }),
                    tip.clone(),
                )?)
                .await?;
            recorder
                .record_tip_observation(&tip, None, CaptureMethod::Poll, SystemTime::now())
                .await?;
            match backfill::run_history(
                &mut client,
                &recorder,
                &history,
                &tip,
                backfill::Settings {
                    page_blocks: args.backfill_page_blocks.min(4),
                    page_pause: args.backfill_page_pause(),
                },
                shutdown.clone(),
            )
            .await
            {
                Ok(backfill::Outcome::UpToDate { .. } | backfill::Outcome::Completed { .. }) => {
                    recorder
                        .record_worker_success(ExtractorWorker::NodeHistory)
                        .await?
                }
                // A deferred page failure leaves history incomplete: not healthy.
                Ok(backfill::Outcome::Deferred { target, .. }) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::NodeHistory,
                            &format!(
                                "node history toward {} deferred after a page failure",
                                hex::encode(&target.hash)
                            ),
                            1,
                        )
                        .await?;
                }
                Ok(
                    backfill::Outcome::Interrupted { .. } | backfill::Outcome::Superseded { .. },
                ) => {}
                Err(error) => {
                    recorder
                        .record_worker_failure(
                            ExtractorWorker::NodeHistory,
                            &format!("{error:#}"),
                            1,
                        )
                        .await?;
                    return Err(error);
                }
            }
            tokio::select! { _=shutdown.changed()=>return Ok(()), _=tokio::time::sleep(args.tip_poll_interval())=>{} }
        }
    };
    let result = if args.confirmed_bmm_fees {
        tokio::select! { result=history_worker=>result, result=crate::fees::monitor(fee_client,fee_recorder,fee_shutdown)=>result }
    } else {
        history_worker.await
    };
    history_recorder
        .finish_run(
            if result.is_ok() {
                "completed"
            } else {
                "failed"
            },
            Some("node worker stopped"),
        )
        .await?;
    result
}

/// Separate futures keep the node running while upstream reconnects. A durable
/// storage failure remains fatal; it must never look like a healthy capture gap.
pub async fn run(args: Args, shutdown: watch::Receiver<bool>) -> Result<()> {
    args.validate()?;
    if args.node_rpc_endpoint.is_none() {
        return crate::runtime::run(args, shutdown).await;
    }
    // One side failing stops the other through this channel, so both runs are
    // finished explicitly instead of one being cancelled mid-write and left
    // `running` until the next startup closes it as orphaned.
    let (stop_tx, stop_rx) = watch::channel(false);
    let enforcer_args = args.clone();
    let enforcer_shutdown = stop_rx.clone();
    let enforcer = async move {
        loop {
            if *enforcer_shutdown.borrow() {
                return Ok(());
            }
            match crate::runtime::run(enforcer_args.clone(), enforcer_shutdown.clone()).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if error
                        .chain()
                        .any(|e| e.is::<tonic::transport::Error>() || e.is::<tonic::Status>()) =>
                {
                    tracing::warn!("enforcer unavailable; node capture continues");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(error) => return Err(error),
            }
        }
    };
    let stop_on_error = |result: Result<()>| {
        if result.is_err() {
            stop_tx.send_replace(true);
        }
        result
    };
    let forward = async {
        let mut shutdown = shutdown;
        if shutdown.changed().await.is_ok() || *shutdown.borrow() {
            stop_tx.send_replace(true);
        }
        std::future::pending::<()>().await;
    };
    let both = async {
        tokio::join!(
            async { stop_on_error(monitor(args, stop_rx.clone()).await) },
            async { stop_on_error(enforcer.await) },
        )
    };
    let (node, enforcer) =
        tokio::select! { results = both => results, () = forward => unreachable!() };
    node.and(enforcer)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn node_work_is_big_endian_and_exact() {
        let w = work(&json!(
            "0000000000000000000000000000000000000000000000000020000000000001"
        ))
        .unwrap();
        assert_eq!(w.to_le_bytes()[0], 1);
        assert_eq!(w.to_le_bytes()[6], 32);
        assert!(work(&json!("01")).is_err());
    }
    async fn fake_rpc(
        responses: Vec<(u16, Value)>,
    ) -> (NodeClient, tokio::task::JoinHandle<()>, PathBuf) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let cookie =
            std::env::temp_dir().join(format!("observer-rpc-test-{}-{port}", std::process::id()));
        std::fs::write(&cookie, "test:local-only").unwrap();
        let client = NodeClient::new(
            &format!("http://127.0.0.1:{port}"),
            cookie.clone(),
            Duration::from_secs(3),
        )
        .unwrap();
        let task = tokio::spawn(async move {
            for (status, value) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 8192];
                let n = stream.read(&mut bytes).await.unwrap();
                assert!(
                    String::from_utf8_lossy(&bytes[..n])
                        .to_lowercase()
                        .contains("authorization: basic ")
                );
                let body = value.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (client, task, cookie)
    }
    #[tokio::test]
    async fn core_not_found_is_preserved_even_with_http_error() {
        let (client, task, cookie) = fake_rpc(vec![(
            500,
            json!({"error":{"code":-5,"message":"not logged"},"result":null}),
        )])
        .await;
        let error = client
            .rpc("getblock", json!(["00".repeat(32), 0]))
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<tonic::Status>().unwrap().code(),
            tonic::Code::NotFound
        );
        task.await.unwrap();
        std::fs::remove_file(cookie).unwrap();
    }
    #[tokio::test]
    async fn raw_block_is_verified_against_official_header_and_work() {
        let block = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Bitcoin);
        let hash = block.block_hash().to_string();
        let raw = hex::encode(bitcoin::consensus::serialize(&block));
        let header = json!({"hash":hash,"height":0,"chainwork":hex::encode(block.header.work().to_be_bytes()),"time":block.header.time});
        let responses = vec![
            (200, json!({"error":null,"result":header})),
            (200, json!({"error":null,"result":raw})),
        ];
        let (client, task, cookie) = fake_rpc(responses).await;
        let captured = client.block(&hex::decode(&hash).unwrap()).await.unwrap();
        assert_eq!(captured.raw_block, bitcoin::consensus::serialize(&block));
        assert_eq!(
            captured.header.unwrap().cumulative_work,
            block.header.work().to_le_bytes()
        );
        task.await.unwrap();
        std::fs::remove_file(cookie).unwrap();
    }
    #[tokio::test]
    async fn another_block_cannot_satisfy_requested_header() {
        let (client, task, cookie) = fake_rpc(vec![(
            200,
            json!({"error":null,"result":{"hash":"11".repeat(32)}}),
        )])
        .await;
        assert!(
            client
                .header(&[0; 32])
                .await
                .unwrap_err()
                .to_string()
                .contains("another block")
        );
        task.await.unwrap();
        std::fs::remove_file(cookie).unwrap();
    }
    #[test]
    fn embedded_rpc_credentials_are_rejected() {
        assert!(
            NodeClient::new(
                "http://user:secret@localhost:1",
                PathBuf::new(),
                Duration::from_secs(1)
            )
            .is_err()
        );
    }
}
