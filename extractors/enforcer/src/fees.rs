//! Partial enrichment of officially observed bids using node transaction data.
use crate::node::{NodeClient, node_event};
use anyhow::{Context, Result, ensure};
use bitcoin::{Amount, Denomination};
use serde_json::Value;
use shared::{
    protobuf::{
        enforcer_extractor::{ConfirmedBmmFee, ConfirmedBmmFees},
        event::node_event,
    },
    recorder::Recorder,
    store::ExtractorWorker,
};
use std::time::Duration;
use tokio::sync::watch;

pub(crate) async fn monitor(
    client: NodeClient,
    recorder: Recorder,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    recorder
        .initialize_worker_statuses(&[ExtractorWorker::ConfirmedBmmFees])
        .await?;
    loop {
        tokio::select! {_=shutdown.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_millis(100))=>{}}
        let Some((source_id, block)) = recorder.store().next_fee_block().await? else {
            tokio::select! {_=shutdown.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(5))=>{}}
            continue;
        };
        let result = async {
            let header = client.header(&block.hash).await?;
            let candidates = recorder
                .store()
                .observed_bmm_candidates(&block.hash, &header.previous_hash)
                .await?;
            let data = client
                .rpc("getblock", serde_json::json!([hex::encode(&block.hash), 3]))
                .await?;
            ensure!(
                data["hash"].as_str() == Some(hex::encode(&block.hash).as_str()),
                "fee response is for another block"
            );
            let transactions = data["tx"]
                .as_array()
                .context("missing block transactions")?;
            let mut fees = Vec::new();
            for (sidechain_number, txid) in candidates {
                let id = hex::encode(&txid);
                let Some(transaction) = transactions
                    .iter()
                    .find(|t| t["txid"].as_str() == Some(&id))
                else {
                    continue;
                };
                let fee = transaction_fee(transaction);
                fees.push(ConfirmedBmmFee {
                    sidechain_number,
                    txid,
                    fee_sats: fee.as_ref().ok().copied(),
                    unavailable_reason: fee
                        .err()
                        .map(|_| "prevout_missing_or_invalid".to_owned())
                        .unwrap_or_default(),
                });
            }
            node_event(
                node_event::Event::ConfirmedBmmFees(ConfirmedBmmFees {
                    header: Some(header),
                    fees,
                    source: "ecash-node:getblock:3;observed_bids_only".into(),
                }),
                block,
            )
        }
        .await;
        // A block that cannot be enriched is scheduled for a bounded retry so
        // it neither blocks the blocks after it nor stops the extractor.
        let outcome = match result {
            Ok(event) => recorder.record_fee_enrichment(source_id, event).await,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => {
                recorder
                    .record_worker_success(ExtractorWorker::ConfirmedBmmFees)
                    .await?;
            }
            Err(error) => {
                let message = format!("{error:#}");
                recorder
                    .store()
                    .record_fee_failure(source_id, &message)
                    .await?;
                recorder
                    .record_worker_failure(ExtractorWorker::ConfirmedBmmFees, &message, 1)
                    .await?;
                tokio::select! {_=shutdown.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(30))=>{}}
            }
        }
    }
}
fn sats(v: &Value) -> Result<u64> {
    let s = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => anyhow::bail!("missing amount"),
    };
    Ok(Amount::from_str_in(&s, Denomination::Bitcoin)?.to_sat())
}
fn transaction_fee(tx: &Value) -> Result<u64> {
    let mut input = 0_u64;
    let mut output = 0_u64;
    let inputs = tx["vin"].as_array().context("missing inputs")?;
    ensure!(!inputs.is_empty(), "empty input list");
    for i in inputs {
        input = input
            .checked_add(sats(&i["prevout"]["value"])?)
            .context("input amount overflow")?;
    }
    for o in tx["vout"].as_array().context("missing outputs")? {
        output = output
            .checked_add(sats(&o["value"])?)
            .context("output amount overflow")?;
    }
    input
        .checked_sub(output)
        .context("negative transaction fee")
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fee_is_exact_and_missing_prevouts_are_unknown() {
        assert_eq!(
            transaction_fee(
                &json!({"vin":[{"prevout":{"value":"0.00000101"}}],"vout":[{"value":"0.00000100"}]})
            )
            .unwrap(),
            1
        );
        assert!(transaction_fee(&json!({"vin":[{}],"vout":[{"value":"0.00"}]})).is_err());
        assert_eq!(sats(&json!("90071992.54740993")).unwrap(), 9007199254740993);
        assert!(sats(&json!("0.000000001")).is_err());
    }
}
