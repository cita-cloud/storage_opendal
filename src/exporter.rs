// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::config::ExportConfig;
use crate::storager::Storager;
use crate::util::get_real_key;
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::Block;
use cita_cloud_proto::client::ClientOptions;
use cita_cloud_proto::client::EVMClientTrait;
use cita_cloud_proto::client::InterceptedSvc;
use cita_cloud_proto::client::RPCClientTrait;
use cita_cloud_proto::common;
use cita_cloud_proto::controller::rpc_service_client::RpcServiceClient;
use cita_cloud_proto::controller::BlockNumber;
use cita_cloud_proto::evm::rpc_service_client::RpcServiceClient as EvmServiceClient;
use cita_cloud_proto::retry::RetryClient;
use cita_cloud_proto::storage::Regions;
use prost::Message as proto_message;
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio::sync::OnceCell;
use tokio::time::Duration;

pub struct Exporter {
    config: ExportConfig,
}

const BLOCKS_TOPIC: &str = "blocks";
const TXS_TOPIC: &str = "txs";
const UTXOS_TOPIC: &str = "utxos";
const SYSTEM_CONFIG_TOPIC: &str = "system-config";
const RECEITPS_TOPIC: &str = "receipts";
const LOGS_TOPIC: &str = "logs";

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct OffsetsSummary {
    #[serde(rename = "beginning_offset", skip_serializing_if = "Option::is_none")]
    pub beginning_offset: Option<i64>,
    #[serde(rename = "end_offset", skip_serializing_if = "Option::is_none")]
    pub end_offset: Option<i64>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct SendOffset {
    #[serde(rename = "partition", skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    #[serde(rename = "offset", skip_serializing_if = "Option::is_none")]
    pub end_offset: Option<i64>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct OffsetList {
    #[serde(rename = "offsets", skip_serializing_if = "Option::is_none")]
    pub offsets: Option<Vec<SendOffset>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConsumerRecordKey {
    Array(Vec<String>),
    Object(serde_json::Value),
    String(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProducerRecordValue {
    Array(Vec<String>),
    Object(serde_json::Value),
    String(String),
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProducerRecord {
    #[serde(rename = "partition", skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    #[serde(rename = "value", deserialize_with = "Option::deserialize")]
    pub value: Option<Box<ProducerRecordValue>>,
    #[serde(rename = "key", skip_serializing_if = "Option::is_none")]
    pub key: Option<Box<ConsumerRecordKey>>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProducerRecordList {
    #[serde(rename = "records", skip_serializing_if = "Option::is_none")]
    pub records: Option<Vec<ProducerRecord>>,
}

/// export json struct
// block
#[derive(Serialize, Deserialize)]
struct ExportBlock {
    height: u64,
    block_hash: String,
    // header
    prev_hash: String,
    timestamp: u64,
    transaction_root: String,
    proposer: String,
    // body
    tx_count: u32,
    // proof
    proof: String,
    // state root
    state_root: String,
    // version
    version: u32,
}

// normal tx
#[derive(Serialize, Deserialize)]
struct ExportTx {
    height: u64,
    index: u32,
    tx_hash: String,
    // transaction
    data: String,
    nonce: String,
    quota: u64,
    to: String,
    valid_until_block: u64,
    value: String,
    version: u32,
    // witness
    sender: String,
    signature: String,
}

// utxo tx
#[derive(Serialize, Deserialize)]
struct ExportUtxo {
    height: u64,
    index: u32,
    tx_hash: String,
    // transaction
    lock_id: u64,
    output: String,
    pre_tx_hash: String,
    version: u32,
    // witness
    sender: String,
    signature: String,
}

// receipt
#[derive(Serialize, Deserialize)]
struct ExportReceipt {
    height: u64,
    index: u32,
    contract_addr: String,
    cumulative_quota_used: String,
    quota_used: String,
    error_msg: String,
    logs_bloom: String,
    tx_hash: String,
}

// event
#[derive(Serialize, Deserialize)]
struct ExportLog {
    address: String,
    topics: String,
    data: String,
    height: u64,
    log_index: u64,
    tx_log_index: u64,
    tx_hash: String,
}

// system config
#[derive(Serialize, Deserialize)]
struct ExportSystemConfig {
    height: u64,
    admin: String,
    block_interval: u32,
    block_limit: u32,
    chain_id: String,
    emergency_brake: bool,
    quota_limit: u32,
    validators: String,
    version: u32,
}

pub static EVM_CLIENT: OnceCell<RetryClient<EvmServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();

const CLIENT_NAME: &str = "storage";

pub fn evm_client() -> RetryClient<EvmServiceClient<InterceptedSvc>> {
    EVM_CLIENT.get().cloned().unwrap()
}

pub static CONTROLLER_CLIENT: OnceCell<RetryClient<RpcServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();

pub fn controller_client() -> RetryClient<RpcServiceClient<InterceptedSvc>> {
    CONTROLLER_CLIENT.get().cloned().unwrap()
}

impl Exporter {
    pub fn new(config: &ExportConfig) -> Self {
        // init evm client
        EVM_CLIENT
            .set({
                let client_options = ClientOptions::new(
                    CLIENT_NAME.to_string(),
                    format!("http://localhost:{}", config.executor_port),
                );
                match client_options.connect_evm() {
                    Ok(retry_client) => retry_client,
                    Err(e) => panic!("client init error: {:?}", &e),
                }
            })
            .unwrap();

        CONTROLLER_CLIENT
            .set({
                let client_options = ClientOptions::new(
                    CLIENT_NAME.to_string(),
                    format!("http://localhost:{}", config.controller_port),
                );
                match client_options.connect_rpc() {
                    Ok(retry_client) => retry_client,
                    Err(e) => panic!("client init error: {:?}", &e),
                }
            })
            .unwrap();

        Exporter {
            config: config.clone(),
        }
    }

    pub async fn read_remote_progress(
        &self,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>, Option<i64>), Box<dyn Error>> {
        let base_path = &self.config.base_path;
        let partitionid = 0;

        let blocks_topicname = format!("cita-cloud.{}.{}", self.config.chain_name, BLOCKS_TOPIC);
        let url = format!("{base_path}/topics/{blocks_topicname}/partitions/{partitionid}/offsets");
        let blocks_offset: OffsetsSummary = reqwest::get(&url).await?.json().await?;

        let txs_topicname = format!("cita-cloud.{}.{}", self.config.chain_name, TXS_TOPIC);
        let url = format!("{base_path}/topics/{txs_topicname}/partitions/{partitionid}/offsets");
        let txs_offset: OffsetsSummary = reqwest::get(&url).await?.json().await?;

        let receipts_topicname =
            format!("cita-cloud.{}.{}", self.config.chain_name, RECEITPS_TOPIC);
        let url =
            format!("{base_path}/topics/{receipts_topicname}/partitions/{partitionid}/offsets");
        let receipts_offset: OffsetsSummary = reqwest::get(&url).await?.json().await?;

        let logs_topicname = format!("cita-cloud.{}.{}", self.config.chain_name, LOGS_TOPIC);
        let url = format!("{base_path}/topics/{logs_topicname}/partitions/{partitionid}/offsets");
        let logs_offset: OffsetsSummary = reqwest::get(&url).await?.json().await?;

        Ok((
            blocks_offset.end_offset,
            txs_offset.end_offset,
            receipts_offset.end_offset,
            logs_offset.end_offset,
        ))
    }

    pub async fn read_remote_progress_with_retry(&self) -> (u64, u64, u64, u64) {
        let remote_blocks_offset: u64;
        let remote_txs_offset: u64;
        let remote_receipts_offset: u64;
        let remote_logs_offset: u64;

        loop {
            if let Ok((
                Some(blocks_offset),
                Some(txs_offset),
                Some(receipts_offset),
                Some(logs_offset),
            )) = self.read_remote_progress().await
            {
                remote_blocks_offset = blocks_offset as u64;
                remote_txs_offset = txs_offset as u64;
                remote_receipts_offset = receipts_offset as u64;
                remote_logs_offset = logs_offset as u64;
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        (
            remote_blocks_offset,
            remote_txs_offset,
            remote_receipts_offset,
            remote_logs_offset,
        )
    }

    pub async fn write(
        &self,
        payload: String,
        topicname: &str,
    ) -> Result<OffsetList, Box<dyn Error>> {
        let base_path = &self.config.base_path;
        let partitionid = 0;
        let url = format!("{base_path}/topics/{topicname}");

        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        let body = ProducerRecordList {
            records: Some(vec![ProducerRecord {
                partition: Some(partitionid),
                value: Some(Box::new(ProducerRecordValue::Object(v))),
                key: None,
            }]),
        };

        let offset: OffsetList = reqwest::Client::new()
            .post(&url)
            .query(&[("async", "false")])
            .header("content-type", "application/vnd.kafka.json.v2+json")
            .body(serde_json::to_string(&body)?)
            .send()
            .await?
            .json()
            .await?;
        Ok(offset)
    }

    pub async fn export(&self, local: &Storager, height: u64) -> Result<(), &str> {
        let mut export_txs: Vec<ExportTx> = Vec::new();
        let mut export_utxos: Vec<ExportUtxo> = Vec::new();
        let mut export_receipts: Vec<ExportReceipt> = Vec::new();
        let mut export_logs: Vec<ExportLog> = Vec::new();
        let mut opt_export_sc: Option<ExportSystemConfig> = None;

        let height_bytes = height.to_be_bytes().to_vec();

        let full_block_data = local
            .load_full_block(&height_bytes)
            .await
            .map_err(|_| "load full block failed")?;
        let full_block = Block::decode(full_block_data.as_slice())
            .map_err(|_| "export failed: decode FullBlock failed")?;

        let block_hash = local
            .load(&get_real_key(Regions::BlockHash, &height_bytes), true)
            .await
            .map_err(|_| "load block hash failed")?;

        let blk_header = full_block.header.unwrap();
        let tx_count = full_block.body.as_ref().unwrap().body.len() as u32;
        let export_block = ExportBlock {
            height,
            block_hash: hex::encode(block_hash),
            // header
            prev_hash: hex::encode(&blk_header.prevhash),
            timestamp: blk_header.timestamp,
            transaction_root: hex::encode(&blk_header.transactions_root),
            proposer: hex::encode(&blk_header.proposer),
            // body
            tx_count,
            // proof
            proof: hex::encode(&full_block.proof),
            // state root
            state_root: hex::encode(&full_block.state_root),
            // version
            version: full_block.version,
        };

        for (tx_index, raw_tx) in full_block
            .body
            .clone()
            .ok_or("no block body")?
            .body
            .into_iter()
            .enumerate()
        {
            if let Some(Tx::UtxoTx(utxo_tx)) = raw_tx.tx.clone() {
                let transaction = utxo_tx.transaction.unwrap();
                let export_utxo = ExportUtxo {
                    height,
                    index: tx_index as u32,
                    tx_hash: hex::encode(&utxo_tx.transaction_hash),
                    lock_id: transaction.lock_id,
                    output: hex::encode(&transaction.output),
                    pre_tx_hash: hex::encode(&transaction.pre_tx_hash),
                    version: transaction.version,
                    sender: hex::encode(&utxo_tx.witnesses[0].sender),
                    signature: hex::encode(&utxo_tx.witnesses[0].signature),
                };
                export_utxos.push(export_utxo);

                let sc = controller_client()
                    .get_system_config_by_number(BlockNumber {
                        block_number: height,
                    })
                    .await
                    .map_err(|_| "get system config failed")?;
                opt_export_sc = Some(ExportSystemConfig {
                    height,
                    admin: hex::encode(&sc.admin),
                    block_interval: sc.block_interval,
                    block_limit: sc.block_limit,
                    chain_id: hex::encode(&sc.chain_id),
                    emergency_brake: sc.emergency_brake,
                    quota_limit: sc.quota_limit,
                    validators: format!(
                        "{:?}",
                        sc.validators
                            .iter()
                            .map(hex::encode)
                            .collect::<Vec<String>>()
                    ),
                    version: sc.version,
                });
            } else if let Some(Tx::NormalTx(tx)) = raw_tx.tx.clone() {
                let transaction = tx.transaction.unwrap();
                let witness = tx.witness.unwrap();
                let export_tx = ExportTx {
                    height,
                    index: tx_index as u32,
                    tx_hash: hex::encode(&tx.transaction_hash),
                    data: hex::encode(&transaction.data),
                    nonce: hex::encode(&transaction.nonce),
                    quota: transaction.quota,
                    to: hex::encode(&transaction.to),
                    valid_until_block: transaction.valid_until_block,
                    value: hex::encode(&transaction.value),
                    version: transaction.version,
                    sender: hex::encode(&witness.sender),
                    signature: hex::encode(&witness.signature),
                };

                export_txs.push(export_tx);

                // get receipts
                let hash = common::Hash {
                    hash: tx.transaction_hash.clone(),
                };
                let receipt = evm_client()
                    .get_transaction_receipt(hash)
                    .await
                    .map_err(|_| "get transaction receipt failed")?;
                let export_receipt = ExportReceipt {
                    height,
                    index: tx_index as u32,
                    contract_addr: hex::encode(receipt.contract_address),
                    cumulative_quota_used: hex::encode(receipt.cumulative_quota_used),
                    quota_used: hex::encode(receipt.quota_used),
                    error_msg: receipt.error_message,
                    logs_bloom: hex::encode(receipt.logs_bloom),
                    tx_hash: hex::encode(receipt.transaction_hash),
                };
                export_receipts.push(export_receipt);

                for log in receipt.logs {
                    let export_log = ExportLog {
                        address: hex::encode(log.address),
                        topics: format!(
                            "{:?}",
                            log.topics.iter().map(hex::encode).collect::<Vec<String>>()
                        ),
                        data: hex::encode(log.data),
                        height,
                        log_index: log.log_index,
                        tx_log_index: log.transaction_log_index,
                        tx_hash: hex::encode(log.transaction_hash),
                    };
                    export_logs.push(export_log);
                }
            } else {
                warn!("export failed: unknown tx type");
                return Err("unknown tx type");
            }
        }

        // write export data to kafka
        if let Some(export_sc) = opt_export_sc {
            let json_str = serde_json::to_string(&export_sc).unwrap();
            self.write(
                json_str,
                &format!(
                    "cita-cloud.{}.{}",
                    self.config.chain_name, SYSTEM_CONFIG_TOPIC
                ),
            )
            .await
            .map(|_| ())
            .map_err(|_| "faile send to kafka")?;
        }

        for export_utxo in export_utxos {
            let json_str = serde_json::to_string(&export_utxo).unwrap();
            self.write(
                json_str,
                &format!("cita-cloud.{}.{}", self.config.chain_name, UTXOS_TOPIC),
            )
            .await
            .map(|_| ())
            .map_err(|_| "faile send to kafka")?;
        }

        for export_tx in export_txs {
            let json_str = serde_json::to_string(&export_tx).unwrap();
            self.write(
                json_str,
                &format!("cita-cloud.{}.{}", self.config.chain_name, TXS_TOPIC),
            )
            .await
            .map(|_| ())
            .map_err(|_| "faile send to kafka")?;
        }

        for export_receipt in export_receipts {
            let json_str = serde_json::to_string(&export_receipt).unwrap();
            self.write(
                json_str,
                &format!("cita-cloud.{}.{}", self.config.chain_name, RECEITPS_TOPIC),
            )
            .await
            .map(|_| ())
            .map_err(|_| "faile send to kafka")?;
        }

        for export_log in export_logs {
            let json_str = serde_json::to_string(&export_log).unwrap();
            self.write(
                json_str,
                &format!("cita-cloud.{}.{}", self.config.chain_name, LOGS_TOPIC),
            )
            .await
            .map(|_| ())
            .map_err(|_| "faile send to kafka")?;
        }

        // block must send last, because we use it as progress
        let json_str = serde_json::to_string(&export_block).unwrap();
        self.write(
            json_str,
            &format!("cita-cloud.{}.{}", self.config.chain_name, BLOCKS_TOPIC),
        )
        .await
        .map(|offsetlist| {
            info!("write block {}, offsetlist: {:?}", height, offsetlist);
        })
        .map_err(|_| "faile send to kafka")?;

        Ok(())
    }
}
