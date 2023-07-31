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

use crate::{
    config::CloudStorage,
    util::{check_layer3_availability, full_to_compact, get_raw_key, get_real_key, u64_decode},
};
use async_recursion::async_recursion;
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    status_code::StatusCodeEnum,
    storage::Regions,
};
use cloud_util::common::get_tx_hash;
use opendal::{
    layers::RetryLayer,
    services::{Azblob, Cos, Memory, Obs, Oss, Rocksdb, S3},
    Builder, ErrorKind, Operator,
};
use prost::Message;
use rand::Rng;
use std::time::Duration;

#[derive(Clone)]
pub struct Storager {
    pub operator: Operator,
    pub next_storager: Option<Box<Storager>>,
    capacity: Option<u64>,
    layer: u8,
    scheme: String,
}

// build API
impl Storager {
    async fn build_one(
        builder: impl Builder,
        next: Option<Box<Storager>>,
        capacity: Option<u64>,
        layer: u8,
    ) -> Self {
        let operator = Operator::new(builder).unwrap().finish();
        if layer == 3 {
            check_layer3_availability(&operator).await;
        }
        let operator = operator.layer(RetryLayer::default());
        let scheme = operator.info().scheme().to_string();
        Self {
            operator,
            next_storager: next,
            capacity,
            layer,
            scheme,
        }
    }

    pub async fn build(
        data_root: &str,
        l3config: &CloudStorage,
        l1_capacity: u64,
        l2_capacity: u64,
        backup_interval: u64,
        retreat_interval: u64,
    ) -> Self {
        // if l3config is empty, storager3 is None
        let storager3 = if l3config.is_empty() {
            None
        } else {
            let storager3 = match l3config.service_type.as_str() {
                "oss" => {
                    let mut oss_builder = Oss::default();
                    oss_builder.access_key_id(l3config.access_key_id.as_str());
                    oss_builder.access_key_secret(l3config.secret_access_key.as_str());
                    oss_builder.endpoint(l3config.endpoint.as_str());
                    oss_builder.bucket(l3config.bucket.as_str());
                    oss_builder.root(l3config.root.as_str());
                    Storager::build_one(oss_builder, None, None, 3).await
                }
                "obs" => {
                    let mut obs_builder = Obs::default();
                    obs_builder.access_key_id(l3config.access_key_id.as_str());
                    obs_builder.secret_access_key(l3config.secret_access_key.as_str());
                    obs_builder.endpoint(l3config.endpoint.as_str());
                    obs_builder.bucket(l3config.bucket.as_str());
                    obs_builder.root(l3config.root.as_str());
                    Storager::build_one(obs_builder, None, None, 3).await
                }
                "cos" => {
                    let mut cos_builder = Cos::default();
                    cos_builder.secret_id(l3config.access_key_id.as_str());
                    cos_builder.secret_key(l3config.secret_access_key.as_str());
                    cos_builder.endpoint(l3config.endpoint.as_str());
                    cos_builder.bucket(l3config.bucket.as_str());
                    cos_builder.root(l3config.root.as_str());
                    Storager::build_one(cos_builder, None, None, 3).await
                }
                "s3" => {
                    let mut s3_builder = S3::default();
                    s3_builder.access_key_id(l3config.access_key_id.as_str());
                    s3_builder.secret_access_key(l3config.secret_access_key.as_str());
                    s3_builder.endpoint(l3config.endpoint.as_str());
                    s3_builder.bucket(l3config.bucket.as_str());
                    s3_builder.root(l3config.root.as_str());
                    Storager::build_one(s3_builder, None, None, 3).await
                }
                "azblob" => {
                    let mut azblob_builder = Azblob::default();
                    azblob_builder.account_name(l3config.access_key_id.as_str());
                    azblob_builder.account_key(l3config.secret_access_key.as_str());
                    azblob_builder.endpoint(l3config.endpoint.as_str());
                    azblob_builder.container(l3config.bucket.as_str());
                    azblob_builder.root(l3config.root.as_str());
                    Storager::build_one(azblob_builder, None, None, 3).await
                }
                _ => unimplemented!(),
            };
            info!(
                "build storager: layer: {}, scheme: {}",
                storager3.layer, storager3.scheme
            );
            Some(storager3)
        };

        let mut rocksdb_builder = Rocksdb::default();
        rocksdb_builder.datadir(data_root);
        let storager2 = if let Some(storager3) = storager3 {
            let storager2 = Storager::build_one(
                rocksdb_builder,
                Some(Box::new(storager3)),
                Some(l2_capacity),
                2,
            )
            .await;
            let storager2_for_backup = storager2.clone();
            tokio::spawn(async move {
                backup(storager2_for_backup, backup_interval, retreat_interval).await
            });
            storager2
        } else {
            Storager::build_one(rocksdb_builder, None, None, 2).await
        };
        info!(
            "build storager: layer: {}, scheme: {}",
            storager2.layer, storager2.scheme
        );

        let mem_builder = Memory::default();
        let storager1 =
            Storager::build_one(mem_builder, Some(Box::new(storager2)), Some(l1_capacity), 1).await;
        info!(
            "build storager: layer: {}, scheme: {}",
            storager1.layer, storager1.scheme
        );
        storager1
    }
}

impl Storager {
    async fn _is_exist(&self, key: &str) -> bool {
        self.operator.is_exist(key).await.unwrap_or(true)
    }

    // collect keys by heihgt
    async fn collect_keys(
        &self,
        height: u64,
        recursive: bool,
    ) -> Result<Vec<String>, StatusCodeEnum> {
        let height_bytes = height.to_be_bytes().to_vec();

        let mut keys = Vec::new();

        let compact_block_key = get_real_key(Regions::CompactBlock, &height_bytes);
        let compact_block_bytes = self.load(&compact_block_key, recursive).await?;
        let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
            warn!(
                "collect keys({}) failed: decode CompactBlock failed",
                height
            );
            StatusCodeEnum::EncodeError
        })?;
        if let Some(compact_body) = compact_block.body {
            for tx_hash in compact_body.tx_hashes {
                keys.push(get_real_key(Regions::Transactions, &tx_hash));
                keys.push(get_real_key(Regions::TransactionHash2blockHeight, &tx_hash));
                keys.push(get_real_key(Regions::TransactionIndex, &tx_hash));
            }
        }
        keys.push(compact_block_key);

        let block_hash_key = get_real_key(Regions::BlockHash, &height_bytes);
        let hash = self.load(&block_hash_key, recursive).await?;
        keys.push(block_hash_key);
        keys.push(get_real_key(Regions::BlockHash2blockHeight, &hash));

        keys.push(get_real_key(Regions::Proof, &height_bytes));
        keys.push(get_real_key(Regions::Result, &height_bytes));

        Ok(keys)
    }

    async fn delete_outdate(&self, delete_height: u64) -> Result<(), StatusCodeEnum> {
        let delete_keys = self.collect_keys(delete_height, true).await?;
        let mut result = Ok(());
        for k in delete_keys {
            if let Err(e) = self.operator.delete(&k).await {
                result = Err(StatusCodeEnum::DeleteError);
                let (region, key) = get_raw_key(&k);
                warn!("delete outdate failed: error: {:?}. layer: {}, scheme: {}. region: {}, key: {}", e, self.layer, self.scheme, region, key);
            }
        }
        info!(
            "delete outdate({}) down: layer: {}, scheme: {}",
            delete_height, self.layer, self.scheme
        );
        result
    }
}

// storage API
impl Storager {
    #[async_recursion]
    pub async fn store(&self, real_key: &str, value: &[u8]) -> Result<(), StatusCodeEnum> {
        let (region, key) = get_raw_key(real_key);

        match self.operator.write(real_key, value.to_owned()).await {
            Ok(()) => match self.layer {
                1 => {
                    // layer 1 will wait for layer 2 to store
                    if let Some(next) = self.next_storager.as_ref() {
                        next.store(real_key, value).await?;
                        if region == Regions::Global as u32 && key == *"0" {
                            let height = u64_decode(value);
                            info!(
                                "store block({}) succeed: layer: {}, scheme: {}",
                                height, self.layer, self.scheme
                            );
                        }
                    }
                    Ok(())
                }
                2 => {
                    if region == Regions::Global as u32 && key == *"0" {
                        let height = u64_decode(value);
                        info!(
                            "store block({}) succeed: layer: {}, scheme: {}",
                            height, self.layer, self.scheme
                        );
                    }
                    Ok(())
                }
                3 => Ok(()),
                i => unimplemented!("not support layer: {}", i),
            },
            Err(e) => {
                warn!(
                    "store data failed: {}. layer: {}, scheme: {}. region: {}, key: {}",
                    e.kind(),
                    self.layer,
                    self.scheme,
                    region,
                    key
                );
                Err(StatusCodeEnum::StoreError)
            }
        }
    }

    #[async_recursion]
    pub async fn load(&self, real_key: &str, recursive: bool) -> Result<Vec<u8>, StatusCodeEnum> {
        let (region, key) = get_raw_key(real_key);
        match self.operator.read(real_key).await {
            Ok(v) => {
                // region 5 is Proof, only get full block will load it
                if region == Regions::Proof as u32 && recursive {
                    info!(
                        "load block({}) succeed: layer: {}, scheme: {}",
                        key, self.layer, self.scheme
                    );
                }
                Ok(v)
            }
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    // if recursive load, try to load from next layer
                    if recursive {
                        if let Some(storager) = self.next_storager.as_ref() {
                            storager.load(real_key, recursive).await
                        } else {
                            Err(StatusCodeEnum::NotFound)
                        }
                    } else {
                        Err(StatusCodeEnum::NotFound)
                    }
                }
                e => {
                    warn!(
                        "load data failed: {}. layer: {}, scheme: {}. region: {}, key: {}",
                        e, self.layer, self.scheme, region, key
                    );
                    Err(StatusCodeEnum::LoadError)
                }
            },
        }
    }
}

// predefined batch operation API
impl Storager {
    pub async fn store_all_block_data(
        &self,
        height_bytes: &[u8],
        block_bytes: &[u8],
    ) -> Result<(), StatusCodeEnum> {
        let height = u64_decode(height_bytes);
        info!("store block({}): start", height);

        let block_hash = block_bytes[..32].to_vec();

        let block = Block::decode(&block_bytes[32..]).map_err(|_| {
            warn!("store block({}) failed: decode Block failed", height);
            StatusCodeEnum::DecodeError
        })?;

        let mut handles = vec![];
        for (tx_index, raw_tx) in block
            .body
            .clone()
            .ok_or(StatusCodeEnum::NoneBlockBody)?
            .body
            .into_iter()
            .enumerate()
        {
            let storager = self.clone();
            let height_bytes = height_bytes.to_owned();
            let h: tokio::task::JoinHandle<Result<(), StatusCodeEnum>> = tokio::spawn(async move {
                let mut tx_bytes = Vec::new();
                raw_tx.encode(&mut tx_bytes).map_err(|_| {
                    warn!(
                        "store block({}) failed: encode RawTransaction failed",
                        height
                    );
                    StatusCodeEnum::EncodeError
                })?;

                let tx_hash = get_tx_hash(&raw_tx)?.to_vec();
                storager
                    .store(&get_real_key(Regions::Transactions, &tx_hash), &tx_bytes)
                    .await?;

                storager
                    .store(
                        &get_real_key(Regions::TransactionHash2blockHeight, &tx_hash),
                        &height_bytes,
                    )
                    .await?;
                storager
                    .store(
                        &get_real_key(Regions::TransactionIndex, &tx_hash),
                        &tx_index.to_be_bytes(),
                    )
                    .await?;
                Ok(())
            });
            handles.push(h);
        }
        for h in handles {
            h.await
                .map_err(|_| StatusCodeEnum::StoreError)?
                .map_err(|e| {
                    warn!("store block({}) failed: store tx failed: {}", height, e);
                    e
                })?;
        }

        self.store(&get_real_key(Regions::BlockHash, height_bytes), &block_hash)
            .await?;
        self.store(
            &get_real_key(Regions::Result, height_bytes),
            &block.state_root,
        )
        .await?;
        self.store(
            &get_real_key(Regions::BlockHash2blockHeight, &block_hash),
            height_bytes,
        )
        .await?;

        let compact_block = full_to_compact(block.clone());
        let mut compact_block_bytes = Vec::new();
        compact_block
            .encode(&mut compact_block_bytes)
            .map_err(|_| {
                warn!("store block({}) failed: encode CompactBlock failed", height);
                StatusCodeEnum::EncodeError
            })?;
        self.store(
            &get_real_key(Regions::CompactBlock, height_bytes),
            &compact_block_bytes,
        )
        .await?;

        self.store(&get_real_key(Regions::Proof, height_bytes), &block.proof)
            .await?;

        self.store(
            &get_real_key(Regions::Global, &0u64.to_be_bytes()),
            height_bytes,
        )
        .await?;

        if let Some(capacity) = self.capacity {
            if height >= capacity {
                let height = u64_decode(height_bytes);
                let storager_for_delete = self.clone();
                tokio::spawn(async move {
                    let _ = storager_for_delete.delete_outdate(height - capacity).await;
                });
            }
        }

        Ok(())
    }

    pub async fn load_full_block(&self, height_bytes: &[u8]) -> Result<Vec<u8>, StatusCodeEnum> {
        let height = u64_decode(height_bytes);
        // get compact_block
        let compact_block_bytes = self
            .load(&get_real_key(Regions::CompactBlock, height_bytes), true)
            .await?;
        let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
            warn!("load block({}) failed: decode CompactBlock failed", height);
            StatusCodeEnum::EncodeError
        })?;

        let mut body = Vec::new();
        let mut handles = vec![];
        if let Some(compact_body) = compact_block.body {
            for tx_hash in compact_body.tx_hashes {
                let storager = self.clone();
                let h: tokio::task::JoinHandle<Result<RawTransaction, StatusCodeEnum>> =
                    tokio::spawn(async move {
                        let tx_bytes = storager
                            .load(&get_real_key(Regions::Transactions, &tx_hash), true)
                            .await?;
                        let raw_tx = RawTransaction::decode(tx_bytes.as_slice()).map_err(|_| {
                            warn!(
                                "load block({}) failed: decode RawTransaction failed",
                                height
                            );
                            StatusCodeEnum::DecodeError
                        })?;
                        Ok(raw_tx)
                    });
                handles.push(h);
            }
        }
        for h in handles {
            let raw_tx = h
                .await
                .map_err(|_| StatusCodeEnum::LoadError)?
                .map_err(|e| {
                    warn!("load block({}) failed: load tx failed: {}", height, e);
                    e
                })?;
            body.push(raw_tx)
        }

        let proof = self
            .load(&get_real_key(Regions::Proof, height_bytes), true)
            .await?;

        let state_root = self
            .load(&get_real_key(Regions::Result, height_bytes), true)
            .await?;

        let block = Block {
            version: compact_block.version,
            header: compact_block.header,
            body: Some(RawTransactions { body }),
            proof,
            state_root,
        };

        let mut block_bytes = Vec::new();
        block.encode(&mut block_bytes).map_err(|_| {
            warn!("load block({}) failed: encode Block failed", height);
            StatusCodeEnum::EncodeError
        })?;

        Ok(block_bytes)
    }
}

async fn backup(local: Storager, backup_interval_secs: u64, retreat_interval_secs: u64) {
    let capacity = local.capacity.unwrap();
    let remote = local.next_storager.as_ref().unwrap();
    let local_height_real_key = get_real_key(Regions::Global, &0u64.to_be_bytes());
    let remote_height_real_key = get_real_key(Regions::Global, &1u64.to_be_bytes());
    let delete_real_key = get_real_key(Regions::Global, &2u64.to_be_bytes());

    let min_interval = backup_interval_secs * 2 / 3;
    let max_interval = backup_interval_secs * 4 / 3;
    'backup_round: loop {
        // sleep
        let backup_interval = {
            let mut rng = rand::thread_rng();
            rng.gen_range(min_interval..=max_interval)
        };
        tokio::time::sleep(Duration::from_secs(backup_interval)).await;

        // avoid concurrent backup
        let mut remote_height = match remote.load(&remote_height_real_key, false).await {
            Ok(value) => u64_decode(&value),
            Err(e) => {
                if e == StatusCodeEnum::NotFound {
                    0
                } else {
                    warn!(
                        "backup failed: load remote height failed: {}. layer: {}, scheme: {}. skip this round",
                        e.to_string(),
                        remote.layer,
                        remote.scheme
                    );
                    continue;
                }
            }
        };
        loop {
            tokio::time::sleep(Duration::from_secs(retreat_interval_secs)).await;
            let new_remote_height = match remote.load(&remote_height_real_key, false).await {
                Ok(value) => u64_decode(&value),
                Err(e) => {
                    if e == StatusCodeEnum::NotFound {
                        0
                    } else {
                        warn!(
                            "backup failed: load remote height failed: {}. layer: {}, scheme: {}. skip this round",
                            e.to_string(),
                            remote.layer,
                            remote.scheme
                        );
                        continue 'backup_round;
                    }
                }
            };
            if remote_height == new_remote_height {
                break;
            } else {
                remote_height = new_remote_height;
                warn!("backup retreat");
            }
        }

        // backup
        let local_height = match local.load(&local_height_real_key, false).await {
            Ok(value) => u64_decode(&value),
            Err(e) => {
                warn!(
                    "backup failed: load local height failed: {}. layer: {}, scheme: {}. skip this round",
                    e.to_string(),
                    local.layer,
                    local.scheme
                );
                continue;
            }
        };
        let remote_target = local_height - 1;
        if remote_height >= remote_target {
            info!("remote is up to date. skip this round");
            continue;
        }
        // for storing genesis block
        let buckup_start = if remote_height == 0 {
            0
        } else {
            remote_height + 1
        };
        info!(
            "backup {} - {}: layer{}: {} to layer{}: {}",
            buckup_start, remote_target, local.layer, local.scheme, remote.layer, remote.scheme
        );
        for height in buckup_start..=remote_target {
            let real_keys = match local.collect_keys(height, false).await {
                Ok(keys) => keys,
                Err(e) => {
                    warn!(
                        "backup({}) failed: collect keys failed: {}. layer: {}, scheme: {}. skip this round",
                        height,
                        e.to_string(),
                        local.layer,
                        local.scheme
                    );
                    continue 'backup_round;
                }
            };
            let mut handles = vec![];
            for real_key in real_keys {
                let local = local.clone();
                let remote = remote.clone();
                let handle = tokio::spawn({
                    async move {
                        let value = match local.load(&real_key, false).await {
                            Ok(value) => value,
                            Err(e) => {
                                let (region, key) = get_raw_key(&real_key);
                                warn!(
                                    "backup({}) failed: load failed: {}. region: {}, key: {}. layer: {}, scheme: {}. skip this round",
                                    height,
                                    e.to_string(),
                                    region,
                                    key,
                                    local.layer,
                                    local.scheme
                                );
                                return false;
                            }
                        };
                        if let Err(e) = remote.store(&real_key, &value).await {
                            let (region, key) = get_raw_key(&real_key);
                            warn!(
                                    "backup({}) failed: store failed: {}. region: {}, key: {}. layer: {}, scheme: {}. skip this round",
                                    height,
                                    e.to_string(),
                                    region,
                                    key,
                                    remote.layer,
                                    remote.scheme
                                );
                            false
                        } else {
                            true
                        }
                    }
                });
                handles.push(handle);
            }
            // update backup height
            for handle in handles {
                if !handle.await.unwrap() {
                    continue 'backup_round;
                }
            }
            if let Err(e) = remote
                .store(&remote_height_real_key, &height.to_be_bytes())
                .await
            {
                warn!(
                    "backup({}) failed: update backup height failed: {}. layer: {}, scheme: {}. skip this round",
                    height,
                    e.to_string(),
                    remote.layer,
                    remote.scheme
                );
                continue 'backup_round;
            }
            info!(
                "backup({}) succeed: layer: {}, scheme: {}",
                height, remote.layer, remote.scheme
            );
        }

        // delete outdate
        let delete_height = match local.load(&delete_real_key, false).await {
            Ok(value) => u64_decode(&value),
            Err(e) => {
                if e == StatusCodeEnum::NotFound {
                    0
                } else {
                    warn!(
                        "backup failed: load delete height failed: {}. layer: {}, scheme: {}. skip this round",
                        e.to_string(),
                        local.layer,
                        local.scheme
                    );
                    continue;
                }
            }
        };
        if local_height - delete_height > capacity {
            info!(
                "delete outdate {} - {}: layer: {}, scheme: {}",
                delete_height + 1,
                local_height - capacity,
                local.layer,
                local.scheme
            );
            for height in delete_height + 1..=local_height - capacity {
                if let Err(e) = local.delete_outdate(height).await {
                    if e != StatusCodeEnum::NotFound {
                        warn!(
                            "delete outdate({}) failed: {}. layer: {}, scheme: {}. skip this round",
                            height,
                            e.to_string(),
                            local.layer,
                            local.scheme
                        );
                        continue 'backup_round;
                    } else {
                        warn!(
                            "delete outdate({}) failed: {}. layer: {}, scheme: {}. already deleted",
                            height,
                            e.to_string(),
                            local.layer,
                            local.scheme
                        );
                    }
                }
                // update delete height
                if let Err(e) = local.store(&delete_real_key, &height.to_be_bytes()).await {
                    warn!(
                        "delete outdate({}) failed: update delete height failed: {}. layer: {}, scheme: {}. skip this round",
                        height,
                        e.to_string(),
                        local.layer,
                        local.scheme
                    );
                    continue 'backup_round;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::get_real_key_by_u32;

    use super::*;
    use quickcheck::quickcheck;
    use quickcheck::Arbitrary;
    use quickcheck::Gen;
    use tempfile::tempdir;

    #[derive(Clone, Debug)]
    struct DBTestArgs {
        region: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    }

    impl Arbitrary for DBTestArgs {
        fn arbitrary(g: &mut Gen) -> Self {
            let region = u32::arbitrary(g) % 10;
            let key = match region {
                1 | 7 | 8 | 9 | 10 => {
                    let mut k = Vec::with_capacity(32);
                    for _ in 0..4 {
                        let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                        k.extend_from_slice(&bytes);
                    }
                    k
                }
                _ => {
                    let mut k = Vec::with_capacity(8);
                    let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                    k.extend_from_slice(&bytes);
                    k
                }
            };

            let value = match region {
                0 | 7 | 8 | 9 => {
                    let mut v = Vec::with_capacity(8);
                    let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                    v.extend_from_slice(&bytes);
                    v
                }
                _ => {
                    let mut v = Vec::with_capacity(32);
                    for _ in 0..4 {
                        let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                        v.extend_from_slice(&bytes);
                    }
                    v
                }
            };

            DBTestArgs { region, key, value }
        }
    }

    quickcheck! {
        fn prop(args: DBTestArgs) -> bool {
            let dir = tempdir().unwrap();
            let path = dir.path().to_str().unwrap();

            let region = args.region;
            let key = args.key.clone();
            let value = args.value;
            let real_key = get_real_key_by_u32(region, &key);

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let storager = Storager::build(path, &CloudStorage::default(), 10, 20, 10, 3).await;
                storager.store(&real_key, &value).await.unwrap();
                storager.load(&real_key, false).await.unwrap() == value
            })
        }
    }
}
