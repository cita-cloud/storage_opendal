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

use cloud_util::{common::read_toml, tracer::LogConfig};
use serde::Serialize;
use serde_derive::Deserialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudStorage {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub bucket: String,
}

impl Default for CloudStorage {
    fn default() -> Self {
        Self {
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            endpoint: "".to_string(),
            bucket: "".to_string(),
        }
    }
}

impl CloudStorage {
    pub fn is_empty(&self) -> bool {
        self.access_key_id.is_empty()
            && self.secret_access_key.is_empty()
            && self.endpoint.is_empty()
            && self.bucket.is_empty()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct StorageConfig {
    pub storage_port: u16,
    pub data_root: String,
    pub enable_metrics: bool,
    pub metrics_port: u16,
    pub metrics_buckets: Vec<f64>,
    /// log config
    pub log_config: LogConfig,
    /// domain
    pub domain: String,

    pub l1_capacity: u64,
    // invalid if cloud_storage is empty
    pub l2_capacity: u64,
    // invalid if cloud_storage is empty
    pub backup_interval: u64,
    pub retreat_interval: u64,
    pub cloud_storage: CloudStorage,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_port: 50003,
            data_root: "chain_data".to_string(),
            enable_metrics: true,
            metrics_port: 60003,
            metrics_buckets: vec![
                0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0,
            ],
            log_config: Default::default(),
            domain: Default::default(),
            l1_capacity: 20,
            l2_capacity: 1000,
            backup_interval: 300,
            retreat_interval: 5,
            cloud_storage: Default::default(),
        }
    }
}

impl StorageConfig {
    pub fn new(config_str: &str) -> Self {
        read_toml(config_str, "storage_opendal")
    }
}

#[cfg(test)]
mod tests {
    use super::StorageConfig;

    #[test]
    fn basic_test() {
        let config = StorageConfig::new("example/config.toml");

        assert_eq!(config.storage_port, 60003);
        assert_eq!(config.domain, "test-chain-node1");
        assert_eq!(config.l1_capacity, 10);
        assert_eq!(config.l2_capacity, 20);
    }
}
