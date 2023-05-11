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

use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx, Block, CompactBlock, CompactBlockBody},
    storage::Regions,
};

pub fn check_region(region: u32) -> bool {
    region < Regions::Button as u8 as u32
}

pub fn check_key(region: u32, key: &[u8]) -> bool {
    match region {
        1 | 7 | 8 | 9 => key.len() == 32,
        _ => key.len() == 8,
    }
}

pub fn check_value(region: u32, value: &[u8]) -> bool {
    match region {
        4 | 6 => value.len() == 32,
        7 | 8 => value.len() == 8,
        _ => true,
    }
}

pub fn u32_decode(data: &[u8]) -> u32 {
    u32::from_be_bytes(data.try_into().unwrap())
}

pub fn u64_decode(data: &[u8]) -> u64 {
    u64::from_be_bytes(data.try_into().unwrap())
}

pub fn get_real_key(region: u32, key: &[u8]) -> String {
    hex::encode([region.to_be_bytes().as_slice(), key].concat())
}

pub fn get_raw_key(real_key: &str) -> (u32, String) {
    let concat = hex::decode(real_key).unwrap();
    let region = u32_decode(&concat[..4]);
    let raw_key = &concat[4..];
    let raw_key_string = if raw_key.len() == 8 {
        u64_decode(raw_key).to_string()
    } else {
        "0x".to_string() + &hex::encode(raw_key)
    };
    (region, raw_key_string)
}

pub fn full_to_compact(block: Block) -> CompactBlock {
    let mut compact_body = CompactBlockBody { tx_hashes: vec![] };

    if let Some(body) = block.body {
        for raw_tx in body.body {
            match raw_tx.tx {
                Some(Tx::NormalTx(normal_tx)) => {
                    compact_body.tx_hashes.push(normal_tx.transaction_hash)
                }
                Some(Tx::UtxoTx(utxo_tx)) => compact_body.tx_hashes.push(utxo_tx.transaction_hash),
                None => {}
            }
        }
    }

    CompactBlock {
        version: block.version,
        header: block.header,
        body: Some(compact_body),
    }
}

pub fn clap_about() -> String {
    let name = env!("CARGO_PKG_NAME").to_string();
    let version = env!("CARGO_PKG_VERSION");
    let authors = env!("CARGO_PKG_AUTHORS");
    name + " " + version + "\n" + authors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_key_convert_test() {
        let region = 5u32;
        let raw_key = 16u64;
        let real_key = get_real_key(region, &raw_key.to_be_bytes());
        let (region, key) = get_raw_key(&real_key);
        assert_eq!(region, 5u32);
        assert_eq!(key, 16.to_string());

        let raw_key = hex::decode("c356876e7f4831476f99ea0593b0cd7a6053e4d3").unwrap();
        let real_key = get_real_key(region, &raw_key);
        let (region, key) = get_raw_key(&real_key);
        assert_eq!(region, 5u32);
        assert_eq!(key, "0xc356876e7f4831476f99ea0593b0cd7a6053e4d3");
    }
}
