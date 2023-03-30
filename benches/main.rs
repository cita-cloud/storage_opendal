use cita_cloud_proto::{
    client::{ClientOptions, StorageClientTrait},
    storage::{Content, ExtKey},
};
use std::{env, time::Instant};

pub async fn store_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let client_options = ClientOptions::new(
        "bench".to_string(),
        format!("http://127.0.0.1:{}", storage_port),
    );
    let client = match client_options.connect_storage() {
        Ok(retry_client) => retry_client,
        Err(e) => panic!("client init error: {:?}", &e),
    };

    let request = Content { region, key, value };

    client.store(request).await?;
    Ok(true)
}

pub async fn load_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let client_options = ClientOptions::new(
        "bench".to_string(),
        format!("http://127.0.0.1:{}", storage_port),
    );
    let client = match client_options.connect_storage() {
        Ok(retry_client) => retry_client,
        Err(e) => panic!("client init error: {:?}", &e),
    };

    let request = ExtKey { region, key };

    client.load(request).await?;
    Ok(true)
}

pub async fn delete_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let client_options = ClientOptions::new(
        "bench".to_string(),
        format!("http://127.0.0.1:{}", storage_port),
    );
    let client = match client_options.connect_storage() {
        Ok(retry_client) => retry_client,
        Err(e) => panic!("client init error: {:?}", &e),
    };

    let request = ExtKey { region, key };

    client.delete(request).await?;
    Ok(true)
}

#[tokio::main]
async fn bench_store() -> Result<(), Box<dyn std::error::Error>> {
    let data = "0".as_bytes().to_vec();

    let mut store_spend = 0.0;
    let mut start = Instant::now();
    for i in 0..10000u64 {
        let _ = store_data(60003, 5, i.to_be_bytes().to_vec(), data.clone()).await?;
        match i {
            999 => {
                start = Instant::now();
                println!("bench store start!");
            }
            3999 | 6999 | 9999 => {
                let elapse = start.elapsed().as_secs_f64() * 1000f64;
                println!(
                    "\tround: {} finish: spend: {} ms, store 3000 times",
                    ((i - 1000) + 1) / 3000,
                    elapse
                );
                start = Instant::now();
                store_spend += elapse;
            }
            _ => {}
        }
    }
    let req_s = 9000.0 / store_spend * 1000.0;
    let ms_req = 1.0 / req_s * 1000.0;
    println!(
        "bench store finish! speed: {} req/s, {} ms/req\n",
        req_s, ms_req
    );
    Ok(())
}

#[tokio::main]
async fn bench_load() -> Result<(), Box<dyn std::error::Error>> {
    // delete_data(50003, 5, 0u64.to_be_bytes().to_vec()).await?;

    // let heights = vec![9999u64, 9500, 5000];
    // for (index, height) in heights.iter().enumerate() {
    //     let mut load_spend = 0.0;
    //     let mut start = Instant::now();
    //     for i in 0..10000u64 {
    //         let _ = load_data(60003, 5, height.to_be_bytes().to_vec()).await?;
    //         match i {
    //             999 => {
    //                 start = Instant::now();
    //                 println!("bench load layer{} start!", index + 1);
    //             }
    //             3999 | 6999 | 9999 => {
    //                 let elapse = start.elapsed().as_secs_f64() * 1000f64;
    //                 println!(
    //                     "\tlayer{} round: {} finish: spend: {} ms, store 3000 times",
    //                     index + 1,
    //                     ((i - 1000) + 1) / 3000,
    //                     elapse
    //                 );
    //                 start = Instant::now();
    //                 load_spend += elapse;
    //             }
    //             _ => {}
    //         }
    //     }
    //     let req_s = 9000.0 / load_spend * 1000.0;
    //     let ms_req = 1.0 / req_s * 1000.0;
    //     println!(
    //         "bench load layer{} finish! speed: {} req/s, {} ms/req\n",
    //         index + 1,
    //         req_s,
    //         ms_req
    //     );
    // }

    Ok(())
}

fn main() {
    let args = env::args();
    match args.into_iter().nth(1).unwrap().as_str() {
        "store" => bench_store().unwrap(),
        "load" => bench_load().unwrap(),
        _ => panic!("only store or load"),
    }
}
