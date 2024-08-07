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

mod config;
mod exporter;
mod health_check;
mod storager;
mod util;

#[macro_use]
extern crate tracing;

use crate::config::StorageConfig;
use crate::health_check::HealthCheckServer;
use crate::util::clap_about;
use crate::util::get_real_key;
use crate::util::get_real_key_by_u32;
use crate::util::u64_decode;
use crate::util::{check_key, check_region, check_value};
use cita_cloud_proto::common::StatusCode;
use cita_cloud_proto::health_check::health_server::HealthServer;
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::storage::Regions;
use cita_cloud_proto::storage::{
    storage_service_server::StorageService, storage_service_server::StorageServiceServer, Content,
    ExtKey, Value,
};
use clap::Parser;
use cloud_util::metrics::{run_metrics_exporter, MiddlewareLayer};
use std::net::AddrParseError;
use std::path::Path;
use std::sync::Arc;
use storager::Storager;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Parser)]
#[clap(version, about = clap_about())]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Parser)]
struct RunOpts {
    /// Chain config path
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::Run(opts) => {
            if let Err(e) = run(opts) {
                warn!("unreachable: {:?}", e);
            }
        }
    }
}

pub struct StorageServer {
    storager: Arc<Storager>,
}

impl StorageServer {
    fn new(storager: Arc<Storager>) -> Self {
        StorageServer { storager }
    }
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    #[instrument(skip_all)]
    async fn store(&self, request: Request<Content>) -> Result<Response<StatusCode>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("store request: {:?}", request);

        let content = request.into_inner();
        let region = content.region;
        let key = content.key;
        let value = content.value;

        if !check_region(region) {
            return Ok(Response::new(StatusCodeEnum::InvalidRegion.into()));
        }
        if !check_key(region, &key) {
            return Ok(Response::new(StatusCodeEnum::InvalidKey.into()));
        }
        if !check_value(region, &value) {
            return Ok(Response::new(StatusCodeEnum::InvalidValue.into()));
        }
        let real_key = get_real_key_by_u32(region, &key);

        match Regions::try_from(region as i32).unwrap() {
            Regions::AllBlockData => match self.storager.store_all_block_data(&key, &value).await {
                Ok(()) => Ok(Response::new(StatusCodeEnum::Success.into())),
                Err(status) => {
                    let height = u64_decode(&key);
                    warn!("store block({}) failed: {}", height, status.to_string());
                    Ok(Response::new(status.into()))
                }
            },
            Regions::TransactionsPool => {
                match self.storager.store_transactions_pool(&value).await {
                    Ok(()) => Ok(Response::new(StatusCodeEnum::Success.into())),
                    Err(status) => {
                        warn!("store transactions failed: {}", status.to_string());
                        Ok(Response::new(status.into()))
                    }
                }
            }
            _ => match self.storager.store(&real_key, &value).await {
                Ok(()) => Ok(Response::new(StatusCodeEnum::Success.into())),
                Err(status) => Ok(Response::new(status.into())),
            },
        }
    }

    #[instrument(skip_all)]
    async fn load(&self, request: Request<ExtKey>) -> Result<Response<Value>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("load request: {:?}", request);

        let ext_key = request.into_inner();
        let region = ext_key.region;
        let key = ext_key.key;

        if !check_region(region) {
            return Ok(Response::new(Value {
                status: Some(StatusCodeEnum::InvalidRegion.into()),
                value: vec![],
            }));
        }
        if !check_key(region, &key) {
            return Ok(Response::new(Value {
                status: Some(StatusCodeEnum::InvalidKey.into()),
                value: vec![],
            }));
        }
        let real_key = get_real_key_by_u32(region, &key);

        match Regions::try_from(region as i32).unwrap() {
            Regions::FullBlock => match self.storager.load_full_block(&key).await {
                Ok(value) => Ok(Response::new(Value {
                    status: Some(StatusCodeEnum::Success.into()),
                    value,
                })),
                Err(status) => {
                    let height = u64_decode(&key);
                    warn!("load block({}) failed: {}", height, status.to_string());
                    Ok(Response::new(Value {
                        status: Some(status.into()),
                        value: vec![],
                    }))
                }
            },
            Regions::TransactionsPool => match self.storager.reload_transactions_pool().await {
                Ok(value) => Ok(Response::new(Value {
                    status: Some(StatusCodeEnum::Success.into()),
                    value,
                })),
                Err(status) => {
                    warn!("load transactions failed: {}", status.to_string());
                    Ok(Response::new(Value {
                        status: Some(status.into()),
                        value: vec![],
                    }))
                }
            },
            Regions::Global if key == 1u64.to_be_bytes().to_vec() => {
                let height_real_key = get_real_key_by_u32(region, &0u64.to_be_bytes());
                match self.storager.load(&height_real_key, true).await {
                    Ok(height) => {
                        let hash_real_key = get_real_key(Regions::BlockHash, &height);
                        match self.storager.load(&hash_real_key, true).await {
                            Ok(value) => Ok(Response::new(Value {
                                status: Some(StatusCodeEnum::Success.into()),
                                value,
                            })),
                            Err(status) => {
                                warn!("load failed: {}", status.to_string());
                                Ok(Response::new(Value {
                                    status: Some(status.into()),
                                    value: vec![],
                                }))
                            }
                        }
                    }
                    Err(status) => {
                        warn!("load failed: {}", status.to_string());
                        Ok(Response::new(Value {
                            status: Some(status.into()),
                            value: vec![],
                        }))
                    }
                }
            }
            _ => match self.storager.load(&real_key, true).await {
                Ok(value) => Ok(Response::new(Value {
                    status: Some(StatusCodeEnum::Success.into()),
                    value,
                })),
                Err(status) => Ok(Response::new(Value {
                    status: Some(status.into()),
                    value: vec![],
                })),
            },
        }
    }

    // only used by bench
    #[instrument(skip_all)]
    async fn delete(&self, request: Request<ExtKey>) -> Result<Response<StatusCode>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("delete request: {:?}", request);

        let ext_key = request.into_inner();
        let region = ext_key.region;
        let key = ext_key.key;

        if !check_region(region) {
            return Ok(Response::new(StatusCodeEnum::InvalidRegion.into()));
        }
        if !check_key(region, &key) {
            return Ok(Response::new(StatusCodeEnum::InvalidKey.into()));
        }

        // unused
        Ok(Response::new(StatusCodeEnum::Success.into()))
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCodeEnum> {
    let rx_signal = cloud_util::graceful_shutdown::graceful_shutdown();

    let config = StorageConfig::new(&opts.config_path);

    // init tracer
    cloud_util::tracer::init_tracer(config.domain.clone(), &config.log_config)
        .map_err(|e| println!("tracer init err: {e}"))
        .unwrap();

    info!("storage grpc port: {}", &config.storage_port);

    // storager_path must be relative path
    assert!(
        !Path::new(&config.data_root).is_absolute(),
        "storager_path must be relative path"
    );
    info!("storager data root: {}", &config.data_root);

    let addr_str = format!("[::]:{}", config.storage_port);
    let addr = addr_str.parse().map_err(|e: AddrParseError| {
        warn!("parse grpc listen address failed: {} ", e);
        StatusCodeEnum::FatalError
    })?;
    // init storager
    let storager = Arc::new(
        Storager::build(
            &config.data_root,
            &config.cloud_storage,
            &config.exporter,
            config.l1_capacity,
            config.l2_capacity,
        )
        .await,
    );
    let storage_server = StorageServer::new(storager.clone());

    let layer = if config.enable_metrics {
        tokio::spawn(async move {
            run_metrics_exporter(config.metrics_port).await.unwrap();
        });

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    info!("start storage_opendal grpc server");
    if let Some(layer) = layer {
        info!("metrics on");
        Server::builder()
            .layer(layer)
            .add_service(
                StorageServiceServer::new(storage_server).max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(storager)))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
            )
            .await
            .map_err(|e| {
                warn!("start storage_opendal grpc server failed: {:?}", e);
                StatusCodeEnum::FatalError
            })?;
    } else {
        info!("metrics off");
        Server::builder()
            .add_service(
                StorageServiceServer::new(storage_server).max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(storager)))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
            )
            .await
            .map_err(|e| {
                warn!("start storage_opendal grpc server failed: {:?}", e);
                StatusCodeEnum::FatalError
            })?;
    }

    Ok(())
}
