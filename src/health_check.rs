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

use crate::{storager::Storager, util::get_real_key};
use cita_cloud_proto::{
    health_check::{
        health_check_response::ServingStatus, health_server::Health, HealthCheckRequest,
        HealthCheckResponse,
    },
    storage::Regions,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

// grpc server of Health Check
pub struct HealthCheckServer {
    storager: Arc<Storager>,
}

impl HealthCheckServer {
    pub fn new(storager: Arc<Storager>) -> Self {
        HealthCheckServer { storager }
    }
}

#[tonic::async_trait]
impl Health for HealthCheckServer {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        info!("healthcheck entry!");

        let real_key = get_real_key(Regions::Global, &u64::MAX.to_be_bytes());
        let store_ret = self
            .storager
            .store(&real_key, &u64::MAX.to_be_bytes())
            .await;
        let load_ret = self.storager.load(&real_key, false).await;

        let status = if store_ret.is_ok()
            && load_ret.is_ok()
            && load_ret.unwrap() == u64::MAX.to_be_bytes().to_vec()
        {
            ServingStatus::Serving.into()
        } else {
            ServingStatus::NotServing.into()
        };

        let reply = Response::new(HealthCheckResponse { status });
        Ok(reply)
    }
}
