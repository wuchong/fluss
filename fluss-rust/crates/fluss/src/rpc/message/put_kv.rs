/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::client::ReadyWriteBatch;
use crate::proto::{PbPutKvReqForBucket, PutKvResponse};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::ReadError;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{TableId, impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[allow(dead_code)]
pub struct PutKvRequest {
    pub(crate) inner_request: proto::PutKvRequest,
}

#[allow(dead_code)]
impl PutKvRequest {
    pub fn new(
        table_id: TableId,
        ack: i16,
        max_request_timeout_ms: i32,
        target_columns: Vec<i32>,
        ready_batches: &mut [ReadyWriteBatch],
    ) -> crate::error::Result<Self> {
        let mut request = proto::PutKvRequest {
            table_id,
            acks: ack as i32,
            timeout_ms: max_request_timeout_ms,
            target_columns,
            ..Default::default()
        };
        for ready_batch in ready_batches {
            request.buckets_req.push(PbPutKvReqForBucket {
                partition_id: ready_batch.table_bucket.partition_id(),
                bucket_id: ready_batch.table_bucket.bucket_id(),
                records: ready_batch.write_batch.build()?,
                original_partition_name: None,
                // 中文解释：当前 Rust 路径仍不发送路由桶数，保留旧请求形式；这没有实现扩缩容后的分区路由支持。
                // REVIEW [F002][P1]: Rust/Python 客户端在扩缩容后持续被拒绝。
                // REVIEW [F002][P1]: 本次新增字段在 Rust PutKvRequest、ProduceLog、Lookup、PrefixLookup
                // REVIEW [F002][P1]: 和扫描请求中仍固定为 None。
                // REVIEW [F002][P1]: 只要分区表完成一次 ALTER bucket.num，服务端对有 bucket key 的表便会拒绝未携带
                // REVIEW [F002][P1]: routing_bucket_count 的客户端请求，因此当前分支的 Rust 客户端及依赖它的 Python
                // REVIEW [F002][P1]: binding 即使重新连接，也无法继续读写该表。
                // REVIEW [F002][P1]: Rust 元数据和 assigner 仍使用表级桶数，因此也不能简单地将表级值填入新字段；需要消费每分区桶数，
                // REVIEW [F002][P1]: 并将实际用于路由的桶数随请求发送。
                // REVIEW [F002][P1]: 此项为静态调用链验证，未运行 Rust/Python 集成测试。
                routing_bucket_count: None,
            })
        }

        Ok(PutKvRequest {
            inner_request: request,
        })
    }
}

impl RequestBody for PutKvRequest {
    type ResponseBody = PutKvResponse;

    const API_KEY: ApiKey = ApiKey::PutKv;
}

impl_write_type!(PutKvRequest);
impl_read_type!(PutKvResponse);
