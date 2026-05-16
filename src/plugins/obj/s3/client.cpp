/*
 * SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client.h"
#include "obj_us_trace.h"
#include "object/s3/utils.h"
#include "object/s3/aws_sdk_init.h"
#include "common/nixl_log.h"
#include <cstdint>
#include <cstdio>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <absl/strings/str_format.h>

awsS3Client::awsS3Client(nixl_b_params_t *custom_params,
                         std::shared_ptr<Aws::Utils::Threading::Executor> executor) {
    // Initialize AWS SDK (thread-safe, only happens once)
    nixl_s3_utils::initAWSSDK();

    Aws::Client::ClientConfiguration config;
    nixl_s3_utils::configureClientCommon(config, custom_params);
    if (executor) config.executor = executor;

    auto credentials_opt = nixl_s3_utils::createAWSCredentials(custom_params);
    bool use_virtual_addressing = nixl_s3_utils::getUseVirtualAddressing(custom_params);
    bucketName_ = Aws::String(nixl_s3_utils::getBucketName(custom_params));

    if (credentials_opt.has_value())
        s3Client_ = std::make_unique<Aws::S3::S3Client>(
            credentials_opt.value(),
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
            use_virtual_addressing);
    else
        s3Client_ = std::make_unique<Aws::S3::S3Client>(
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
            use_virtual_addressing);
}

void
awsS3Client::setExecutor(std::shared_ptr<Aws::Utils::Threading::Executor> executor) {
    throw std::runtime_error("AwsS3Client::setExecutor() not supported - "
                             "AWS SDK doesn't allow changing executor after client creation");
}

void
awsS3Client::putObjectAsync(std::string_view key,
                             uintptr_t data_ptr,
                             size_t data_len,
                             size_t offset,
                             put_object_callback_t callback,
                             std::optional<std::string> rdma_token,
                             uint64_t req_id) {
    NIXL_OBJ_US_R("client_putObjectAsync_enter", req_id);
    if (rdma_token.has_value()) {
        // RDMA path: Content-Length=0 (no TCP body) + RDMA token header.
        // RGW reads the real data length from the token and updates s->content_length,
        // so the size check in execute() passes.  Content-Length=0 tells beast the
        // HTTP body is immediately done, preventing the body-drain loop that would
        // otherwise block ~150ms waiting for data that never arrives over TCP.
        NIXL_OBJ_US_R("client_build_request_start", req_id);
        Aws::S3::Model::PutObjectRequest request;
        request.WithBucket(bucketName_).WithKey(Aws::String(key));
        auto empty_stream = Aws::MakeShared<Aws::StringStream>("RdmaEmpty", "");
        request.SetBody(empty_stream);
        request.SetContentLength(0);
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-buffer", rdma_token.value());
        // Tag the request with our trace id so RGW + the wire pcap can join.
        char rid_buf[32];
        std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
        request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);
        NIXL_OBJ_US_R("client_build_request_done", req_id);

        NIXL_OBJ_US_R("client_sdk_call_s3rdma_buffer_put", req_id);
        s3Client_->PutObjectAsync(
            request,
            [callback, req_id](const Aws::S3::S3Client*,
                       const Aws::S3::Model::PutObjectRequest&,
                       const Aws::S3::Model::PutObjectOutcome& outcome,
                       const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_buffer_put", req_id);
                if (!outcome.IsSuccess())
                    NIXL_ERROR << "putObjectAsync (RDMA) error: "
                               << outcome.GetError().GetMessage()
                               << " (HTTP " << static_cast<int>(
                                      outcome.GetError().GetResponseCode()) << ")";
                callback(outcome.IsSuccess());
                NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_buffer_put", req_id);
            },
            nullptr);
        NIXL_OBJ_US_R("client_sdk_returned_s3rdma_buffer_put", req_id);
        return;
    }

    // TCP path: body carries data.
    if (offset != 0) {
        callback(false);
        return;
    }

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(key));

    auto preallocated_stream_buf = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
        "PutObjectStreamBuf", reinterpret_cast<unsigned char *>(data_ptr), data_len);
    auto data_stream =
        Aws::MakeShared<Aws::IOStream>("PutObjectInputStream", preallocated_stream_buf.get());
    request.SetBody(data_stream);

    NIXL_OBJ_US_R("client_sdk_call_s3tcp_put", req_id);
    s3Client_->PutObjectAsync(
        request,
        [callback, preallocated_stream_buf, data_stream, req_id](
            const Aws::S3::S3Client *,
            const Aws::S3::Model::PutObjectRequest &,
            const Aws::S3::Model::PutObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3tcp_put", req_id);
            if (!outcome.IsSuccess())
                NIXL_ERROR << "putObjectAsync (TCP) error: "
                           << outcome.GetError().GetMessage()
                           << " (HTTP " << static_cast<int>(
                                  outcome.GetError().GetResponseCode()) << ")";
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3tcp_put", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3tcp_put", req_id);
}

void
awsS3Client::getObjectAsync(std::string_view key,
                            uintptr_t data_ptr,
                            size_t data_len,
                            size_t offset,
                            get_object_callback_t callback,
                            std::optional<std::string> rdma_token,
                            uint64_t req_id) {
    NIXL_OBJ_US_R("client_getObjectAsync_enter", req_id);
    if (rdma_token.has_value()) {
        // RDMA path: send token header so RGW can RDMA_WRITE data directly into
        // data_ptr.  Response body is empty (Content-Length: 0); the callback
        // fires after the HTTP 200 arrives, at which point data is already in place.
        Aws::S3::Model::GetObjectRequest request;
        request.WithBucket(bucketName_)
            .WithKey(Aws::String(key))
            .WithRange(absl::StrFormat("bytes=%d-%d", offset, offset + data_len - 1));
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-buffer", rdma_token.value());
        char rid_buf[32];
        std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
        request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);

        NIXL_OBJ_US_R("client_sdk_call_s3rdma_buffer_get", req_id);
        s3Client_->GetObjectAsync(
            request,
            [callback, req_id](const Aws::S3::S3Client*,
                       const Aws::S3::Model::GetObjectRequest&,
                       const Aws::S3::Model::GetObjectOutcome& outcome,
                       const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_buffer_get", req_id);
                callback(outcome.IsSuccess());
                NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_buffer_get", req_id);
            },
            nullptr);
        NIXL_OBJ_US_R("client_sdk_returned_s3rdma_buffer_get", req_id);
        return;
    }

    auto preallocated_stream_buf = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
        "GetObjectStreamBuf", reinterpret_cast<unsigned char *>(data_ptr), data_len);
    auto stream_factory = Aws::MakeShared<Aws::IOStreamFactory>(
        "GetObjectStreamFactory", [preallocated_stream_buf]() -> Aws::IOStream * {
            return new Aws::IOStream(preallocated_stream_buf.get());
        });

    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucketName_)
        .WithKey(Aws::String(key))
        .WithRange(absl::StrFormat("bytes=%d-%d", offset, offset + data_len - 1));
    request.SetResponseStreamFactory(*stream_factory.get());

    NIXL_OBJ_US_R("client_sdk_call_s3tcp_get", req_id);
    s3Client_->GetObjectAsync(
        request,
        [callback, stream_factory, req_id](const Aws::S3::S3Client *,
                                   const Aws::S3::Model::GetObjectRequest &,
                                   const Aws::S3::Model::GetObjectOutcome &outcome,
                                   const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3tcp_get", req_id);
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3tcp_get", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3tcp_get", req_id);
}

void
awsS3Client::getBatchAsync(const std::vector<std::string>& chunk_keys,
                            size_t object_size,
                            int server_aggregate_size,
                            uintptr_t data_ptr,
                            size_t data_len,
                            get_object_callback_t callback,
                            std::optional<std::string> rdma_token,
                            uint64_t req_id) {
    (void)data_ptr;
    (void)data_len;
    NIXL_OBJ_US_R("client_getBatchAsync_enter", req_id);

    // Build JSON for x-amz-rdma-batch header: chunk keys, uniform per-object
    // size, and the server-side aggregation granularity (0 = bulk push of
    // the whole batch; N = one RDMA push per N objects).
    std::string json = "{\"chunks\":[";
    for (size_t i = 0; i < chunk_keys.size(); i++) {
        if (i > 0) json += ",";
        json += "\"" + chunk_keys[i] + "\"";
    }
    json += "],\"object_size\":" + std::to_string(object_size)
         +  ",\"server_aggregate_size\":" + std::to_string(server_aggregate_size) + "}";

    // Use first chunk key as the request object key (server uses x-amz-rdma-batch, not the key)
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(chunk_keys[0]));
    request.SetAdditionalCustomHeaderValue("x-amz-rdma-batch", json);
    if (rdma_token.has_value())
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-buffer", rdma_token.value());
    char rid_buf[32];
    std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
    request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);

    NIXL_INFO << "getBatchAsync: chunks=" << chunk_keys.size()
              << " object_size=" << object_size
              << " server_aggregate=" << server_aggregate_size
              << " json_len=" << json.size();

    NIXL_OBJ_US_R("client_sdk_call_s3rdma_batch_default", req_id);
    s3Client_->GetObjectAsync(
        request,
        [callback, req_id](const Aws::S3::S3Client*,
                   const Aws::S3::Model::GetObjectRequest&,
                   const Aws::S3::Model::GetObjectOutcome& outcome,
                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_batch_default", req_id);
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_batch_default", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3rdma_batch_default", req_id);
}

// ─── cuObject-style split-plane control hop ──────────────────────────────
//
// Sends a PUT with Content-Length: 0 + x-amz-rdma-direct: 1 header. RGW
// runs its full PUT pipeline against DAOS (creates the object record via
// the DAOS SAL plugin's ds3_obj_open path) but never sees a body. Once
// 200 OK fires, the split-plane engine dispatches the actual data write
// via libdfs to the DAOS server, bypassing the HTTP frontend entirely.
void
awsS3Client::putObjectRdmaDirectControlAsync(std::string_view key,
                                              put_object_callback_t callback,
                                              uint64_t req_id) {
    NIXL_OBJ_US_R("client_putRdmaDirectControl_enter", req_id);
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(key));
    auto empty_stream = Aws::MakeShared<Aws::StringStream>("RdmaDirectEmpty", "");
    request.SetBody(empty_stream);
    request.SetContentLength(0);
    request.SetAdditionalCustomHeaderValue("x-amz-rdma-direct", "1");
    char rid_buf[32];
    std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
    request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);

    NIXL_OBJ_US_R("client_sdk_call_s3rdma_direct_put", req_id);
    s3Client_->PutObjectAsync(
        request,
        [callback, req_id](const Aws::S3::S3Client*,
                           const Aws::S3::Model::PutObjectRequest&,
                           const Aws::S3::Model::PutObjectOutcome& outcome,
                           const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_direct_put", req_id);
            if (!outcome.IsSuccess())
                NIXL_ERROR << "putObjectRdmaDirectControlAsync error: "
                           << outcome.GetError().GetMessage()
                           << " (HTTP " << static_cast<int>(
                                  outcome.GetError().GetResponseCode()) << ")";
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_direct_put", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3rdma_direct_put", req_id);
}

// HEAD request with x-amz-rdma-direct: 1 header. Used as the GET-side
// control hop — RGW does the bucket lookup + dfs_get_size (via the
// existing dirent bypass) and returns 200 with the object metadata. The
// split-plane engine then issues the libdfs read directly against DAOS.
void
awsS3Client::headObjectRdmaDirectControlAsync(std::string_view key,
                                               get_object_callback_t callback,
                                               uint64_t req_id) {
    NIXL_OBJ_US_R("client_headRdmaDirectControl_enter", req_id);
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(key));
    request.SetAdditionalCustomHeaderValue("x-amz-rdma-direct", "1");
    char rid_buf[32];
    std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
    request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);

    NIXL_OBJ_US_R("client_sdk_call_s3rdma_direct_get", req_id);
    s3Client_->HeadObjectAsync(
        request,
        [callback, req_id](const Aws::S3::S3Client*,
                           const Aws::S3::Model::HeadObjectRequest&,
                           const Aws::S3::Model::HeadObjectOutcome& outcome,
                           const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_direct_get", req_id);
            if (!outcome.IsSuccess())
                NIXL_ERROR << "headObjectRdmaDirectControlAsync error: "
                           << outcome.GetError().GetMessage()
                           << " (HTTP " << static_cast<int>(
                                  outcome.GetError().GetResponseCode()) << ")";
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_direct_get", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3rdma_direct_get", req_id);
}

// Per-batch control hop. One PUT against the first chunk's key with the
// x-amz-rdma-batch: 1 marker header and the batch descriptor JSON in the
// request BODY (not the header — that hits beast's max_request_field_size
// at ~1024 keys). RGW's PUT handler detects the marker, parses JSON from
// the body, validates (auth + bucket lookup) and returns 200 OK without
// writing any object. NIXL then fans out N parallel dfs_reads to DAOS.
void
awsS3Client::headBatchControlAsync(const std::vector<std::string>& chunk_keys,
                                    size_t object_size,
                                    int server_aggregate_size,
                                    get_object_callback_t callback,
                                    uint64_t req_id) {
    NIXL_OBJ_US_R("client_headBatchControl_enter", req_id);

    // Build the batch descriptor JSON — sent in the request BODY.
    std::string json = "{\"chunks\":[";
    for (size_t i = 0; i < chunk_keys.size(); i++) {
        if (i > 0) json += ",";
        json += "\"" + chunk_keys[i] + "\"";
    }
    json += "],\"object_size\":" + std::to_string(object_size)
         +  ",\"server_aggregate_size\":" + std::to_string(server_aggregate_size) + "}";

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(chunk_keys[0]));

    // JSON descriptor goes in the body. AWS V4 signature includes SHA256(body),
    // so the batch contents are integrity-protected automatically.
    auto body_stream = Aws::MakeShared<Aws::StringStream>("RdmaBatchBody");
    *body_stream << json;
    request.SetBody(body_stream);
    request.SetContentLength(static_cast<long long>(json.size()));
    request.SetContentType("application/json");

    // Tiny marker header — tells RGW "this PUT body is a batch descriptor,
    // not object data; parse JSON from body and short-circuit to 200".
    request.SetAdditionalCustomHeaderValue("x-amz-rdma-batch", "1");
    request.SetAdditionalCustomHeaderValue("x-amz-rdma-direct", "1");
    char rid_buf[32];
    std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
    request.SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);

    NIXL_OBJ_US_R("client_sdk_call_s3rdma_batch_split", req_id);
    s3Client_->PutObjectAsync(
        request,
        [callback, req_id, body_stream](const Aws::S3::S3Client*,
                           const Aws::S3::Model::PutObjectRequest&,
                           const Aws::S3::Model::PutObjectOutcome& outcome,
                           const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            NIXL_OBJ_US_R("client_sdk_callback_fired_s3rdma_batch_split", req_id);
            if (!outcome.IsSuccess())
                NIXL_ERROR << "headBatchControlAsync (PUT-body) error: "
                           << outcome.GetError().GetMessage()
                           << " (HTTP " << static_cast<int>(
                                  outcome.GetError().GetResponseCode()) << ")";
            callback(outcome.IsSuccess());
            NIXL_OBJ_US_R("client_sdk_callback_done_s3rdma_batch_split", req_id);
        },
        nullptr);
    NIXL_OBJ_US_R("client_sdk_returned_s3rdma_batch_split", req_id);
}

bool
awsS3Client::checkObjectExists(std::string_view key) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(key));

    auto outcome = s3Client_->HeadObject(request);
    if (outcome.IsSuccess())
        return true;
    else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        return false;
    else
        throw std::runtime_error("Failed to check if object exists: " +
                                 outcome.GetError().GetMessage());
}
