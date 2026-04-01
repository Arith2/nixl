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
#include "object/s3/utils.h"
#include "object/s3/aws_sdk_init.h"
#include "common/nixl_log.h"
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
                             std::optional<std::string> rdma_token) {
    if (rdma_token.has_value()) {
        // RDMA path: Content-Length=0 (no TCP body) + RDMA token header.
        // RGW reads the real data length from the token and updates s->content_length,
        // so the size check in execute() passes.  Content-Length=0 tells beast the
        // HTTP body is immediately done, preventing the body-drain loop that would
        // otherwise block ~150ms waiting for data that never arrives over TCP.
        Aws::S3::Model::PutObjectRequest request;
        request.WithBucket(bucketName_).WithKey(Aws::String(key));
        auto empty_stream = Aws::MakeShared<Aws::StringStream>("RdmaEmpty", "");
        request.SetBody(empty_stream);
        request.SetContentLength(0);
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-token", rdma_token.value());

        s3Client_->PutObjectAsync(
            request,
            [callback](const Aws::S3::S3Client*,
                       const Aws::S3::Model::PutObjectRequest&,
                       const Aws::S3::Model::PutObjectOutcome& outcome,
                       const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                if (!outcome.IsSuccess())
                    NIXL_ERROR << "putObjectAsync (RDMA) error: "
                               << outcome.GetError().GetMessage()
                               << " (HTTP " << static_cast<int>(
                                      outcome.GetError().GetResponseCode()) << ")";
                callback(outcome.IsSuccess());
            },
            nullptr);
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

    s3Client_->PutObjectAsync(
        request,
        [callback, preallocated_stream_buf, data_stream](
            const Aws::S3::S3Client *,
            const Aws::S3::Model::PutObjectRequest &,
            const Aws::S3::Model::PutObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            if (!outcome.IsSuccess())
                NIXL_ERROR << "putObjectAsync (TCP) error: "
                           << outcome.GetError().GetMessage()
                           << " (HTTP " << static_cast<int>(
                                  outcome.GetError().GetResponseCode()) << ")";
            callback(outcome.IsSuccess());
        },
        nullptr);
}

void
awsS3Client::getObjectAsync(std::string_view key,
                            uintptr_t data_ptr,
                            size_t data_len,
                            size_t offset,
                            get_object_callback_t callback,
                            std::optional<std::string> rdma_token) {
    if (rdma_token.has_value()) {
        // RDMA path: send token header so RGW can RDMA_WRITE data directly into
        // data_ptr.  Response body is empty (Content-Length: 0); the callback
        // fires after the HTTP 200 arrives, at which point data is already in place.
        Aws::S3::Model::GetObjectRequest request;
        request.WithBucket(bucketName_)
            .WithKey(Aws::String(key))
            .WithRange(absl::StrFormat("bytes=%d-%d", offset, offset + data_len - 1));
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-token", rdma_token.value());

        s3Client_->GetObjectAsync(
            request,
            [callback](const Aws::S3::S3Client*,
                       const Aws::S3::Model::GetObjectRequest&,
                       const Aws::S3::Model::GetObjectOutcome& outcome,
                       const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                callback(outcome.IsSuccess());
            },
            nullptr);
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

    s3Client_->GetObjectAsync(
        request,
        [callback, stream_factory](const Aws::S3::S3Client *,
                                   const Aws::S3::Model::GetObjectRequest &,
                                   const Aws::S3::Model::GetObjectOutcome &outcome,
                                   const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            callback(outcome.IsSuccess());
        },
        nullptr);
}

void
awsS3Client::getKVCacheAsync(const std::vector<std::string>& chunk_keys,
                              int num_layers,
                              size_t kv_per_token_per_layer,
                              size_t tokens_per_chunk,
                              int layer_aggregate,
                              uintptr_t data_ptr,
                              size_t data_len,
                              get_object_callback_t callback,
                              std::optional<std::string> rdma_token) {
    // Build JSON for x-amz-kvcache header
    std::string json = "{\"chunks\":[";
    for (size_t i = 0; i < chunk_keys.size(); i++) {
        if (i > 0) json += ",";
        json += "\"" + chunk_keys[i] + "\"";
    }
    json += "],\"num_layers\":" + std::to_string(num_layers)
         + ",\"kv_per_token_per_layer\":" + std::to_string(kv_per_token_per_layer)
         + ",\"tokens_per_chunk\":" + std::to_string(tokens_per_chunk)
         + ",\"layer_aggregate\":" + std::to_string(layer_aggregate) + "}";

    // Use first chunk key as the request object key (server uses x-amz-kvcache, not the key)
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(chunk_keys[0]));
    request.SetAdditionalCustomHeaderValue("x-amz-kvcache", json);
    if (rdma_token.has_value())
        request.SetAdditionalCustomHeaderValue("x-amz-rdma-token", rdma_token.value());

    NIXL_INFO << "getKVCacheAsync: chunks=" << chunk_keys.size()
              << " layers=" << num_layers << " json_len=" << json.size();

    s3Client_->GetObjectAsync(
        request,
        [callback](const Aws::S3::S3Client*,
                   const Aws::S3::Model::GetObjectRequest&,
                   const Aws::S3::Model::GetObjectOutcome& outcome,
                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            callback(outcome.IsSuccess());
        },
        nullptr);
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
