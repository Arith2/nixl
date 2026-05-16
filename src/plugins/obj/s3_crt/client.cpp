/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "client.h"
#include "object/s3/utils.h"
#include "object/s3/aws_sdk_init.h"
#include "engine_utils.h"
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/crt/io/EventLoopGroup.h>
#include <aws/crt/io/HostResolver.h>
#include <aws/crt/io/Bootstrap.h>
#include <absl/strings/str_format.h>
#include <optional>
#include <iostream>
#include <cstdio>
#include "common/nixl_log.h"
#include "s3/obj_us_trace.h"

awsS3CrtClient::awsS3CrtClient(nixl_b_params_t *custom_params,
                               std::shared_ptr<Aws::Utils::Threading::Executor> executor)
    : awsS3Client(custom_params, executor) {
    // Initialize AWS SDK (thread-safe, only happens once)
    nixl_s3_utils::initAWSSDK();

    // Create S3 CRT client configuration
    Aws::S3Crt::ClientConfiguration config;
    nixl_s3_utils::configureClientCommon(config, custom_params);
    if (executor) config.executor = executor;

    // Override the default single-threaded CRT event loop group with a sized one,
    // so HTTP-response callbacks can be drained in parallel instead of serializing
    // through one thread (which becomes the dominant tail under high in-flight depth).
    static const uint16_t crt_event_loop_threads = []() {
        const char *e = std::getenv("NIXL_CRT_EVENT_LOOP_THREADS");
        return e ? static_cast<uint16_t>(std::atoi(e)) : uint16_t{8};
    }();
    static auto event_loop_group =
        std::make_shared<Aws::Crt::Io::EventLoopGroup>(crt_event_loop_threads);
    static auto host_resolver =
        std::make_shared<Aws::Crt::Io::DefaultHostResolver>(*event_loop_group, 8, 30);
    static auto client_bootstrap =
        std::make_shared<Aws::Crt::Io::ClientBootstrap>(*event_loop_group, *host_resolver);
    config.clientBootstrap = client_bootstrap;
    NIXL_INFO << "S3 CRT client using " << crt_event_loop_threads
              << " event-loop threads";

    // Align the CRT multipart thresholds with crtMinLimit so that every object
    // routed to this client (size >= crtMinLimit) is uploaded via multipart.
    // If crtMinLimit < 5 MiB the CRT SDK clamps partSize to 5 MiB internally
    // (with a warning log) while keeping multipartUploadThreshold at the user
    // value, so MPU still activates at crtMinLimit — but the effective part
    // size will be 5 MiB regardless.
    const size_t crt_min_limit = getCrtMinLimit(custom_params);
    if (crt_min_limit > 0) {
        config.partSize = crt_min_limit;
        config.multipartUploadThreshold = crt_min_limit;
    }

    auto credentials_opt = nixl_s3_utils::createAWSCredentials(custom_params);
    bool use_virtual_addressing = nixl_s3_utils::getUseVirtualAddressing(custom_params);
    config.useVirtualAddressing = use_virtual_addressing;

    if (credentials_opt.has_value())
        s3CrtClient_ = std::make_unique<Aws::S3Crt::S3CrtClient>(
            credentials_opt.value(),
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
            use_virtual_addressing);
    else
        s3CrtClient_ = std::make_unique<Aws::S3Crt::S3CrtClient>(
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
            use_virtual_addressing);
}

void
awsS3CrtClient::setExecutor(std::shared_ptr<Aws::Utils::Threading::Executor> executor) {
    throw std::runtime_error("awsS3CrtClient::setExecutor() not supported - "
                             "AWS SDK doesn't allow changing executor after client creation");
}

void
awsS3CrtClient::putObjectAsync(std::string_view key,
                               uintptr_t data_ptr,
                               size_t data_len,
                               size_t offset,
                               put_object_callback_t callback,
                               std::optional<std::string> rdma_token,
                               uint64_t req_id) {
    // RDMA path: empty body + x-amz-rdma-token header. Mirrors the vanilla
    // awsS3Client::putObjectAsync RDMA branch but uses the CRT client's
    // event-driven I/O model instead of libcurl's blocking thread-per-request.
    if (rdma_token.has_value()) {
        NIXL_OBJ_US_R("client_putObjectAsync_enter", req_id);
        NIXL_OBJ_US_R("client_build_request_start", req_id);
        auto request = Aws::MakeShared<Aws::S3Crt::Model::PutObjectRequest>("PutObjectRequest");
        request->WithBucket(bucketName_).WithKey(Aws::String(key));
        auto empty_stream = Aws::MakeShared<Aws::StringStream>("RdmaEmpty", "");
        request->SetBody(empty_stream);
        request->SetContentLength(0);
        request->SetAdditionalCustomHeaderValue("x-amz-rdma-token", rdma_token.value());
        char rid_buf[32];
        std::snprintf(rid_buf, sizeof(rid_buf), "%lu", (unsigned long)req_id);
        request->SetAdditionalCustomHeaderValue("x-nixl-req-id", rid_buf);
        NIXL_OBJ_US_R("client_build_request_done", req_id);

        NIXL_OBJ_US_R("client_sdk_PutObjectAsync_call", req_id);
        s3CrtClient_->PutObjectAsync(
            *request,
            [callback, request, empty_stream, req_id](
                const Aws::S3Crt::S3CrtClient *,
                const Aws::S3Crt::Model::PutObjectRequest &,
                const Aws::S3Crt::Model::PutObjectOutcome &outcome,
                const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
                NIXL_OBJ_US_R("client_sdk_callback_fired", req_id);
                if (!outcome.IsSuccess()) {
                    NIXL_ERROR << "putObjectAsync (CRT-RDMA) error: "
                               << outcome.GetError().GetMessage()
                               << " (HTTP " << static_cast<int>(
                                      outcome.GetError().GetResponseCode()) << ")";
                }
                callback(outcome.IsSuccess());
                NIXL_OBJ_US_R("client_sdk_callback_done", req_id);
            },
            nullptr);
        NIXL_OBJ_US_R("client_sdk_PutObjectAsync_returned", req_id);
        return;
    }

    if (offset != 0) {
        callback(false);
        return;
    }

    // Heap-allocate the request so it outlives this function: the CRT SDK stores
    // a raw pointer to it (userData->originalRequest) and dereferences it in
    // S3CrtRequestHeadersCallback after putObjectAsync() has returned.
    auto request = Aws::MakeShared<Aws::S3Crt::Model::PutObjectRequest>("PutObjectRequest");
    request->WithBucket(bucketName_).WithKey(Aws::String(key));

    auto preallocated_stream_buf = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
        "PutObjectStreamBuf", reinterpret_cast<unsigned char *>(data_ptr), data_len);
    auto data_stream =
        Aws::MakeShared<Aws::IOStream>("PutObjectInputStream", preallocated_stream_buf.get());
    request->SetBody(data_stream);

    s3CrtClient_->PutObjectAsync(
        *request,
        [callback, preallocated_stream_buf, data_stream, request](
            const Aws::S3Crt::S3CrtClient *,
            const Aws::S3Crt::Model::PutObjectRequest &,
            const Aws::S3Crt::Model::PutObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            if (!outcome.IsSuccess()) {
                NIXL_ERROR << absl::StrFormat("putObjectAsync (CRT) error: %s",
                                              outcome.GetError().GetMessage());
            }
            callback(outcome.IsSuccess());
        },
        nullptr);
}

void
awsS3CrtClient::getObjectAsync(std::string_view key,
                               uintptr_t data_ptr,
                               size_t data_len,
                               size_t offset,
                               get_object_callback_t callback,
                               std::optional<std::string> /*rdma_token*/,
                               uint64_t /*req_id*/) {
    auto preallocated_stream_buf = Aws::MakeShared<Aws::Utils::Stream::PreallocatedStreamBuf>(
        "GetObjectStreamBuf", reinterpret_cast<unsigned char *>(data_ptr), data_len);
    auto stream_factory = Aws::MakeShared<Aws::IOStreamFactory>(
        "GetObjectStreamFactory", [preallocated_stream_buf]() -> Aws::IOStream * {
            return new Aws::IOStream(preallocated_stream_buf.get());
        });

    // Heap-allocate the request for the same reason as putObjectAsync: the SDK
    // stores a raw pointer to it (userData->originalRequest) used in callbacks
    // that fire after getObjectAsync() has returned.
    auto request = Aws::MakeShared<Aws::S3Crt::Model::GetObjectRequest>("GetObjectRequest");
    request->WithBucket(bucketName_)
        .WithKey(Aws::String(key))
        .WithRange(absl::StrFormat("bytes=%d-%d", offset, offset + data_len - 1));
    request->SetResponseStreamFactory(*stream_factory.get());

    s3CrtClient_->GetObjectAsync(
        *request,
        [callback, stream_factory, request](
            const Aws::S3Crt::S3CrtClient *,
            const Aws::S3Crt::Model::GetObjectRequest &,
            const Aws::S3Crt::Model::GetObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &) {
            if (!outcome.IsSuccess())
                NIXL_ERROR << "getObjectAsync (CRT) error: " << outcome.GetError().GetMessage();
            callback(outcome.IsSuccess());
        },
        nullptr);
}

bool
awsS3CrtClient::checkObjectExists(std::string_view key) {
    Aws::S3Crt::Model::HeadObjectRequest request;
    request.WithBucket(bucketName_).WithKey(Aws::String(key));

    auto outcome = s3CrtClient_->HeadObject(request);
    if (outcome.IsSuccess())
        return true;
    else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        return false;
    else
        throw std::runtime_error("Failed to check if object exists (CRT): " +
                                 outcome.GetError().GetMessage());
}
