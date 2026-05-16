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

#ifndef OBJ_PLUGIN_S3_CLIENT_H
#define OBJ_PLUGIN_S3_CLIENT_H

#include <memory>
#include <optional>
#include <string_view>
#include <cstdint>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/Aws.h>
#include "obj_backend.h"
#include "nixl_types.h"

/**
 * S3 Vanilla Object Client - Base implementation using AWS SDK S3Client.
 * This is the standard S3 client implementation that other S3-based clients can inherit from.
 */
class awsS3Client : public iS3Client {
public:
    /**
     * Constructor that creates an AWS S3Client from custom parameters.
     * @param custom_params Custom parameters containing S3 configuration
     * @param executor Optional executor for async operations
     */
    awsS3Client(nixl_b_params_t *custom_params,
                std::shared_ptr<Aws::Utils::Threading::Executor> executor = nullptr);

    virtual ~awsS3Client() = default;

    void
    setExecutor(std::shared_ptr<Aws::Utils::Threading::Executor> executor) override;

    void
    putObjectAsync(std::string_view key,
                   uintptr_t data_ptr,
                   size_t data_len,
                   size_t offset,
                   put_object_callback_t callback,
                   std::optional<std::string> rdma_token = std::nullopt,
                   uint64_t req_id = 0) override;

    void
    getObjectAsync(std::string_view key,
                   uintptr_t data_ptr,
                   size_t data_len,
                   size_t offset,
                   get_object_callback_t callback,
                   std::optional<std::string> rdma_token = std::nullopt,
                   uint64_t req_id = 0) override;

    void
    getBatchAsync(const std::vector<std::string>& chunk_keys,
                  size_t object_size,
                  int server_aggregate_size,
                  uintptr_t data_ptr,
                  size_t data_len,
                  get_object_callback_t callback,
                  std::optional<std::string> rdma_token = std::nullopt,
                  uint64_t req_id = 0) override;

    bool
    checkObjectExists(std::string_view key) override;

    // ── cuObject-style split-plane control hop ──────────────────────────────
    //
    // These methods are NOT part of the iS3Client interface — they are only
    // used by the s3_split_plane sub-engine. They send an empty-body HTTP
    // request to RGW with the x-amz-rdma-direct header so that:
    //
    //   1. RGW does its full bucket / dirent / DAOS metadata work (the
    //      "control plane completes against DAOS through Ceph") and
    //   2. NO bytes flow through the HTTP frontend.
    //
    // The split-plane engine uses the success callback as the "ready"
    // signal: only after the HTTP control returns 200 does it dispatch the
    // libdfs data plane to talk to the DAOS server directly.
    //
    // putObjectRdmaDirectControlAsync — empty-body PUT (Content-Length: 0)
    //                                   to (re)create the object record on
    //                                   the Ceph side. RGW handles this via
    //                                   its existing DAOS SAL ds3_obj_open
    //                                   path; the dirent bypass already
    //                                   skips the xattr write.
    //
    // headObjectRdmaDirectControlAsync — HEAD request that drives RGW's
    //                                    object lookup (dfs_get_size via
    //                                    the dirent bypass). Used as the
    //                                    GET-side control hop.
    void
    putObjectRdmaDirectControlAsync(std::string_view key,
                                    put_object_callback_t callback,
                                    uint64_t req_id = 0);

    void
    headObjectRdmaDirectControlAsync(std::string_view key,
                                     get_object_callback_t callback,
                                     uint64_t req_id = 0);

    // Per-batch control hop. One HEAD with x-amz-rdma-batch JSON header
    // listing the N chunk keys + uniform object_size + server_aggregate_size.
    // Returns 200 OK once RGW has authenticated and validated the batch.
    // Used by split-plane batch READ: after the callback fires, the engine
    // dispatches N libdfs reads directly to the DAOS server.
    void
    headBatchControlAsync(const std::vector<std::string>& chunk_keys,
                          size_t object_size,
                          int server_aggregate_size,
                          get_object_callback_t callback,
                          uint64_t req_id = 0);

protected:
    std::unique_ptr<Aws::S3::S3Client> s3Client_;
    Aws::String bucketName_;
};

#endif // OBJ_PLUGIN_S3_CLIENT_H
