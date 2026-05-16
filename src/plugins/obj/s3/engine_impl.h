/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBJ_PLUGIN_S3_ENGINE_IMPL_H
#define OBJ_PLUGIN_S3_ENGINE_IMPL_H

#include <atomic>
#include <vector>

#include "obj_backend.h"
#include "rdma_ctx.h"

// Metadata for DRAM_SEG registrations — stores address for MR deregistration.
class nixlDramBEMD : public nixlBackendMD {
public:
    nixlDramBEMD(uintptr_t addr, size_t len)
        : nixlBackendMD(true), addr(addr), len(len) {}
    uintptr_t addr;
    size_t    len;
};

class DefaultObjEngineImpl : public nixlObjEngineImpl {
public:
    explicit DefaultObjEngineImpl(const nixlBackendInitParams *init_params);
    DefaultObjEngineImpl(const nixlBackendInitParams *init_params,
                         std::shared_ptr<iS3Client> s3_client,
                         std::shared_ptr<iS3Client> s3_client_crt);
    ~DefaultObjEngineImpl() override;

    nixl_mem_list_t
    getSupportedMems() const override {
        return {DRAM_SEG, OBJ_SEG};
    }

    nixl_status_t
    registerMem(const nixlBlobDesc &mem, const nixl_mem_t &nixl_mem, nixlBackendMD *&out) override;
    nixl_status_t
    deregisterMem(nixlBackendMD *meta) override;
    nixl_status_t
    queryMem(const nixl_reg_dlist_t &descs, std::vector<nixl_query_resp_t> &resp) const override;
    nixl_status_t
    prepXfer(const nixl_xfer_op_t &operation,
             const nixl_meta_dlist_t &local,
             const nixl_meta_dlist_t &remote,
             const std::string &remote_agent,
             const std::string &local_agent,
             nixlBackendReqH *&handle,
             const nixl_opt_b_args_t *opt_args) const override;
    nixl_status_t
    postXfer(const nixl_xfer_op_t &operation,
             const nixl_meta_dlist_t &local,
             const nixl_meta_dlist_t &remote,
             const std::string &remote_agent,
             nixlBackendReqH *&handle,
             const nixl_opt_b_args_t *opt_args) const override;
    nixl_status_t
    checkXfer(nixlBackendReqH *handle) const override;
    nixl_status_t
    releaseReqH(nixlBackendReqH *handle) const override;

protected:
    virtual iS3Client *
    getClient() const;
    virtual iS3Client *
    getClientForSize(size_t data_len) const;

    std::shared_ptr<asioThreadPoolExecutor> executor_;
    // Larger executor used by every client in s3ClientPool_ — sized to
    // kS3PoolSize * 4 worker threads to give the asio dispatcher enough
    // capacity to wake up all in-flight requests in parallel.
    std::shared_ptr<asioThreadPoolExecutor> fat_executor_;
    // Primary S3 client (kept for backwards compat / single-client tests).
    std::shared_ptr<iS3Client> s3Client_;
    // Pool of independent S3 clients used to bypass the single-CurlHttpClient
    // serialization bottleneck. When kS3PoolSize > 1, getClient() round-robins
    // across this pool so concurrent putObjectAsync calls land on independent
    // libcurl multi-handles and can transmit truly in parallel.
    std::vector<std::shared_ptr<iS3Client>> s3ClientPool_;
    mutable std::atomic<size_t> s3ClientPoolNext_{0};
    static constexpr size_t kS3PoolSize = 16;

    std::unordered_map<uint64_t, std::string> devIdToObjKey_;
    size_t crtMinLimit_;
    std::unique_ptr<RdmaContext> rdma_ctx_;

    // Batch mode (x-amz-rdma-batch)
    bool batch_mode_{false};
    int batch_size_{16};              // number of objects per batched request
    int server_aggregate_size_{0};    // objects per server-side RDMA push (0 = whole batch)
    mutable size_t batch_window_offset_{0};  // sliding window position into sorted key pool
    uintptr_t batch_buf_addr_{0};  // registered DRAM buffer base
    size_t batch_buf_len_{0};      // registered DRAM buffer size
};

#endif // OBJ_PLUGIN_S3_ENGINE_IMPL_H
