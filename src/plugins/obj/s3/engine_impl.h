/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBJ_PLUGIN_S3_ENGINE_IMPL_H
#define OBJ_PLUGIN_S3_ENGINE_IMPL_H

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
    std::shared_ptr<iS3Client> s3Client_;
    std::unordered_map<uint64_t, std::string> devIdToObjKey_;
    size_t crtMinLimit_;
    std::unique_ptr<RdmaContext> rdma_ctx_;

    // KV cache streaming mode
    bool kvcache_mode_{false};
    int kvcache_num_layers_{0};
    int kvcache_kv_per_token_{0};
    int kvcache_layer_aggregate_{0};  // 0=all layers at once
    size_t kvcache_tokens_per_chunk_{0};  // computed from block_size / (num_layers * kv_per_token)
    uintptr_t kvcache_buf_addr_{0};  // registered DRAM buffer base
    size_t kvcache_buf_len_{0};      // registered DRAM buffer size
};

#endif // OBJ_PLUGIN_S3_ENGINE_IMPL_H
