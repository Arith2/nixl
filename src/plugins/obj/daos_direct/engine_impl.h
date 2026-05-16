/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Direct DAOS DFS engine — bypasses Ceph RGW entirely on the data plane.
 * S3 key strings are used as DFS filenames in a POSIX container.
 * Does NOT initialize any S3/AWS SDK — only needs libdaos + libdfs.
 */

#ifndef OBJ_PLUGIN_DAOS_DIRECT_ENGINE_IMPL_H
#define OBJ_PLUGIN_DAOS_DIRECT_ENGINE_IMPL_H

#include "obj_backend.h"

// AWS SDK and DAOS both define DO_PRAGMA; undefine before including DAOS headers
#undef DO_PRAGMA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <daos.h>
#include <daos_fs.h>
#pragma GCC diagnostic pop

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <string>
#include <memory>

// Forward declare — we use a simple thread pool for async completion dispatch.
namespace Aws { namespace Utils { namespace Threading { class Executor; class PooledThreadExecutor; } } }

class DaosDirectObjEngineImpl : public nixlObjEngineImpl {
public:
    explicit DaosDirectObjEngineImpl(const nixlBackendInitParams *init_params);
    ~DaosDirectObjEngineImpl() override;

    nixl_mem_list_t
    getSupportedMems() const override {
        return {DRAM_SEG, OBJ_SEG, VRAM_SEG};
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

private:
    // Key mapping: devId → DFS filename (same as S3 key string)
    std::unordered_map<uint64_t, std::string> devIdToObjKey_;

    // Simple thread pool for async completion dispatch
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> executor_;

    // DAOS handles (opened once at init, closed at destruction)
    daos_handle_t poh_ = DAOS_HDL_INVAL;
    daos_handle_t coh_ = DAOS_HDL_INVAL;
    dfs_t        *dfs_ = nullptr;

    // Pool of daos_eq + progress threads
    static constexpr size_t kMaxEQ = 16;
    daos_handle_t    eq_pool_[kMaxEQ] = {};
    std::thread      eq_threads_[kMaxEQ];
    size_t           eq_pool_size_ = 0;
    std::atomic<bool> eq_run_{false};
    mutable std::atomic<uint64_t> eq_rr_{0};

    daos_handle_t getEQ() const;
    void startEQPool(size_t n);
    void stopEQPool();

    std::string pool_label_;
    std::string cont_label_;

    // Hashoid mode: when enabled, postXfer bypasses dfs_open() and instead
    // synthesizes the OID client-side via hashoid.h helpers (shared byte-for-byte
    // with DAOS agg_sidecar). NIXL postXfer + DAOS RPC path is preserved;
    // only the dentry lookup is skipped.
    bool hashoid_mode_ = false;
    int  hashoid_T_    = 1;   // from customParams; used as the T component in oid.hi
    int  hashoid_IOD_  = 1;   // from customParams; used as the IOD component in oid.hi
};

#endif // OBJ_PLUGIN_DAOS_DIRECT_ENGINE_IMPL_H
