/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * S3 split-plane engine — cuObject-style control / data plane separation.
 *
 *   Control plane: NIXL → Ceph RGW (HTTP) → DAOS (RGW does its full
 *                  bucket / dirent / DAOS metadata work with the
 *                  x-amz-rdma-direct header; no bytes flow over HTTP).
 *
 *   Data plane:    NIXL → DAOS server (libdfs over UCX dc_x). Issued
 *                  ONLY after the control plane returns 200 OK.
 *
 * Mirrors the cuObject pattern: the S3 frontend is consulted for control,
 * the data path goes directly to the storage backend, and the S3 frontend
 * never touches the bytes. The control→data ordering is enforced by
 * callback chaining — the libdfs op is dispatched from the HTTP success
 * callback, never before.
 */

#ifndef OBJ_PLUGIN_S3_SPLIT_PLANE_ENGINE_IMPL_H
#define OBJ_PLUGIN_S3_SPLIT_PLANE_ENGINE_IMPL_H

#include "obj_backend.h"

// AWS SDK and DAOS both define DO_PRAGMA; undefine before including DAOS headers
#undef DO_PRAGMA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <daos.h>
#include <daos_fs.h>
#pragma GCC diagnostic pop

#include <atomic>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class awsS3Client;
namespace Aws { namespace Utils { namespace Threading {
    class Executor;
    class PooledThreadExecutor;
} } }

class S3SplitPlaneObjEngineImpl : public nixlObjEngineImpl {
public:
    explicit S3SplitPlaneObjEngineImpl(const nixlBackendInitParams *init_params);
    ~S3SplitPlaneObjEngineImpl() override;

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
    getXferDoneIndices(nixlBackendReqH *handle,
                       std::vector<int> &done_indices) const override;
    nixl_status_t
    releaseReqH(nixlBackendReqH *handle) const override;

private:
    // ── data plane state (cloned from daos_direct) ───────────────────────
    std::unordered_map<uint64_t, std::string> devIdToObjKey_;

    daos_handle_t poh_ = DAOS_HDL_INVAL;
    daos_handle_t coh_ = DAOS_HDL_INVAL;
    dfs_t        *dfs_ = nullptr;

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

    // ── control plane state ─────────────────────────────────────────────
    // Dedicated S3 client for HTTP control hops to RGW. We instantiate it
    // with its own AWS SDK default executor so its async callbacks fire
    // on a thread pool independent of `executor_`, avoiding any deadlock
    // when chaining control → data on the same engine.
    std::shared_ptr<awsS3Client> s3_client_;

    // Thread pool used to (a) dispatch the libdfs op from inside the HTTP
    // success callback (so we don't run libdfs on an AWS SDK callback
    // thread) and (b) own per-request promises.
    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> executor_;

    // ── batch mode state ────────────────────────────────────────────────
    // Per postXfer in batch mode:
    //   1. Pull batch_size_ keys via sliding window over devIdToObjKey_
    //   2. One HTTP control hop (headBatchControlAsync) — auth + bucket lookup
    //   3. After 200 OK, fan out N dfs_read tasks to executor_ (executor pool
    //      provides multi-thread scheduling)
    // server_aggregate_size_ is forwarded to the control hop as JSON metadata
    // (RGW may use it for logging / future server-side aggregation; the
    // split-plane data path always uses client-side libdfs fan-out).
    bool batch_mode_{false};
    bool lmcache_batch_mode_{false};
    int batch_size_{16};
    int server_aggregate_size_{0};
    int num_threads_batch_{16};
    int iodepth_batch_{0};
    mutable size_t batch_window_offset_{0};

    // Per-batch in-flight cap for the daos_obj_fetch fan-out inside the
    // HTTP callback. Without this cap, the callback bursts ALL batch_size_
    // fetches back-to-back (e.g. 2048 at T=64), overwhelming the engine.
    // With cap = NT × IOD (e.g. 4 × 8 = 32), the dispatcher matches the
    // sliding-window concurrency model of daos_direct hashoid: 1 HTTP per
    // logical load + N parallel chunk fetches throttled to NT × IOD.
    // Configured via customParam "batch_inflight_cap" (default 32).
    int batch_inflight_cap_{32};

    // Captured at registerMem time so the batch READ fan-out can address
    // the full DRAM/VRAM region (each task writes into buf_base + i*S).
    // The local descriptor in postXfer only carries len = one object_size,
    // so we cannot rely on it for the multi-object batch layout.
    uintptr_t batch_buf_addr_{0};
    size_t    batch_buf_len_{0};

    // Hashoid mode: skip dfs_open, synthesize OID client-side via
    // hashoid.h (shared with DAOS agg_sidecar). Covers both
    // s3rdma_direct (per-descriptor) and s3rdma_batch (fan-out).
    // Control-plane HTTP hop + RDMA path are preserved — only the
    // dentry lookup in the data plane is replaced by hash-as-OID.
    bool hashoid_mode_{false};
    int  hashoid_T_{1};
    int  hashoid_IOD_{1};

    // Aggregate mode (s3rdma_agg) — fork of s3rdma_batch's dispatch.
    // When set, each batch of agg_chunks_per_layer_ chunks is dispatched
    // as ONE daos_obj_fetch on the layer's aggregate OID; the server
    // synthesizes chunk OIDs as agg_oid.lo + i (sequential within layer)
    // and stitches the result into a single bulk transfer. Recipe is
    // passed via daos_set_nixl_agg_recipe(). The first key's trailing
    // integer suffix decides the layer: layer_idx = suffix / chunks_per_layer.
    bool daos_agg_mode_{false};
    // Agg-patch mode keeps s3rdma_batch's one-HTTP + sliding-window DAOS
    // scheduler, but calls daos_obj_fetch_agg for each logical object.
    bool daos_agg_patch_mode_{false};
    bool daos_agg_patch_lwagg_manifest_{false};
    bool daos_agg_patch_rangeget_{false};
    int  agg_chunks_per_layer_{256};
    // Total chunks in a layer (= ISL/T). Used to compute layer_idx from a
    // key's trailing integer index (= prepop_idx / chunks_per_layer_).
    // Distinct from agg_chunks_per_layer_ which is the agg fetch unit:
    // when agg < whole-layer, multiple agg fetches share the same layer.
    int  chunks_per_layer_{0};

    // Temporary lw-Agg client-emulation mode for LMCache integration dry runs.
    // It preserves the NIXL->RGW control plane, then reconstructs full
    // chunkwise LMCache objects by range-fetching slices from a precomputed
    // hashoid manifest. This validates LMCache key-list -> NIXL routing before
    // moving the same mapping into the DAOS server-side agg handler.
    bool daos_lwagg_client_emulate_{false};
    bool daos_lwagg_server_mode_{false};
    std::string lwagg_issue_mode_{"lanes"};
    std::string lwagg_manifest_tsv_;
    int lwagg_num_layers_{32};
    size_t lwagg_layer_bytes_{0};
    size_t lwagg_object_bytes_{0};
    size_t lwagg_manifest_start_{0};
    struct LwAggManifestEntry {
        uint64_t oid_lo{0};
        uint64_t oid_hi_user{0};
        std::string object_key;
    };
    std::vector<LwAggManifestEntry> lwagg_manifest_;
    std::unordered_map<std::string, size_t> lwagg_manifest_by_key_;
    mutable std::shared_ptr<std::vector<char>> lwagg_stage_buf_;
    size_t lwagg_stage_buf_bytes_{0};
    bool lwagg_stage_buf_mlocked_{false};

    // s3rdma_batch hashoid OID override: when set, a TSV manifest mapping
    // LMCache object_key → (oid_lo, oid_hi_user) overrides the default
    // splitmix(fnv1a(key)) derivation in the s3rdma_batch hashoid path. Used
    // to enforce round-robin target placement for fair comparison with the
    // lw-Agg path. Same TSV format as the lwagg manifest (chunk_idx, key,
    // oid_lo, oid_hi_user, object_key) but consumed only by s3rdma_batch
    // PUT/GET. Empty path disables the override.
    std::string s3rdma_batch_oid_manifest_tsv_;
    std::unordered_map<std::string,
        std::pair<uint64_t, uint64_t>> s3rdma_batch_oid_override_by_key_;

    // New batch-mode pipeline (replaces the wait-for-all barrier in hashoid
    // mode). Each postXfer represents ONE chunk; engine enqueues the op and,
    // when the queue reaches batch_size, dispatches 1 HTTP HEAD + batch_size
    // *independent* async daos_obj_fetch ops — each resolves its own promise.
    // Worker-level iodepth therefore gives a true per-thread sliding window:
    // with NT workers × IOD iodepth, NT × IOD chunks are in flight at all
    // times; completion of ANY one triggers the worker to dispatch the next.
    // HEADs are amortized across batch_size consecutive chunks.
    struct PendingBatchOp {
        std::string key;
        uintptr_t   data_ptr;
        size_t      data_len;
        size_t      offset;
        std::shared_ptr<std::promise<nixl_status_t>> promise;
        uint64_t    req_id;
    };
    mutable std::mutex            pending_reads_mtx_;
    mutable std::deque<PendingBatchOp> pending_reads_;
};

#endif // OBJ_PLUGIN_S3_SPLIT_PLANE_ENGINE_IMPL_H
