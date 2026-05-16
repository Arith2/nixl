/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * S3 split-plane engine. See engine_impl.h for the architecture.
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "engine_impl.h"
#pragma GCC diagnostic pop

#include "common/nixl_log.h"
#include "s3/client.h"
#include "s3/obj_us_trace.h"
#include "../daos_direct/hashoid.h"  // shared with DAOS agg_sidecar + daos_direct

#include <aws/core/utils/threading/Executor.h>
#include <daos_array.h>
#include <daos/daos_nixl_req_id.h>  // daos_set_nixl_req_id: tags obj_rw RPCs
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <sched.h>
#include <sstream>
#include <sys/mman.h>

// FNV-1a 32-bit string hash — same primitive as daos_direct hashoid path.
// Byte-level (no std::string), C-linkable.
static inline uint32_t split_hashoid_fnv1a_32(const char *s, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; ++i) { h ^= (unsigned char)s[i]; h *= 16777619u; }
    return h;
}

static const char kSplitHashoidDkey[] = "d";
static const char kSplitHashoidAkey[] = "a";
static constexpr uint32_t kLwAggRecipeFlag = 0x80000000u;

static bool
split_parse_bool(const std::string &value) {
    return value == "1" || value == "true" || value == "TRUE" ||
           value == "yes" || value == "YES";
}

// Async hashoid fetch context. Heap-allocated (not on stack) so the EQ
// progress thread can safely run the completion callback long after
// dispatch_one_async_fetch returns. Lifetime is owned by the completion
// callback (deletes itself).
// Completion for async hashoid fetch. Must NOT call daos_event_fini or
// daos_obj_close from the EQ progress thread itself — those reacquire the
// EQ-internal mutex and deadlock. Mirror daos_direct's pattern: the callback
// only signals a waiter; an executor thread does the cleanup + promise.
struct HashoidAsyncFetchWaiter {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done = false;
    int                     err  = 0;
};

struct HashoidAsyncFetchCtx {
    std::shared_ptr<std::promise<nixl_status_t>> promise;
    uint64_t       req_id{0};
    daos_handle_t  oh{DAOS_HDL_INVAL};
    daos_event_t   ev{};
    daos_key_t     dkey{};
    daos_recx_t    recx{};
    daos_iod_t     iod{};
    d_iov_t        iov{};
    d_sg_list_t    sgl{};
    HashoidAsyncFetchWaiter waiter;
};

static int
__attribute__((unused))
hashoid_async_fetch_completion_cb(void *arg, daos_event_t * /*ev*/, int ret) {
    auto *ctx = static_cast<HashoidAsyncFetchCtx*>(arg);
    { std::lock_guard<std::mutex> lock(ctx->waiter.mtx);
      ctx->waiter.done = true;
      ctx->waiter.err  = ret; }
    ctx->waiter.cv.notify_one();
    return 0;
}

// Aggregate-fetch context: one daos_obj_fetch covers N chunks via the
// server-side aggregate handler. The client side deliberately presents this
// as one contiguous aggregate buffer so the DAOS fetch shape matches
// s3rdma_batch as closely as possible.
struct AggCompletionQueue {
    std::mutex              mtx;
    std::condition_variable cv;
    std::deque<int>         completed_slots;
};

struct AggBatchCtx {
    std::vector<std::shared_ptr<std::promise<nixl_status_t>>> promises;
    daos_handle_t  oh{DAOS_HDL_INVAL};
    daos_event_t   ev{};
    daos_key_t     dkey{};
    daos_recx_t    recx{};
    daos_iod_t     iod{};
    std::vector<char> aggregate_buf;
    d_iov_t        iov{};
    std::vector<d_iov_t> iovs;
    d_sg_list_t    sgl{};
    HashoidAsyncFetchWaiter waiter;
    uint64_t       batch_id{0};
    int            slot_id{-1};
    AggCompletionQueue *completion_q{nullptr};
    bool           close_oh_on_cleanup{false};
};

__attribute__((unused))
static int
agg_batch_completion_cb(void *arg, daos_event_t * /*ev*/, int ret) {
    auto *ctx = static_cast<AggBatchCtx*>(arg);
    NIXL_OBJ_US_R("split_agg_ev_cb", ctx->batch_id);
    { std::lock_guard<std::mutex> lock(ctx->waiter.mtx);
      ctx->waiter.done = true;
      ctx->waiter.err  = ret; }
    ctx->waiter.cv.notify_one();
    if (ctx->completion_q != nullptr) {
        {
            std::lock_guard<std::mutex> lock(ctx->completion_q->mtx);
            ctx->completion_q->completed_slots.push_back(ctx->slot_id);
        }
        ctx->completion_q->cv.notify_one();
    }
    return 0;
}

// ── Minimal peek into dfs_obj_t to extract the DAOS object handle ──────────
// dfs_obj_t layout: { dfs_t*, daos_obj_id_t, daos_handle_t oh, ... }
// Stable across DAOS 2.x. Used by the async KVCache path.
namespace {
struct dfs_obj_peek { void *dfs; daos_obj_id_t oid; daos_handle_t oh; };
} // namespace
#include <memory>
#include <mutex>

// ────────────────────────────────────────────────────────────────────────────
// Metadata classes
// ────────────────────────────────────────────────────────────────────────────
class S3SplitObjMD : public nixlBackendMD {
public:
    S3SplitObjMD(nixl_mem_t type, uint64_t devId, std::string key)
        : nixlBackendMD(true), type(type), devId(devId), objKey(std::move(key)) {}
    nixl_mem_t type;
    uint64_t devId;
    std::string objKey;
};

class S3SplitDramMD : public nixlBackendMD {
public:
    S3SplitDramMD(uintptr_t addr, size_t len)
        : nixlBackendMD(true), addr(addr), len(len) {}
    uintptr_t addr;
    size_t len;
};

// ────────────────────────────────────────────────────────────────────────────
// Request handle — holds one future per descriptor in the postXfer batch.
// Each future fires after BOTH the HTTP control hop AND the libdfs op
// complete for that descriptor.
// ────────────────────────────────────────────────────────────────────────────
class S3SplitReqH : public nixlBackendReqH {
public:
    struct Slot {
        int index;
        std::future<nixl_status_t> future;
    };

    std::mutex slots_mtx_;
    std::vector<Slot> slots_;

    void addFuture(int index, std::future<nixl_status_t> future) {
        std::lock_guard<std::mutex> lk(slots_mtx_);
        slots_.push_back(Slot{index, std::move(future)});
    }

    nixl_status_t poll(std::vector<int> *done_indices) {
        std::lock_guard<std::mutex> lk(slots_mtx_);
        for (auto it = slots_.begin(); it != slots_.end();) {
            if (it->future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
                nixl_status_t s = it->future.get();
                if (s != NIXL_SUCCESS) {
                    slots_.clear();
                    return s;
                }
                if (done_indices != nullptr) {
                    done_indices->push_back(it->index);
                }
                it = slots_.erase(it);
            } else {
                ++it;
            }
        }
        return slots_.empty() ? NIXL_SUCCESS : NIXL_IN_PROG;
    }

    nixl_status_t getOverallStatus() {
        return poll(nullptr);
    }

    nixl_status_t getDoneIndices(std::vector<int> &done_indices) {
        return poll(&done_indices);
    }
};

// ────────────────────────────────────────────────────────────────────────────
// Per-DFS-op waiter (kept for the eventual async path; currently unused
// because we use synchronous DFS for bring-up safety).
// ────────────────────────────────────────────────────────────────────────────
struct [[maybe_unused]] S3SplitWaiter {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done = false;
    int                     err  = 0;
};

[[maybe_unused]] static int
s3split_completion_cb(void *arg, daos_event_t * /*ev*/, int ret) {
    auto *w = static_cast<S3SplitWaiter*>(arg);
    { std::lock_guard<std::mutex> lock(w->mtx); w->done = true; w->err = ret; }
    w->cv.notify_one();
    return 0;
}

// ────────────────────────────────────────────────────────────────────────────
// EQ pool (cloned from daos_direct)
// ────────────────────────────────────────────────────────────────────────────
void S3SplitPlaneObjEngineImpl::startEQPool(size_t n) {
    if (n < 1) n = 1;
    if (n > kMaxEQ) n = kMaxEQ;
    eq_pool_size_ = n;
    eq_run_.store(true);
    for (size_t i = 0; i < n; ++i) {
        int rc = daos_eq_create(&eq_pool_[i]);
        if (rc != 0) { eq_pool_[i] = DAOS_HDL_INVAL; continue; }
        eq_threads_[i] = std::thread([this, i]() {
            daos_event_t *completed[64];
            while (eq_run_.load(std::memory_order_relaxed))
                daos_eq_poll(eq_pool_[i], 0, 100, 64, completed);
        });
    }
}

void S3SplitPlaneObjEngineImpl::stopEQPool() {
    eq_run_.store(false);
    for (size_t i = 0; i < eq_pool_size_; ++i) {
        if (eq_threads_[i].joinable()) eq_threads_[i].join();
        if (eq_pool_[i].cookie != 0) daos_eq_destroy(eq_pool_[i], 0);
    }
}

daos_handle_t S3SplitPlaneObjEngineImpl::getEQ() const {
    if (eq_pool_size_ == 0) return DAOS_HDL_INVAL;
    return eq_pool_[eq_rr_.fetch_add(1, std::memory_order_relaxed) % eq_pool_size_];
}

// ────────────────────────────────────────────────────────────────────────────
// Construction / destruction
//
// We bring up BOTH a libdfs DAOS data plane AND a vanilla S3 client. The
// S3 client uses the AWS SDK default executor (we pass nullptr), so its
// async callbacks fire on a thread pool independent of `executor_` — this
// is important because we dispatch libdfs work from inside the success
// callback and don't want to deadlock if all chain threads were blocked.
// ────────────────────────────────────────────────────────────────────────────
S3SplitPlaneObjEngineImpl::S3SplitPlaneObjEngineImpl(const nixlBackendInitParams *init_params) {
    auto *p = init_params->customParams;
    pool_label_ = (p && p->count("daos_pool")) ? p->at("daos_pool") : "Pool1";
    cont_label_ = (p && p->count("daos_cont")) ? p->at("daos_cont") : "nixl_direct";

    // Executor pool size = caller-provided num_threads (1:1 mapping).
    // executor_ only carries libdfs tasks; HTTP callbacks go to the AWS
    // SDK's separate default pool (see s3_client_ construction below), so
    // there's no need to oversubscribe this pool.
    //
    // Default 16 is used when num_threads is unset (legacy s3rdma_direct
    // runs with no batch_mode wiring through customParams).
    size_t num_threads = 16;
    if (p && p->count("num_threads"))
        num_threads = std::max(size_t{1}, size_t(std::stoul(p->at("num_threads"))));
    if (p && p->count("num_threads_batch"))
        num_threads = std::max(size_t{1}, size_t(std::stoul(p->at("num_threads_batch"))));
    if (p && p->count("num_threads_daos"))
        num_threads = std::max(size_t{1}, size_t(std::stoul(p->at("num_threads_daos"))));
    num_threads_batch_ = static_cast<int>(num_threads);
    executor_ = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(num_threads);

    std::cerr << "S3 split-plane engine: pool=" << pool_label_
              << " cont=" << cont_label_ << std::endl;

    // ── data plane bringup (libdfs) ──────────────────────────────────────
    int rc = daos_init();
    std::cerr << "S3 split-plane: daos_init() returned " << rc << std::endl;
    if (rc != 0) return;

    rc = daos_pool_connect(pool_label_.c_str(), nullptr, DAOS_PC_RW, &poh_, nullptr, nullptr);
    std::cerr << "S3 split-plane: daos_pool_connect returned " << rc << std::endl;
    if (rc != 0) { daos_fini(); return; }

    rc = daos_cont_open(poh_, cont_label_.c_str(), DAOS_COO_RW, &coh_, nullptr, nullptr);
    if (rc == -DER_NONEXIST) {
        std::cerr << "S3 split-plane: creating container " << cont_label_ << std::endl;
        uuid_t cont_uuid;
        rc = dfs_cont_create_with_label(poh_, cont_label_.c_str(), nullptr,
                                        &cont_uuid, &coh_, &dfs_);
    } else if (rc == 0) {
        rc = dfs_mount(poh_, coh_, O_RDWR, &dfs_);
    }
    if (rc != 0 || !dfs_) {
        std::cerr << "S3 split-plane: container/mount failed: " << rc << std::endl;
        if (coh_.cookie != 0) daos_cont_close(coh_, nullptr);
        daos_pool_disconnect(poh_, nullptr);
        daos_fini();
        return;
    }
    std::cerr << "S3 split-plane: DFS mounted on " << cont_label_ << std::endl;

    const char *eq_env = std::getenv("NIXL_DAOS_EQ_POOL");
    startEQPool(eq_env ? static_cast<size_t>(std::atoi(eq_env)) : 8);

    // ── control plane bringup (S3 client) ────────────────────────────────
    // Pass nullptr for the executor so the AWS SDK uses its own default
    // pool — keeps callback dispatch isolated from `executor_`.
    s3_client_ = std::make_shared<awsS3Client>(init_params->customParams, nullptr);
    std::cerr << "S3 split-plane: S3 control client ready" << std::endl;

    // ── hashoid mode parsing ────────────────────────────────────────────
    // When enabled, the per-descriptor libdfs op (do_libdfs_op) is replaced
    // with do_hashoid_op — dfs_open is skipped, OID is synthesized via
    // hashoid.h (same encoding as DAOS agg_sidecar). Applies to both
    // s3rdma_direct (per-descriptor) and s3rdma_batch (fan-out).
    if (p && p->count("daos_hashoid") && p->at("daos_hashoid") == "true")
        hashoid_mode_ = true;
    if (p && p->count("hashoid_T"))
        hashoid_T_ = std::max(1, std::atoi(p->at("hashoid_T").c_str()));
    if (p && p->count("hashoid_IOD"))
        hashoid_IOD_ = std::max(1, std::atoi(p->at("hashoid_IOD").c_str()));
    if (hashoid_mode_) {
        std::cerr << "S3 split-plane: HASHOID mode enabled (T=" << hashoid_T_
                  << " IOD=" << hashoid_IOD_
                  << " — bypasses dfs_open on data plane)" << std::endl;
    }

    // ── batch mode parsing ──────────────────────────────────────────────
    // Per postXfer: 1 HTTP control hop, then N parallel dfs_reads (N from
    // batch_size). Sliding window over registered keys is engine-local.
    if (p && p->count("batch_mode") && p->at("batch_mode") == "1") {
        batch_mode_ = true;
        if (p->count("lmcache_batch_mode") && p->at("lmcache_batch_mode") == "1")
            lmcache_batch_mode_ = true;
        if (p->count("batch_size"))
            batch_size_ = std::stoi(p->at("batch_size"));
        if (p->count("server_aggregate_size"))
            server_aggregate_size_ = std::stoi(p->at("server_aggregate_size"));
        if (p->count("batch_window_start"))
            batch_window_offset_ = std::stoul(p->at("batch_window_start"));
        if (p->count("iodepth_daos")) {
            iodepth_batch_ = std::max(1, std::stoi(p->at("iodepth_daos")));
            batch_inflight_cap_ = static_cast<int>(num_threads) * iodepth_batch_;
        } else if (p->count("iodepth_batch")) {
            iodepth_batch_ = std::max(1, std::stoi(p->at("iodepth_batch")));
            batch_inflight_cap_ = static_cast<int>(num_threads) * iodepth_batch_;
        } else if (p->count("batch_inflight_cap")) {
            batch_inflight_cap_ = std::max(1, std::stoi(p->at("batch_inflight_cap")));
        }
        std::cerr << "S3 split-plane: batch mode enabled (1 HTTP + "
                  << batch_size_ << " dfs_reads per postXfer, throttled to "
                  << batch_inflight_cap_ << " in-flight; "
                  << "num_threads_daos=" << num_threads
                  << "; iodepth_daos=" << iodepth_batch_
                  << "; "
                  << "server_aggregate_size=" << server_aggregate_size_
                  << "; lmcache_batch_mode=" << (lmcache_batch_mode_ ? "1" : "0")
                  << ")"
                  << std::endl;
    }

    // ── aggregate mode parsing (s3rdma_agg) ─────────────────────────────
    if (p && p->count("daos_agg") && p->at("daos_agg") == "true") {
        daos_agg_mode_ = true;
        if (p->count("agg_chunks_per_layer"))
            agg_chunks_per_layer_ = std::max(1,
                std::atoi(p->at("agg_chunks_per_layer").c_str()));
        // chunks_per_layer = total chunks in one layer (= ISL/T). Defaults
        // to agg_chunks_per_layer_ for the legacy whole-layer-agg case
        // (agg unit = whole layer ⇒ chunks_per_layer == agg_chunks_per_layer).
        if (p->count("chunks_per_layer"))
            chunks_per_layer_ = std::max(1,
                std::atoi(p->at("chunks_per_layer").c_str()));
        else
            chunks_per_layer_ = agg_chunks_per_layer_;
        // Do NOT override batch_size_. With the new agg postXfer flow
        // (mirrors batch's "1 HTTP per load + N fan-out tasks"), the
        // caller sets batch_size_ to OBJECTS_PER_LOAD; the plugin then
        // groups the load's chunks into (batch_size_/agg_chunks_per_layer)
        // per-layer agg fetches and submits each as one executor task.
        std::cerr << "S3 split-plane: AGG mode enabled (agg_chunks="
                  << agg_chunks_per_layer_ << " layer_chunks="
                  << chunks_per_layer_ << " recipe T=" << hashoid_T_
                  << " IOD=" << hashoid_IOD_ << ")" << std::endl;
    }
    if (p && p->count("daos_agg_patch") && p->at("daos_agg_patch") == "true") {
        daos_agg_patch_mode_ = true;
        daos_agg_patch_lwagg_manifest_ =
            p->count("daos_agg_patch_lwagg_manifest") &&
            p->at("daos_agg_patch_lwagg_manifest") == "true";
        daos_agg_patch_rangeget_ =
            p->count("daos_agg_patch_rangeget") &&
            p->at("daos_agg_patch_rangeget") == "true";
        if (p->count("agg_chunks_per_layer"))
            agg_chunks_per_layer_ = std::max(1,
                std::atoi(p->at("agg_chunks_per_layer").c_str()));
        if (p->count("chunks_per_layer"))
            chunks_per_layer_ = std::max(1,
                std::atoi(p->at("chunks_per_layer").c_str()));
        else
            chunks_per_layer_ = agg_chunks_per_layer_;
        std::cerr << "S3 split-plane: AGG_PATCH mode enabled (same scheduler "
                  << "as s3rdma_batch, daos_obj_fetch_agg chunks="
                  << agg_chunks_per_layer_ << " layer_chunks="
                  << chunks_per_layer_
                  << (daos_agg_patch_lwagg_manifest_
                          ? " lwagg_manifest=on"
                          : "")
                  << (daos_agg_patch_rangeget_
                          ? " rangeget=on"
                          : "")
                  << ")" << std::endl;
    }

    // ── lw-Agg client emulation parsing ────────────────────────────────
    // Dry-run path for LMCache integration: use real LMCache keys for the
    // HTTP control hop, then range-fetch the corresponding chunkwise DAOS
    // objects from a pre-registered manifest.
    if (p && p->count("daos_lwagg_client_emulate") &&
        split_parse_bool(p->at("daos_lwagg_client_emulate"))) {
        daos_lwagg_client_emulate_ = true;
    }
    if (p && p->count("daos_lwagg_server_mode") &&
        split_parse_bool(p->at("daos_lwagg_server_mode"))) {
        daos_lwagg_server_mode_ = true;
    }
    if (p && p->count("lwagg_issue_mode"))
        lwagg_issue_mode_ = p->at("lwagg_issue_mode");
    if (p && p->count("lwagg_manifest_tsv"))
        lwagg_manifest_tsv_ = p->at("lwagg_manifest_tsv");
    if (p && p->count("lwagg_num_layers"))
        lwagg_num_layers_ = std::max(1,
            std::atoi(p->at("lwagg_num_layers").c_str()));
    if (p && p->count("lwagg_layer_bytes"))
        lwagg_layer_bytes_ = std::max<size_t>(
            1, static_cast<size_t>(std::stoull(p->at("lwagg_layer_bytes"))));
    if (p && p->count("lwagg_object_bytes"))
        lwagg_object_bytes_ = std::max<size_t>(
            1, static_cast<size_t>(std::stoull(p->at("lwagg_object_bytes"))));
    if (p && p->count("lwagg_manifest_start"))
        lwagg_manifest_start_ = static_cast<size_t>(
            std::stoull(p->at("lwagg_manifest_start")));

    // s3rdma_batch hashoid OID override manifest (Option A).
    // Loads a TSV with the same format as the lwagg manifest:
    //   chunk_idx<TAB>chunk_hash<TAB>oid_lo<TAB>oid_hi_user<TAB>object_key
    // For each row, records object_key -> (oid_lo, oid_hi_user). The
    // s3rdma_batch hashoid PUT/GET path consults this map BEFORE falling
    // back to splitmix(fnv1a(key)) derivation. Used to redirect writes
    // and reads to round-robin-placed OIDs.
    if (p && p->count("s3rdma_batch_oid_manifest_tsv"))
        s3rdma_batch_oid_manifest_tsv_ =
            p->at("s3rdma_batch_oid_manifest_tsv");
    if (!s3rdma_batch_oid_manifest_tsv_.empty()) {
        std::ifstream in(s3rdma_batch_oid_manifest_tsv_);
        std::string line;
        size_t loaded = 0;
        while (std::getline(in, line)) {
            if (line.empty() || line[0] == '#') continue;
            if (line.rfind("chunk_idx", 0) == 0) continue;
            std::stringstream ss(line);
            std::string chunk_idx, chunk_hash, oid_lo_s, oid_hi_s, object_key;
            if (!std::getline(ss, chunk_idx, '\t') ||
                !std::getline(ss, chunk_hash, '\t') ||
                !std::getline(ss, oid_lo_s, '\t') ||
                !std::getline(ss, oid_hi_s, '\t')) {
                continue;
            }
            std::getline(ss, object_key, '\t');
            if (object_key.empty()) continue;
            uint64_t lo = std::strtoull(oid_lo_s.c_str(), nullptr, 10);
            uint64_t hi = std::strtoull(oid_hi_s.c_str(), nullptr, 10);
            s3rdma_batch_oid_override_by_key_[object_key] = {lo, hi};
            ++loaded;
            (void)chunk_idx;
            (void)chunk_hash;
        }
        std::cerr << "S3 split-plane: s3rdma_batch OID override manifest "
                  << "loaded entries=" << loaded
                  << " path=" << s3rdma_batch_oid_manifest_tsv_ << std::endl;
    }

    if (daos_lwagg_client_emulate_ || daos_lwagg_server_mode_) {
        if (lwagg_manifest_tsv_.empty()) {
            std::cerr << "S3 split-plane: LWAGG requested "
                      << "without lwagg_manifest_tsv" << std::endl;
            daos_lwagg_client_emulate_ = false;
            daos_lwagg_server_mode_ = false;
        } else {
            std::ifstream in(lwagg_manifest_tsv_);
            std::string line;
            size_t loaded = 0;
            while (std::getline(in, line)) {
                if (line.empty() || line[0] == '#') continue;
                if (line.rfind("chunk_idx", 0) == 0) continue;

                std::stringstream ss(line);
                std::string chunk_idx;
                std::string chunk_hash;
                std::string oid_lo;
                std::string oid_hi_user;
                std::string object_key;
                if (!std::getline(ss, chunk_idx, '\t') ||
                    !std::getline(ss, chunk_hash, '\t') ||
                    !std::getline(ss, oid_lo, '\t') ||
                    !std::getline(ss, oid_hi_user, '\t')) {
                    continue;
                }
                std::getline(ss, object_key, '\t');

                LwAggManifestEntry entry;
                entry.oid_lo = std::strtoull(oid_lo.c_str(), nullptr, 10);
                entry.oid_hi_user = std::strtoull(oid_hi_user.c_str(), nullptr, 10);
                entry.object_key = object_key;
                if (!entry.object_key.empty())
                    lwagg_manifest_by_key_[entry.object_key] = lwagg_manifest_.size();
                lwagg_manifest_.push_back(std::move(entry));
                ++loaded;
                (void)chunk_idx;
                (void)chunk_hash;
            }
            if (lwagg_manifest_.empty()) {
                std::cerr << "S3 split-plane: failed to load LWAGG manifest "
                          << lwagg_manifest_tsv_ << std::endl;
                daos_lwagg_client_emulate_ = false;
                daos_lwagg_server_mode_ = false;
            } else {
                std::cerr << "S3 split-plane: LWAGG enabled "
                          << "(client_emulate="
                          << (daos_lwagg_client_emulate_ ? "1" : "0")
                          << " server_mode="
                          << (daos_lwagg_server_mode_ ? "1" : "0")
                          << " manifest=" << lwagg_manifest_tsv_
                          << " entries=" << loaded
                          << " keyed=" << lwagg_manifest_by_key_.size()
                          << " issue_mode=" << lwagg_issue_mode_
                          << " layers=" << lwagg_num_layers_
                          << " layer_bytes=" << lwagg_layer_bytes_
                          << " object_bytes=" << lwagg_object_bytes_
                          << " start=" << lwagg_manifest_start_
                          << ")" << std::endl;
            }
        }

        if (daos_lwagg_server_mode_ && lwagg_layer_bytes_ > 0 &&
            chunks_per_layer_ > 0) {
            size_t stage_bytes =
                static_cast<size_t>(lwagg_num_layers_) *
                static_cast<size_t>(chunks_per_layer_) *
                lwagg_layer_bytes_;
            bool overflow =
                stage_bytes / lwagg_layer_bytes_ !=
                static_cast<size_t>(lwagg_num_layers_) *
                static_cast<size_t>(chunks_per_layer_);
            if (!overflow && stage_bytes > 0) {
                try {
                    lwagg_stage_buf_ =
                        std::make_shared<std::vector<char>>(stage_bytes);
                    std::memset(lwagg_stage_buf_->data(), 0,
                                lwagg_stage_buf_->size());
                    lwagg_stage_buf_bytes_ = stage_bytes;

                    const char *mlock_env =
                        std::getenv("NIXL_LWAGG_MLOCK_STAGE");
                    bool want_mlock =
                        mlock_env && split_parse_bool(mlock_env);
                    if (want_mlock) {
                        if (::mlock(lwagg_stage_buf_->data(),
                                    lwagg_stage_buf_->size()) == 0) {
                            lwagg_stage_buf_mlocked_ = true;
                        } else {
                            std::cerr << "S3 split-plane: LWAGG stage "
                                      << "mlock failed; continuing with "
                                      << "resident pre-touched buffer"
                                      << std::endl;
                        }
                    }
                    std::cerr << "S3 split-plane: LWAGG preallocated "
                              << "stage buffer bytes=" << stage_bytes
                              << " mlocked="
                              << (lwagg_stage_buf_mlocked_ ? "1" : "0")
                              << std::endl;
                } catch (const std::exception &e) {
                    std::cerr << "S3 split-plane: LWAGG stage prealloc "
                              << "failed: " << e.what() << std::endl;
                    lwagg_stage_buf_.reset();
                    lwagg_stage_buf_bytes_ = 0;
                }
            }
        }
    }
}

S3SplitPlaneObjEngineImpl::~S3SplitPlaneObjEngineImpl() {
    if (lwagg_stage_buf_mlocked_ && lwagg_stage_buf_ &&
        !lwagg_stage_buf_->empty()) {
        ::munlock(lwagg_stage_buf_->data(), lwagg_stage_buf_->size());
        lwagg_stage_buf_mlocked_ = false;
    }
    stopEQPool();
    if (dfs_) { dfs_umount(dfs_); dfs_ = nullptr; }
    if (coh_.cookie != 0) daos_cont_close(coh_, nullptr);
    if (poh_.cookie != 0) daos_pool_disconnect(poh_, nullptr);
    daos_fini();
}

// ────────────────────────────────────────────────────────────────────────────
// registerMem / deregisterMem / queryMem / prepXfer
// ────────────────────────────────────────────────────────────────────────────
nixl_status_t
S3SplitPlaneObjEngineImpl::registerMem(const nixlBlobDesc &mem,
                                       const nixl_mem_t &nixl_mem,
                                       nixlBackendMD *&out) {
    if (nixl_mem == OBJ_SEG) {
        auto md = std::make_unique<S3SplitObjMD>(
            nixl_mem, mem.devId,
            mem.metaInfo.empty() ? std::to_string(mem.devId) : mem.metaInfo);
        devIdToObjKey_[mem.devId] = md->objKey;
        out = md.release();
    } else if (nixl_mem == DRAM_SEG || nixl_mem == VRAM_SEG) {
        out = new S3SplitDramMD(mem.addr, mem.len);
        if (batch_mode_) {
            batch_buf_addr_ = mem.addr;
            batch_buf_len_  = mem.len;
        }
    } else {
        return NIXL_ERR_NOT_SUPPORTED;
    }
    return NIXL_SUCCESS;
}

nixl_status_t
S3SplitPlaneObjEngineImpl::deregisterMem(nixlBackendMD *meta) {
    if (auto *obj_md = dynamic_cast<S3SplitObjMD*>(meta))
        devIdToObjKey_.erase(obj_md->devId);
    delete meta;
    return NIXL_SUCCESS;
}

nixl_status_t
S3SplitPlaneObjEngineImpl::queryMem(const nixl_reg_dlist_t &descs,
                                    std::vector<nixl_query_resp_t> &resp) const {
    for (int i = 0; i < descs.descCount(); ++i)
        resp.push_back(nixl_b_params_t{});
    return NIXL_SUCCESS;
}

nixl_status_t
S3SplitPlaneObjEngineImpl::prepXfer(const nixl_xfer_op_t &,
                                    const nixl_meta_dlist_t &,
                                    const nixl_meta_dlist_t &remote,
                                    const std::string &,
                                    const std::string &,
                                    nixlBackendReqH *&handle,
                                    const nixl_opt_b_args_t *) const {
    for (int i = 0; i < remote.descCount(); ++i) {
        if (devIdToObjKey_.find(remote[i].devId) == devIdToObjKey_.end())
            return NIXL_ERR_INVALID_PARAM;
    }
    handle = nullptr;
    return NIXL_SUCCESS;
}

// ────────────────────────────────────────────────────────────────────────────
// libdfs op — runs on a chain executor thread (NOT on an AWS SDK callback
// thread). Issues async dfs_write/read against the EQ pool and waits on a
// per-op condition variable. State is heap-allocated and held alive by
// shared_ptr captures so it survives until the EQ poller fires the
// completion callback (matches daos_direct/engine_impl.cpp pattern).
//
// Returns 0 on success, non-zero on failure (DAOS errno).
// ────────────────────────────────────────────────────────────────────────────
static int
do_libdfs_op(dfs_t *dfs,
             daos_handle_t eq,
             const std::string &key,
             nixl_xfer_op_t op,
             uintptr_t data_ptr,
             size_t data_len,
             size_t offset,
             uint64_t req_id) {
    NIXL_OBJ_US_R("split_dfs_open_pre", req_id);
    int flags = (op == NIXL_WRITE) ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDONLY;
    dfs_obj_t *obj = nullptr;
    int rc = dfs_open(dfs, nullptr, key.c_str(),
                      S_IFREG | DEFFILEMODE, flags, 0, 0, nullptr, &obj);
    NIXL_OBJ_US_R("split_dfs_open_post", req_id);
    if (rc != 0) {
        NIXL_OBJ_US_R("split_dfs_open_failed", req_id);
        return rc;
    }

    // Heap-allocate the iov/sgl so they survive any internal DAOS
    // callback (defensive). Use SYNCHRONOUS DFS — pass nullptr for the
    // event — to skip the EQ pool entirely. This is slower per op but
    // avoids any chance of an event-handling race during bring-up. We
    // can swap to async (EQ pool) once split-plane is proven correct.
    (void)eq;  // unused in sync mode
    auto iov = std::make_shared<d_iov_t>();
    d_iov_set(iov.get(), reinterpret_cast<void*>(data_ptr), data_len);
    auto sgl = std::make_shared<d_sg_list_t>();
    *sgl = {};
    sgl->sg_nr     = 1;
    sgl->sg_iovs   = iov.get();
    sgl->sg_nr_out = 1;

    // Tag the next obj_rw RPC(s) issued on this thread with our req_id so
    // the DAOS server's DAOS_US trace stamps the same value on every
    // rpc_recv / vos_begin / vos_end / rpc_reply for this request. That
    // lets the post-sweep merger join client<->server events by exact
    // equality instead of fuzzy time-windowing. See
    // /opt/daos_install/include/daos/daos_nixl_req_id.h for the plumbing.
    daos_set_nixl_req_id(req_id);
    NIXL_OBJ_US_R("split_dfs_issue", req_id);
    if (op == NIXL_WRITE) {
        rc = dfs_write(dfs, obj, sgl.get(), offset, nullptr);
    } else {
        daos_size_t got = data_len;
        rc = dfs_read(dfs, obj, sgl.get(), offset, &got, nullptr);
    }
    NIXL_OBJ_US_R("split_dfs_issued", req_id);
    daos_set_nixl_req_id(0);

    if (rc != 0) {
        NIXL_OBJ_US_R("split_dfs_issue_failed", req_id);
        dfs_release(obj);
        return rc;
    }

    NIXL_OBJ_US_R("split_dfs_wait_post", req_id);
    dfs_release(obj);
    return 0;
}

// ────────────────────────────────────────────────────────────────────────────
// Hashoid variant of do_libdfs_op. Skips dfs_open (no dentry lookup); OID is
// synthesized client-side via hashoid.h. Synchronous (matches do_libdfs_op).
// Returns 0 on success, non-zero on failure (DAOS errno).
// ────────────────────────────────────────────────────────────────────────────
static int
do_hashoid_op(daos_handle_t coh,
              const std::string &key,
              int T, int IOD,
              nixl_xfer_op_t op,
              uintptr_t data_ptr,
              size_t data_len,
              size_t offset,
              uint64_t req_id,
              const std::unordered_map<std::string,
                  std::pair<uint64_t, uint64_t>> *oid_override = nullptr) {
    NIXL_OBJ_US_R("split_hashoid_open_pre", req_id);
    daos_obj_id_t oid;
    bool overridden = false;
    if (oid_override != nullptr) {
        auto it = oid_override->find(key);
        if (it != oid_override->end()) {
            oid.lo = it->second.first;
            oid.hi = it->second.second;
            overridden = true;
        }
    }
    if (!overridden) {
        oid.lo = hashoid_oid_lo(
            static_cast<int>(split_hashoid_fnv1a_32(key.data(), key.size())));
        oid.hi = hashoid_oid_hi_user(T, IOD, data_len);
    }
    int rc = daos_obj_generate_oid(coh, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
    if (rc != 0) {
        NIXL_OBJ_US_R("split_hashoid_genoid_failed", req_id);
        return rc;
    }

    daos_handle_t oh = DAOS_HDL_INVAL;
    rc = daos_obj_open(coh, oid,
                       (op == NIXL_WRITE) ? DAOS_OO_RW : DAOS_OO_RO,
                       &oh, nullptr);
    NIXL_OBJ_US_R("split_hashoid_open_post", req_id);
    if (rc != 0) return rc;

    daos_key_t dkey;
    d_iov_set(&dkey, (void*)kSplitHashoidDkey, sizeof(kSplitHashoidDkey) - 1);
    daos_recx_t recx;
    recx.rx_idx = offset;
    recx.rx_nr  = data_len;
    daos_iod_t iod = {};
    d_iov_set(&iod.iod_name, (void*)kSplitHashoidAkey, sizeof(kSplitHashoidAkey) - 1);
    iod.iod_type  = DAOS_IOD_ARRAY;
    iod.iod_size  = 1;
    iod.iod_nr    = 1;
    iod.iod_recxs = &recx;

    d_iov_t iov;
    d_iov_set(&iov, reinterpret_cast<void*>(data_ptr), data_len);
    d_sg_list_t sgl = {};
    sgl.sg_nr     = 1;
    sgl.sg_iovs   = &iov;
    sgl.sg_nr_out = 1;

    // See do_libdfs_op for the rationale — tag the next obj_rw RPC with
    // this request's nixl_req_id so the DAOS server trace can be joined
    // by exact equality.
    daos_set_nixl_req_id(req_id);
    NIXL_OBJ_US_R("split_hashoid_issue", req_id);
    if (op == NIXL_WRITE) {
        rc = daos_obj_update(oh, DAOS_TX_NONE, 0, &dkey, 1, &iod, &sgl, nullptr);
    } else {
        rc = daos_obj_fetch(oh, DAOS_TX_NONE, 0, &dkey, 1, &iod, &sgl, nullptr, nullptr);
    }
    NIXL_OBJ_US_R("split_hashoid_issued", req_id);
    daos_set_nixl_req_id(0);

    daos_obj_close(oh, nullptr);
    return rc;
}

static int
do_lwagg_manifest_range_fetch(daos_handle_t coh,
                              uint64_t oid_lo,
                              uint64_t oid_hi_user,
                              uintptr_t data_ptr,
                              size_t data_len,
                              int num_layers,
                              size_t layer_bytes,
                              uint64_t req_id) {
    if (num_layers <= 0 || layer_bytes == 0) return -DER_INVAL;
    const size_t total_layer_bytes = static_cast<size_t>(num_layers) * layer_bytes;
    if (total_layer_bytes > data_len) return -DER_INVAL;

    daos_obj_id_t oid;
    oid.lo = oid_lo;
    oid.hi = oid_hi_user;
    NIXL_OBJ_US_R("split_lwagg_open_pre", req_id);
    int rc = daos_obj_generate_oid(coh, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
    if (rc != 0) {
        NIXL_OBJ_US_R("split_lwagg_genoid_failed", req_id);
        return rc;
    }

    daos_handle_t oh = DAOS_HDL_INVAL;
    rc = daos_obj_open(coh, oid, DAOS_OO_RO, &oh, nullptr);
    NIXL_OBJ_US_R("split_lwagg_open_post", req_id);
    if (rc != 0) return rc;

    daos_key_t dkey;
    d_iov_set(&dkey, (void*)kSplitHashoidDkey, sizeof(kSplitHashoidDkey) - 1);

    std::vector<daos_recx_t> recxs(static_cast<size_t>(num_layers));
    std::vector<d_iov_t> iovs(static_cast<size_t>(num_layers));
    for (int layer = 0; layer < num_layers; ++layer) {
        const size_t off = static_cast<size_t>(layer) * layer_bytes;
        recxs[static_cast<size_t>(layer)].rx_idx = off;
        recxs[static_cast<size_t>(layer)].rx_nr  = layer_bytes;
        d_iov_set(&iovs[static_cast<size_t>(layer)],
                  reinterpret_cast<void*>(data_ptr + off),
                  layer_bytes);
    }

    daos_iod_t iod = {};
    d_iov_set(&iod.iod_name, (void*)kSplitHashoidAkey, sizeof(kSplitHashoidAkey) - 1);
    iod.iod_type  = DAOS_IOD_ARRAY;
    iod.iod_size  = 1;
    iod.iod_nr    = static_cast<unsigned int>(num_layers);
    iod.iod_recxs = recxs.data();

    d_sg_list_t sgl = {};
    sgl.sg_nr     = iovs.size();
    sgl.sg_iovs   = iovs.data();
    sgl.sg_nr_out = iovs.size();

    daos_set_nixl_req_id(req_id);
    NIXL_OBJ_US_R("split_lwagg_issue", req_id);
    rc = daos_obj_fetch(oh, DAOS_TX_NONE, 0, &dkey, 1, &iod, &sgl, nullptr, nullptr);
    NIXL_OBJ_US_R("split_lwagg_issued", req_id);
    daos_set_nixl_req_id(0);

    daos_obj_close(oh, nullptr);
    return rc;
}

// ────────────────────────────────────────────────────────────────────────────
// postXfer:
//
//   For each (local, remote) descriptor pair:
//     1. Issue HTTP control hop via awsS3Client (async, fires on AWS SDK
//        thread).
//     2. From the success callback, dispatch a libdfs task to executor_.
//     3. The libdfs task runs do_libdfs_op() and sets the per-descriptor
//        promise.
//
// The user-visible invariant — "libdfs is only issued after the HTTP
// success callback fires" — is enforced by the chaining: libdfs is dispatched
// from inside the HTTP callback, so step 3 cannot run before step 2 sees a
// 200 OK from RGW.
// ────────────────────────────────────────────────────────────────────────────
nixl_status_t
S3SplitPlaneObjEngineImpl::postXfer(const nixl_xfer_op_t &operation,
                                    const nixl_meta_dlist_t &local,
                                    const nixl_meta_dlist_t &remote,
                                    const std::string &,
                                    nixlBackendReqH *&handle,
                                    const nixl_opt_b_args_t *) const {
    if (!dfs_) return NIXL_ERR_BACKEND;
    if (!s3_client_) return NIXL_ERR_BACKEND;

    static std::atomic<uint64_t> g_req_id{0};
    uint64_t batch_id = g_req_id.fetch_add(1, std::memory_order_relaxed);
    NIXL_OBJ_US_R("split_postXfer_enter", batch_id);

    auto *req_h = new S3SplitReqH();
    handle = req_h;

    dfs_t *dfs_local = dfs_;
    auto executor = executor_;
    auto get_eq = [this]() { return getEQ(); };

    // Hashoid data-plane swap: when enabled, lambdas below invoke
    // do_hashoid_op(coh_local, ...) instead of do_libdfs_op(dfs_local, ...).
    daos_handle_t coh_local   = coh_;
    bool          hashoid_on  = hashoid_mode_;
    int           hashoid_T   = hashoid_T_;
    int           hashoid_IOD = hashoid_IOD_;

    // ── lw-Agg DAOS server-side path ───────────────────────────────────
    // Keep the LMCache/NIXL/RGW control hop unchanged, then issue
    // synthetic aggregate DAOS reads. Each RPC asks the DAOS server to
    // map a manifest chunk window and fetch one layer slice from each
    // chunkwise object. The client receives the aggregate as one contiguous
    // object, matching s3rdma_batch's single-iov fetch shape.
    if (daos_lwagg_server_mode_ && hashoid_on && batch_mode_ &&
        operation == NIXL_READ && local.descCount() > 0) {
        if (!s3_client_) return NIXL_ERR_BACKEND;
        if (lwagg_manifest_.empty()) return NIXL_ERR_INVALID_PARAM;
        if (agg_chunks_per_layer_ <= 0) return NIXL_ERR_INVALID_PARAM;

        auto ops_shared = std::make_shared<std::vector<PendingBatchOp>>();
        ops_shared->reserve(local.descCount());
        std::vector<std::string> keys;
        keys.reserve(local.descCount());
        std::vector<std::shared_ptr<std::promise<nixl_status_t>>> promises;
        promises.reserve(local.descCount());

        for (int i = 0; i < local.descCount(); ++i) {
            const auto &ld = local[i];
            const auto &rd = remote[i];
            auto it = devIdToObjKey_.find(rd.devId);
            if (it == devIdToObjKey_.end()) return NIXL_ERR_INVALID_PARAM;

            auto promise = std::make_shared<std::promise<nixl_status_t>>();
            req_h->addFuture(i, promise->get_future());
            promises.push_back(promise);
            uint64_t req_id = (batch_id << 16) | static_cast<uint16_t>(i);

            ops_shared->push_back(
                PendingBatchOp{it->second, ld.addr, ld.len, rd.addr, nullptr,
                               req_id});
            keys.push_back(it->second);
        }
        if (lwagg_manifest_start_ + ops_shared->size() > lwagg_manifest_.size())
            return NIXL_ERR_INVALID_PARAM;

        const size_t object_size = lwagg_object_bytes_ > 0
                                     ? lwagg_object_bytes_
                                     : ops_shared->front().data_len;
        const bool range_object_mode =
            object_size != ops_shared->front().data_len;
        size_t effective_layer_bytes = lwagg_layer_bytes_;
        if (effective_layer_bytes == 0) {
            if (lwagg_num_layers_ <= 0 ||
                object_size % static_cast<size_t>(lwagg_num_layers_) != 0) {
                return NIXL_ERR_INVALID_PARAM;
            }
            effective_layer_bytes =
                object_size / static_cast<size_t>(lwagg_num_layers_);
        }

        const int group_cap = std::max(1, agg_chunks_per_layer_);
        const int num_groups =
            static_cast<int>((ops_shared->size() + group_cap - 1) / group_cap);
        const int total_fetches = num_groups * lwagg_num_layers_;

        auto layer_handles =
            std::make_shared<std::vector<daos_handle_t>>(
                static_cast<size_t>(lwagg_num_layers_), DAOS_HDL_INVAL);
        auto layer_handles_closed = std::make_shared<std::atomic<bool>>(false);
        auto close_layer_handles = [layer_handles, layer_handles_closed]() {
            bool expected = false;
            if (!layer_handles_closed->compare_exchange_strong(expected, true))
                return;
            for (auto &oh : *layer_handles) {
                if (!daos_handle_is_inval(oh)) {
                    daos_obj_close(oh, nullptr);
                    oh = DAOS_HDL_INVAL;
                }
            }
        };

        auto remaining = std::make_shared<std::atomic<int>>(total_fetches);
        auto first_err = std::make_shared<std::atomic<int>>(0);
        auto resolve_all = [promises, remaining, first_err, batch_id,
                            close_layer_handles](nixl_status_t s) {
            if (s != NIXL_SUCCESS) {
                int expected = 0;
                first_err->compare_exchange_strong(expected, (int)s);
            }
            if (remaining->fetch_sub(1) == 1) {
                close_layer_handles();
                int err = first_err->load();
                nixl_status_t final_status =
                    err == 0 ? NIXL_SUCCESS : (nixl_status_t)err;
                NIXL_OBJ_US_R("split_lwagg_server_load_done", batch_id);
                for (auto &p : promises)
                    p->set_value(final_status);
            }
        };

        daos_handle_t coh_cap = coh_local;
        int T_cap = hashoid_T;
        int IOD_cap = hashoid_IOD;
        int num_layers_cap = lwagg_num_layers_;
        size_t layer_bytes_cap = effective_layer_bytes;
        auto get_eq_cap = get_eq;
        auto executor_cap = executor;
        int inflight_cap = batch_inflight_cap_;
        int lanes_cap = std::max(1, num_threads_batch_);
        int depth_cap = iodepth_batch_ > 0
                          ? iodepth_batch_
                          : std::max(1, (inflight_cap + lanes_cap - 1) /
                                         lanes_cap);
        auto prealloc_stage_buf = lwagg_stage_buf_;
        size_t prealloc_stage_bytes = lwagg_stage_buf_bytes_;
        size_t manifest_start_cap = lwagg_manifest_start_;
        const std::string lwagg_issue_mode_cap = lwagg_issue_mode_;
        const bool per_fetch_open_cap =
            (range_object_mode ||
             lwagg_issue_mode_cap == "s3rdma_agg" ||
             lwagg_issue_mode_cap == "perfetch_open");

        bool preopen_ok = true;
        if (!per_fetch_open_cap) {
            for (int layer = 0; layer < num_layers_cap; ++layer) {
                daos_obj_id_t layer_oid{};
                layer_oid.lo = hashoid_layer_base(layer);
                layer_oid.hi = hashoid_oid_hi_user(T_cap, IOD_cap, object_size);
                uint64_t trace_id = (batch_id << 16) |
                                    (0x8000u | static_cast<uint16_t>(layer));
                NIXL_OBJ_US_R("split_lwagg_server_layer_genoid_pre", trace_id);
                int rc = daos_obj_generate_oid(coh_cap, &layer_oid,
                                               DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
                NIXL_OBJ_US_R("split_lwagg_server_layer_genoid_post", trace_id);
                if (rc != 0) {
                    preopen_ok = false;
                    break;
                }
                NIXL_OBJ_US_R("split_lwagg_server_layer_open_pre", trace_id);
                rc = daos_obj_open(coh_cap, layer_oid, DAOS_OO_RO,
                                   &layer_handles->at(static_cast<size_t>(layer)),
                                   nullptr);
                NIXL_OBJ_US_R("split_lwagg_server_layer_open_post", trace_id);
                if (rc != 0) {
                    preopen_ok = false;
                    break;
                }
            }
        }
        if (!preopen_ok) {
            close_layer_handles();
            for (int i = 0; i < total_fetches; ++i)
                resolve_all(NIXL_ERR_BACKEND);
            NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
            return NIXL_IN_PROG;
        }

        NIXL_OBJ_US_R("split_lwagg_server_http_start", batch_id);
        s3_client_->headBatchControlAsync(
            keys, object_size, server_aggregate_size_,
            [ops_shared, num_groups, group_cap, object_size, layer_bytes_cap,
             T_cap, IOD_cap, coh_cap, get_eq_cap, executor_cap, resolve_all,
             num_layers_cap, batch_id, inflight_cap, lanes_cap,
             depth_cap, layer_handles, manifest_start_cap, prealloc_stage_buf,
             prealloc_stage_bytes, range_object_mode,
             issue_mode_cap = lwagg_issue_mode_cap,
             per_fetch_open_cap](bool http_ok) mutable {
                NIXL_OBJ_US_R("split_lwagg_server_http_done", batch_id);
                if (!http_ok) {
                    for (int i = 0; i < num_groups * num_layers_cap; ++i)
                        resolve_all(NIXL_ERR_BACKEND);
                    return;
                }

                const int total_fetches_cap = num_groups * num_layers_cap;
                const int pipe_cap = std::max(1, std::min(inflight_cap,
                                                          total_fetches_cap));
                const size_t max_aggregate_bytes =
                    static_cast<size_t>(group_cap) * layer_bytes_cap;
                const size_t per_layer_stage_bytes =
                    ops_shared->size() * layer_bytes_cap;
                const size_t needed_stage_bytes =
                    static_cast<size_t>(num_layers_cap) *
                    per_layer_stage_bytes;
                auto fallback_stage_buf =
                    std::make_shared<std::vector<char>>();
                bool use_prealloc_stage =
                    prealloc_stage_buf && !prealloc_stage_buf->empty() &&
                    prealloc_stage_bytes >= needed_stage_bytes;
                if (!use_prealloc_stage)
                    fallback_stage_buf->resize(needed_stage_bytes);
                auto issue_fetch =
                    [ops_shared, num_groups, group_cap, object_size,
                     layer_bytes_cap, T_cap, IOD_cap, coh_cap,
	                     resolve_all, num_layers_cap, max_aggregate_bytes,
	                     layer_handles,
	                     prealloc_stage_buf, fallback_stage_buf,
	                     use_prealloc_stage, per_layer_stage_bytes,
	                     manifest_start_cap, per_fetch_open_cap,
                         range_object_mode, batch_id]
                    (AggBatchCtx& ctx, int idx, daos_handle_t lane_eq,
                     AggCompletionQueue *lane_q) mutable -> bool {
                        if (idx >= num_groups * num_layers_cap) return false;

                        int layer_idx = idx / num_groups;
                        int g = idx % num_groups;
                        size_t begin = static_cast<size_t>(g) * group_cap;
                        size_t end = std::min(begin + static_cast<size_t>(group_cap),
                                              ops_shared->size());
                        if (begin >= end) {
                            resolve_all(NIXL_SUCCESS);
                            return false;
                        }

                        // Prefix-hit lw-Agg uses the canonical manifest order:
                        // request chunk i maps to DAOS manifest[start + i].
                        // The server receives that interval start in the
                        // recipe and uses DAOS_LWAGG_MANIFEST to resolve the
                        // real chunk hashoids.
                        size_t first_manifest_idx = manifest_start_cap + begin;

                        {
                            std::lock_guard<std::mutex> lk(ctx.waiter.mtx);
                            ctx.waiter.done = false;
                            ctx.waiter.err = 0;
                        }
                        ctx.oh = DAOS_HDL_INVAL;
                        ctx.ev = {};
                        ctx.dkey = {};
                        ctx.recx = {};
                        ctx.iod = {};
                        ctx.iov = {};
                        ctx.sgl = {};
                        ctx.completion_q = lane_q;

                        const uint64_t trace_req_id =
                            (batch_id << 16) | static_cast<uint16_t>(idx);
                        NIXL_OBJ_US_R("split_lwagg_server_pick", trace_req_id);
                        if (per_fetch_open_cap) {
                            daos_obj_id_t layer_oid{};
                            layer_oid.lo = range_object_mode
                                ? hashoid_layer_base(0) +
                                      static_cast<uint64_t>(first_manifest_idx)
                                : hashoid_layer_base(layer_idx);
                            layer_oid.hi = hashoid_oid_hi_user(
                                T_cap, IOD_cap, object_size);
                            NIXL_OBJ_US_R("split_lwagg_server_layer_genoid_pre",
                                          trace_req_id);
                            int rc = daos_obj_generate_oid(
                                coh_cap, &layer_oid, DAOS_OT_MULTI_HASHED,
                                OC_SX, 0, 0);
                            NIXL_OBJ_US_R("split_lwagg_server_layer_genoid_post",
                                          trace_req_id);
                            if (rc != 0) {
                                resolve_all(NIXL_ERR_BACKEND);
                                return false;
                            }
                            NIXL_OBJ_US_R("split_lwagg_server_layer_open_pre",
                                          trace_req_id);
                            rc = daos_obj_open(coh_cap, layer_oid, DAOS_OO_RO,
                                               &ctx.oh, nullptr);
                            NIXL_OBJ_US_R("split_lwagg_server_layer_open_post",
                                          trace_req_id);
                            if (rc != 0) {
                                resolve_all(NIXL_ERR_BACKEND);
                                return false;
                            }
                            ctx.close_oh_on_cleanup = true;
                        } else {
                            ctx.oh = layer_handles->at(
                                static_cast<size_t>(layer_idx));
                            if (daos_handle_is_inval(ctx.oh)) {
                                resolve_all(NIXL_ERR_BACKEND);
                                return false;
                            }
                            ctx.close_oh_on_cleanup = false;
                        }

                        d_iov_set(&ctx.dkey, (void*)kSplitHashoidDkey,
                                  sizeof(kSplitHashoidDkey) - 1);
                        ctx.recx.rx_idx = range_object_mode
                            ? static_cast<uint64_t>(layer_idx) *
                                  layer_bytes_cap
                            : static_cast<uint64_t>(first_manifest_idx) *
                                  layer_bytes_cap;
                        ctx.recx.rx_nr =
                            static_cast<uint64_t>(end - begin) * layer_bytes_cap;
                        ctx.iod = {};
                        d_iov_set(&ctx.iod.iod_name, (void*)kSplitHashoidAkey,
                                  sizeof(kSplitHashoidAkey) - 1);
                        ctx.iod.iod_type  = DAOS_IOD_ARRAY;
                        ctx.iod.iod_size  = 1;
                        ctx.iod.iod_nr    = 1;
                        ctx.iod.iod_recxs = &ctx.recx;

                        ctx.iovs.resize(static_cast<size_t>(end - begin));
                        for (size_t j = begin; j < end; ++j) {
                            d_iov_set(&ctx.iovs[j - begin],
                                      reinterpret_cast<void*>(
                                          ops_shared->at(j).data_ptr),
                                      ops_shared->at(j).data_len);
                        }
                        ctx.sgl = {};
                        ctx.sgl.sg_nr     = ctx.iovs.size();
                        ctx.sgl.sg_nr_out = ctx.iovs.size();
                        ctx.sgl.sg_iovs   = ctx.iovs.data();
                        NIXL_OBJ_US_R("split_lwagg_server_sgl_ready", trace_req_id);

                        NIXL_OBJ_US_R("split_lwagg_server_event_init_pre", trace_req_id);
                        int rc = daos_event_init(&ctx.ev, lane_eq, nullptr);
                        NIXL_OBJ_US_R("split_lwagg_server_event_init_post", trace_req_id);
                        if (rc != 0) {
                            if (ctx.close_oh_on_cleanup &&
                                !daos_handle_is_inval(ctx.oh)) {
                                daos_obj_close(ctx.oh, nullptr);
                                ctx.oh = DAOS_HDL_INVAL;
                            }
                            resolve_all(NIXL_ERR_BACKEND);
                            return false;
                        }
                        ctx.batch_id = trace_req_id;
                        daos_event_register_comp_cb(
                            &ctx.ev, agg_batch_completion_cb, &ctx);

                        const uint32_t lwagg_count =
                            static_cast<uint32_t>(end - begin);
                        const uint32_t lwagg_layer_recipe =
                            kLwAggRecipeFlag |
                            static_cast<uint32_t>(layer_idx);
                        const uint32_t lwagg_manifest_start =
                            static_cast<uint32_t>(first_manifest_idx);
                        NIXL_OBJ_US_R("split_lwagg_server_recipe_set", trace_req_id);
                        NIXL_OBJ_US_R("split_lwagg_server_fetch_pre", trace_req_id);
                        daos_set_nixl_req_id(trace_req_id);
                        daos_set_nixl_agg_recipe(
                            lwagg_count,
                            static_cast<uint32_t>(layer_bytes_cap),
                            lwagg_layer_recipe,
                            lwagg_manifest_start);
                        rc = daos_obj_fetch(ctx.oh, DAOS_TX_NONE, 0,
                                            &ctx.dkey, 1, &ctx.iod,
                                            &ctx.sgl, nullptr, &ctx.ev);
                        daos_set_nixl_agg_recipe(0, 0, 0, 0);
                        daos_set_nixl_req_id(0);
                        NIXL_OBJ_US_R("split_lwagg_server_fetch_post", trace_req_id);
                        if (rc != 0) {
                            daos_event_fini(&ctx.ev);
                            if (ctx.close_oh_on_cleanup &&
                                !daos_handle_is_inval(ctx.oh)) {
                                daos_obj_close(ctx.oh, nullptr);
                                ctx.oh = DAOS_HDL_INVAL;
                            }
                            resolve_all(NIXL_ERR_BACKEND);
                            return false;
                        }

                        return true;
                };

                if (issue_mode_cap == "taskpool" ||
                    issue_mode_cap == "s3rdma_agg") {
                    for (int idx = 0; idx < total_fetches_cap; ++idx) {
                        executor_cap->Submit([issue_fetch, get_eq_cap,
                                              resolve_all, idx]() mutable {
                            AggBatchCtx ctx;
                            daos_handle_t eq = get_eq_cap();
                            if (!issue_fetch(ctx, idx, eq, nullptr))
                                return;

                            {
                                std::unique_lock<std::mutex> lk(ctx.waiter.mtx);
                                ctx.waiter.cv.wait(lk, [&] {
                                    return ctx.waiter.done;
                                });
                            }

                            int err = 0;
                            {
                                std::lock_guard<std::mutex> lk(ctx.waiter.mtx);
                                err = ctx.waiter.err;
                            }

                            daos_event_fini(&ctx.ev);
                            if (ctx.close_oh_on_cleanup &&
                                !daos_handle_is_inval(ctx.oh)) {
                                daos_obj_close(ctx.oh, nullptr);
                                ctx.oh = DAOS_HDL_INVAL;
                            }
                            NIXL_OBJ_US_R("split_lwagg_server_cleanup_done",
                                          ctx.batch_id);
                            resolve_all(err == 0 ? NIXL_SUCCESS
                                                 : NIXL_ERR_BACKEND);
                        });
                    }
                    return;
                }

                int lanes = std::max(1, lanes_cap);
                int depth = std::max(1, depth_cap);
                int max_lanes_by_cap = std::max(1, inflight_cap / depth);
                lanes = std::min(lanes, max_lanes_by_cap);
                lanes = std::min(lanes, pipe_cap);
                auto next_dynamic_idx =
                    std::make_shared<std::atomic<int>>(pipe_cap);

                for (int lane = 0; lane < lanes; ++lane) {
                    executor_cap->Submit([issue_fetch, get_eq_cap, lane, lanes,
                                          depth, total_fetches_cap,
                                          max_aggregate_bytes,
                                          resolve_all, pipe_cap,
                                          next_dynamic_idx]() mutable {
                        daos_handle_t lane_eq = get_eq_cap();
                        auto completion_q =
                            std::make_shared<AggCompletionQueue>();
                        std::vector<std::shared_ptr<AggBatchCtx>> slots;
                        slots.reserve(static_cast<size_t>(depth));
                        for (int s = 0; s < depth; ++s) {
                            auto ctx = std::make_shared<AggBatchCtx>();
                            ctx->slot_id = s;
                            ctx->completion_q = completion_q.get();
                            ctx->aggregate_buf.resize(max_aggregate_bytes);
                            slots.push_back(std::move(ctx));
                        }

                        int active = 0;
                        for (int s = 0; s < depth; ++s) {
                            int seed_idx = lane + s * lanes;
                            if (seed_idx >= pipe_cap ||
                                seed_idx >= total_fetches_cap)
                                continue;
                            if (issue_fetch(*slots.at(static_cast<size_t>(s)),
                                            seed_idx, lane_eq,
                                            completion_q.get())) {
                                active++;
                            }
                        }
                        auto refill_slot = [&](AggBatchCtx &ctx) -> bool {
                            for (;;) {
                                int idx = next_dynamic_idx->fetch_add(
                                    1, std::memory_order_relaxed);
                                if (idx >= total_fetches_cap)
                                    return false;
                                if (issue_fetch(ctx, idx, lane_eq,
                                                completion_q.get()))
                                    return true;
                            }
                        };

                        for (;;) {
                            if (next_dynamic_idx->load(
                                    std::memory_order_acquire) >=
                                    total_fetches_cap &&
                                active == 0)
                                break;
                            int slot_id = -1;
                            {
                                std::unique_lock<std::mutex> lk(completion_q->mtx);
                                completion_q->cv.wait(lk, [&] {
                                    return !completion_q->completed_slots.empty();
                                });
                                slot_id = completion_q->completed_slots.front();
                                completion_q->completed_slots.pop_front();
                            }

                            auto& ctx = *slots.at(static_cast<size_t>(slot_id));
                            int err = 0;
                            {
                                std::lock_guard<std::mutex> lk(ctx.waiter.mtx);
                                err = ctx.waiter.err;
                            }

                            daos_event_fini(&ctx.ev);
                            if (ctx.close_oh_on_cleanup &&
                                !daos_handle_is_inval(ctx.oh)) {
                                daos_obj_close(ctx.oh, nullptr);
                                ctx.oh = DAOS_HDL_INVAL;
                            }
                            NIXL_OBJ_US_R("split_lwagg_server_cleanup_done",
                                          ctx.batch_id);
                            active--;
                            resolve_all(err == 0 ? NIXL_SUCCESS : NIXL_ERR_BACKEND);

                            if (refill_slot(ctx))
                                active++;
                        }
                    });
                }
            },
            batch_id);

        NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
        return NIXL_IN_PROG;
    }

    // ── lw-Agg client emulation dry-run path ───────────────────────────
    // Keep the LMCache/NIXL/RGW control plane unchanged: the batched HEAD
    // still carries the real LMCache object keys. After success, map each
    // key to a pre-registered chunkwise DAOS OID and range-fetch one slice
    // per layer into the caller's destination chunk buffer.
    if (daos_lwagg_client_emulate_ && hashoid_on && batch_mode_ &&
        operation == NIXL_READ && local.descCount() > 0) {
        if (lwagg_manifest_.empty()) return NIXL_ERR_INVALID_PARAM;

        struct LwAggEmulOp {
            PendingBatchOp op;
            size_t manifest_idx;
        };

        auto ops_shared = std::make_shared<std::vector<LwAggEmulOp>>();
        ops_shared->reserve(local.descCount());
        std::vector<std::string> keys;
        keys.reserve(local.descCount());

        for (int i = 0; i < local.descCount(); ++i) {
            const auto &ld = local[i];
            const auto &rd = remote[i];
            auto it = devIdToObjKey_.find(rd.devId);
            if (it == devIdToObjKey_.end()) return NIXL_ERR_INVALID_PARAM;

            size_t manifest_idx = lwagg_manifest_start_ + static_cast<size_t>(i);
            auto mit = lwagg_manifest_by_key_.find(it->second);
            if (mit != lwagg_manifest_by_key_.end())
                manifest_idx = mit->second;
            if (manifest_idx >= lwagg_manifest_.size())
                return NIXL_ERR_INVALID_PARAM;

            auto promise = std::make_shared<std::promise<nixl_status_t>>();
            req_h->addFuture(i, promise->get_future());
            uint64_t req_id = (batch_id << 16) | static_cast<uint16_t>(i);

            ops_shared->push_back(LwAggEmulOp{
                PendingBatchOp{it->second, ld.addr, ld.len, rd.addr, promise, req_id},
                manifest_idx
            });
            keys.push_back(it->second);
        }

        daos_handle_t coh_cap = coh_local;
        int num_layers_cap = lwagg_num_layers_;
        size_t layer_bytes_cap = lwagg_layer_bytes_;
        auto manifest_cap = lwagg_manifest_;
        auto executor_cap = executor;
        const size_t obj_size = ops_shared->front().op.data_len;

        NIXL_OBJ_US_R("split_lwagg_http_start", batch_id);
        s3_client_->headBatchControlAsync(
            keys, obj_size, server_aggregate_size_,
            [ops_shared, manifest_cap, coh_cap, num_layers_cap, layer_bytes_cap,
             executor_cap, batch_id](bool http_ok) mutable {
                NIXL_OBJ_US_R("split_lwagg_http_done", batch_id);
                if (!http_ok) {
                    for (auto &entry : *ops_shared)
                        entry.op.promise->set_value(NIXL_ERR_BACKEND);
                    return;
                }

                for (const auto &entry : *ops_shared) {
                    executor_cap->Submit(
                        [entry, manifest_cap, coh_cap, num_layers_cap,
                         layer_bytes_cap]() mutable {
                            const PendingBatchOp &op = entry.op;
                            if (entry.manifest_idx >= manifest_cap.size()) {
                                op.promise->set_value(NIXL_ERR_BACKEND);
                                return;
                            }
                            size_t effective_layer_bytes = layer_bytes_cap;
                            if (effective_layer_bytes == 0) {
                                if (num_layers_cap <= 0 ||
                                    op.data_len % static_cast<size_t>(num_layers_cap) != 0) {
                                    op.promise->set_value(NIXL_ERR_BACKEND);
                                    return;
                                }
                                effective_layer_bytes =
                                    op.data_len / static_cast<size_t>(num_layers_cap);
                            }
                            const auto &manifest_entry = manifest_cap[entry.manifest_idx];
                            int rc = do_lwagg_manifest_range_fetch(
                                coh_cap,
                                manifest_entry.oid_lo,
                                manifest_entry.oid_hi_user,
                                op.data_ptr,
                                op.data_len,
                                num_layers_cap,
                                effective_layer_bytes,
                                op.req_id);
                            op.promise->set_value(rc == 0
                                ? NIXL_SUCCESS
                                : NIXL_ERR_BACKEND);
                        });
                }
            },
            batch_id);

        NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
        return NIXL_IN_PROG;
    }

    // ── Aggregate mode (s3rdma_agg) ─────────────────────────────────────
    // Same accumulator as s3rdma_batch but instead of N independent
    // daos_obj_fetch ops, ONE async fetch on the layer's aggregate OID
    // covers the entire batch via server-side aggregation. The server
    // synthesizes chunk_oid_i.lo = agg_oid.lo + i (sequential within
    // layer) and stitches the result into a single bulk transfer. We
    // pass the recipe via daos_set_nixl_agg_recipe (TLS) on the
    // dispatching thread, then immediately clear it.
    bool          daos_agg_on            = daos_agg_mode_;
    int           agg_chunks_per_layer   = agg_chunks_per_layer_;
    if (hashoid_on && batch_mode_ && daos_agg_on &&
        operation == NIXL_READ && local.descCount() > 0) {
        if (!s3_client_) return NIXL_ERR_BACKEND;
        if (agg_chunks_per_layer <= 0) return NIXL_ERR_INVALID_PARAM;

        // Build load_keys from the registered key pool (size = batch_size_),
        // mirroring the legacy batch path. nixlbench delivers descCount=1
        // per postXfer in batch_mode, so we cannot pull keys from local —
        // instead we walk a sliding window over devIdToObjKey_ that
        // covers batch_size_ keys per postXfer (= one logical load worth).
        std::vector<std::string> all_keys;
        all_keys.reserve(devIdToObjKey_.size());
        for (const auto& [devId, key] : devIdToObjKey_)
            all_keys.push_back(key);
        std::sort(all_keys.begin(), all_keys.end());
        if (all_keys.empty()) return NIXL_ERR_INVALID_PARAM;

        const size_t pool_size = all_keys.size();
        const size_t N = std::min(static_cast<size_t>(batch_size_), pool_size);
        if (N % (size_t)agg_chunks_per_layer != 0) {
            std::cerr << "[s3rdma_agg] batch_size=" << N
                      << " not divisible by agg_chunks_per_layer="
                      << agg_chunks_per_layer << std::endl;
            return NIXL_ERR_INVALID_PARAM;
        }

        const size_t object_size = local[0].len;
        const uintptr_t buf_base = batch_buf_addr_ ? batch_buf_addr_ : local[0].addr;

        // ONE promise covers the whole load (same convention as the legacy
        // batch path). It resolves when ALL N agg fetches complete.
        auto promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->addFuture(0, promise->get_future());

        // Build ops_shared from the load_keys window with synthetic per-
        // chunk addresses at buf_base + i*object_size.
        auto ops_shared = std::make_shared<std::vector<PendingBatchOp>>();
        ops_shared->reserve(N);
        std::vector<std::string> keys;
        keys.reserve(N);
        for (size_t i = 0; i < N; ++i) {
            const std::string &key = all_keys[(batch_window_offset_ + i) % pool_size];
            ops_shared->push_back(PendingBatchOp{
                key,
                buf_base + i * object_size,
                object_size,
                0,            // offset within the chunk OID (always 0 for hashoid)
                nullptr,      // promise — not used per-chunk; coordinator below
                (batch_id << 16) | static_cast<uint16_t>(i & 0xFFFF)
            });
            keys.push_back(key);
        }
        batch_window_offset_ = (batch_window_offset_ + N) % pool_size;

        const size_t chunk_bs = object_size;
        const int    num_groups = (int)N / agg_chunks_per_layer;

        daos_handle_t coh_cap      = coh_local;
        int           T_cap        = hashoid_T;
        int           IOD_cap      = hashoid_IOD;
        int           N_cap        = agg_chunks_per_layer;
        int           layer_chunks = chunks_per_layer_ > 0
                                       ? chunks_per_layer_
                                       : agg_chunks_per_layer;
        auto          get_eq_cap   = get_eq;
        auto          executor_cap = executor;
        int           inflight_cap = batch_inflight_cap_;
        int           lanes_cap    = std::max(1, num_threads_batch_);
        int           depth_cap    = iodepth_batch_ > 0
                                      ? iodepth_batch_
                                      : std::max(1, (inflight_cap + lanes_cap - 1) /
                                                     lanes_cap);

        // Coordinator: one promise resolves when ALL num_groups per-layer
        // agg fetches finish. First non-zero status wins; SUCCESS otherwise.
        // Emits split_agg_load_done trace event when the load completes,
        // for clean per-load wall-time stats.
        auto remaining = std::make_shared<std::atomic<int>>(num_groups);
        auto first_err = std::make_shared<std::atomic<int>>(0); // 0 = success
        auto resolve_one = [promise, remaining, first_err, batch_id](nixl_status_t s) {
            if (s != NIXL_SUCCESS) {
                int expected = 0;
                first_err->compare_exchange_strong(expected, (int)s);
            }
            if (remaining->fetch_sub(1) == 1) {
                int err = first_err->load();
                NIXL_OBJ_US_R("split_agg_load_done", batch_id);
                promise->set_value(err == 0 ? NIXL_SUCCESS
                                            : (nixl_status_t)err);
            }
        };

        // ONE HTTP HEAD-batch covering all keys of this logical load. After
        // it returns, run the same DAOS lane/depth refill scheduler as
        // s3rdma_batch, except each issued op is an aggregate fetch over
        // N_cap chunks instead of one independent object.
        s3_client_->headBatchControlAsync(
            keys, chunk_bs, server_aggregate_size_,
            [ops_shared, num_groups, N_cap, layer_chunks, chunk_bs, T_cap, IOD_cap,
             coh_cap, get_eq_cap, executor_cap, batch_id,
             resolve_one, inflight_cap, lanes_cap, depth_cap](bool http_ok) mutable {
                NIXL_OBJ_US_R("split_agg_http_done", batch_id);
                if (!http_ok) {
                    for (int g = 0; g < num_groups; ++g)
                        resolve_one(NIXL_ERR_BACKEND);
                    return;
                }

                auto next_group = std::make_shared<std::atomic<int>>(0);
                auto issue_fetch =
                    [ops_shared, next_group, num_groups, N_cap, layer_chunks,
                     chunk_bs, T_cap, IOD_cap, coh_cap, get_eq_cap, resolve_one,
                     batch_id](std::shared_ptr<AggBatchCtx>& out) mutable -> bool {
                    out.reset();
                    while (true) {
                        int g = next_group->fetch_add(1, std::memory_order_relaxed);
                        if (g >= num_groups) return false;

                        uint64_t trace_req_id =
                            (batch_id << 16) | static_cast<uint16_t>(g & 0xFFFF);
                        auto ctx = std::make_shared<AggBatchCtx>();
                        ctx->batch_id = trace_req_id;
                        ctx->close_oh_on_cleanup = true;

                        auto group_begin = ops_shared->begin() + (size_t)g * N_cap;
                        auto group_end = ops_shared->begin() + (size_t)(g + 1) * N_cap;
                        if (group_begin == group_end) {
                            resolve_one(NIXL_ERR_BACKEND);
                            continue;
                        }

                        // Layer index = first key's trailing int / chunks_per_layer.
                        // Must use layer_chunks (= ISL/T) — NOT N_cap (agg unit) —
                        // so partial-layer agg (N_cap < layer_chunks) maps multiple
                        // groups within a layer to the SAME layer_idx.
                        // chunk_off_in_layer = position of this group within its
                        // layer (in chunks); used to compute recx.rx_idx so the
                        // engine reads the correct sub-range of the layer's agg OID.
                        int layer_idx = 0;
                        int chunk_off_in_layer = 0;
                        {
                            const std::string &first_key = group_begin->key;
                            size_t p_us = first_key.find_last_of('_');
                            if (p_us != std::string::npos) {
                                int prepop_idx = std::atoi(first_key.c_str() + p_us + 1);
                                layer_idx = prepop_idx / layer_chunks;
                                chunk_off_in_layer = prepop_idx % layer_chunks;
                            }
                        }
                        daos_obj_id_t agg_oid;
                        agg_oid.lo = hashoid_layer_base(layer_idx);
                        agg_oid.hi = hashoid_oid_hi_user(T_cap, IOD_cap, chunk_bs);
                        int rc = daos_obj_generate_oid(coh_cap, &agg_oid,
                                    DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
                        if (rc != 0) { resolve_one(NIXL_ERR_BACKEND); continue; }
                        rc = daos_obj_open(coh_cap, agg_oid, DAOS_OO_RO, &ctx->oh, nullptr);
                        if (rc != 0) { resolve_one(NIXL_ERR_BACKEND); continue; }
                        d_iov_set(&ctx->dkey, (void*)kSplitHashoidDkey,
                                  sizeof(kSplitHashoidDkey) - 1);
                        ctx->recx.rx_idx = (uint64_t)chunk_off_in_layer * chunk_bs;
                        ctx->recx.rx_nr  = (uint64_t)N_cap * chunk_bs;
                        ctx->iod = {};
                        d_iov_set(&ctx->iod.iod_name, (void*)kSplitHashoidAkey,
                                  sizeof(kSplitHashoidAkey) - 1);
                        ctx->iod.iod_type  = DAOS_IOD_ARRAY;
                        ctx->iod.iod_size  = 1;
                        ctx->iod.iod_nr    = 1;
                        ctx->iod.iod_recxs = &ctx->recx;
                        ctx->iovs.resize(static_cast<size_t>(N_cap));
                        size_t k = 0;
                        for (auto it = group_begin; it != group_end; ++it, ++k) {
                            d_iov_set(&ctx->iovs[k],
                                      reinterpret_cast<void*>(it->data_ptr),
                                      it->data_len);
                        }
                        ctx->sgl = {};
                        ctx->sgl.sg_nr     = ctx->iovs.size();
                        ctx->sgl.sg_nr_out = ctx->iovs.size();
                        ctx->sgl.sg_iovs   = ctx->iovs.data();

                        rc = daos_event_init(&ctx->ev, get_eq_cap(), nullptr);
                        if (rc != 0) {
                            daos_obj_close(ctx->oh, nullptr);
                            resolve_one(NIXL_ERR_BACKEND);
                            continue;
                        }
                        daos_event_register_comp_cb(&ctx->ev,
                            agg_batch_completion_cb, ctx.get());

                        daos_set_nixl_agg_recipe((uint32_t)N_cap, (uint32_t)chunk_bs,
                                                  (uint32_t)T_cap, (uint32_t)IOD_cap);
                        daos_set_nixl_req_id(trace_req_id);
                        NIXL_OBJ_US_R("split_s3agg_fetch_pre", trace_req_id);
                        rc = daos_obj_fetch(ctx->oh, DAOS_TX_NONE, 0, &ctx->dkey, 1,
                                            &ctx->iod, &ctx->sgl, nullptr, &ctx->ev);
                        NIXL_OBJ_US_R("split_s3agg_fetch_post", trace_req_id);
                        daos_set_nixl_req_id(0);
                        daos_set_nixl_agg_recipe(0, 0, 0, 0);
                        if (rc != 0) {
                            daos_event_fini(&ctx->ev);
                            daos_obj_close(ctx->oh, nullptr);
                            resolve_one(NIXL_ERR_BACKEND);
                            continue;
                        }

                        out = std::move(ctx);
                        return true;
                    }
                };

                int lanes = std::max(1, lanes_cap);
                int depth = std::max(1, depth_cap);
                int max_lanes_by_cap = std::max(1, inflight_cap / depth);
                lanes = std::min(lanes, max_lanes_by_cap);

                for (int lane = 0; lane < lanes; ++lane) {
                    executor_cap->Submit([issue_fetch, resolve_one, depth]() mutable {
                        std::vector<std::shared_ptr<AggBatchCtx>> slots;
                        slots.reserve(depth);

                        for (int s = 0; s < depth; ++s) {
                            std::shared_ptr<AggBatchCtx> ctx;
                            if (issue_fetch(ctx)) slots.push_back(std::move(ctx));
                        }

                        while (!slots.empty()) {
                            bool reaped_any = false;
                            for (auto it = slots.begin(); it != slots.end();) {
                                auto& ctx = *it;
                                bool done = false;
                                {
                                    std::lock_guard<std::mutex> lk(ctx->waiter.mtx);
                                    done = ctx->waiter.done;
                                }
                                if (!done) {
                                    ++it;
                                    continue;
                                }

                                daos_event_fini(&ctx->ev);
                                if (ctx->close_oh_on_cleanup)
                                    daos_obj_close(ctx->oh, nullptr);
                                nixl_status_t status = (ctx->waiter.err == 0)
                                    ? NIXL_SUCCESS : NIXL_ERR_BACKEND;
                                resolve_one(status);

                                std::shared_ptr<AggBatchCtx> next;
                                if (issue_fetch(next)) {
                                    *it = std::move(next);
                                    ++it;
                                } else {
                                    it = slots.erase(it);
                                }
                                reaped_any = true;
                            }

                            if (!reaped_any && !slots.empty()) {
                                std::unique_lock<std::mutex> lk(
                                    slots.front()->waiter.mtx);
                                slots.front()->waiter.cv.wait_for(
                                    lk, std::chrono::microseconds(50),
                                    [&]{ return slots.front()->waiter.done; });
                            }
                        }
                    });
                }
            }, batch_id);

        NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
        return NIXL_IN_PROG;
    }

    // Batch mode (s3rdma_batch) — DISABLED experimental hashoid-pipeline
    // path that assumed nixlbench's batch_mode posts batch_size descriptors
    // per postXfer. In reality nixlbench posts 1 descriptor per postXfer,
    // so this branch never accumulated enough descriptors to fire its HTTP.
    // The legacy `if (batch_mode_ && ...)` block below already handles
    // s3rdma_batch correctly for both hashoid and non-hashoid: per
    // postXfer it builds load_keys of size batch_size_ from the registered
    // pool, fires 1 HTTP HEAD-batch, then submits N do_hashoid_op (or
    // do_libdfs_op) tasks to executor_ — pool size = num_threads_batch
    // becomes the natural cap on simultaneous in-flight RPCs.
    if (lmcache_batch_mode_ && hashoid_on && batch_mode_ &&
        operation == NIXL_READ && local.descCount() > 0) {
        if (!s3_client_) return NIXL_ERR_BACKEND;

        // ── Each postXfer = ONE batched HTTP for whatever descriptors it
        //    carries. nixlbench's batch_mode posts `batch_size` descriptors
        //    per postXfer (= one logical KV load), so the wire shape is
        //    exactly what s3rdma_batch's design intent says: one HTTP
        //    HEAD-batch + N daos_obj_fetch (throttled by executor pool
        //    size = num_threads_batch). The earlier `pending_reads_`
        //    accumulation was wrong: it required `batch_size` postXfers
        //    each carrying 1 desc to fire one HTTP, which never happens
        //    in our nixlbench config.
        auto ops_shared = std::make_shared<std::vector<PendingBatchOp>>();
        ops_shared->reserve(local.descCount());
        for (int i = 0; i < local.descCount(); ++i) {
            const auto &ld = local[i];
            const auto &rd = remote[i];
            auto it = devIdToObjKey_.find(rd.devId);
            if (it == devIdToObjKey_.end()) return NIXL_ERR_INVALID_PARAM;

            auto promise = std::make_shared<std::promise<nixl_status_t>>();
            req_h->addFuture(i, promise->get_future());
            uint64_t req_id = (batch_id << 16) | static_cast<uint16_t>(i);

            ops_shared->push_back(PendingBatchOp{
                it->second, ld.addr, ld.len, rd.addr, promise, req_id
            });
        }

        if (!ops_shared->empty()) {
            std::vector<std::string> keys;
            keys.reserve(ops_shared->size());
            for (auto& op : *ops_shared) keys.push_back(op.key);
            const size_t obj_size = ops_shared->front().data_len;

            // Capture coh_local + hashoid params for the async fan-out.
            daos_handle_t coh_cap      = coh_local;
            int           T_cap        = hashoid_T;
            int           IOD_cap      = hashoid_IOD;
            int           inflight_cap = batch_inflight_cap_;
            int           lanes_cap    = std::max(1, num_threads_batch_);
            int           depth_cap    = iodepth_batch_ > 0
                                           ? iodepth_batch_
                                           : std::max(1, (inflight_cap + lanes_cap - 1) /
                                                          lanes_cap);
            auto          get_eq_cap   = get_eq;
            bool          agg_patch_cap = daos_agg_patch_mode_;
            bool          agg_patch_lwagg_manifest_cap =
                daos_agg_patch_lwagg_manifest_;
            bool          agg_patch_rangeget_cap = daos_agg_patch_rangeget_;
            int           agg_patch_chunks_cap =
                std::max(1, agg_chunks_per_layer_);
            // Pointer to the (immutable post-init) override map; nullptr if
            // no manifest was loaded → async lambdas fall through to the
            // splitmix(fnv1a(key)) path. The engine outlives any in-flight
            // async ops, so the const pointer is safe to capture.
            const auto *oid_override_ptr_cap =
                s3rdma_batch_oid_override_by_key_.empty()
                    ? nullptr
                    : &s3rdma_batch_oid_override_by_key_;

            {  // open scope so existing closure block stays at right indent
                auto executor_cap = executor;
                NIXL_OBJ_US_R("split_batch_http_start", batch_id);
                s3_client_->headBatchControlAsync(
                    keys, obj_size, server_aggregate_size_,
                    [ops_shared, coh_cap, T_cap, IOD_cap, get_eq_cap,
                     executor_cap, batch_id, inflight_cap, lanes_cap,
                     depth_cap, oid_override_ptr_cap, agg_patch_cap,
                     agg_patch_lwagg_manifest_cap,
                     agg_patch_rangeget_cap,
                     agg_patch_chunks_cap](bool http_ok) mutable {
                        NIXL_OBJ_US_R("split_batch_http_done", batch_id);
                        if (!http_ok) {
                            for (auto& op : *ops_shared)
                                op.promise->set_value(NIXL_ERR_BACKEND);
                            return;
                        }
                        // Async DAOS data plane for s3rdma_batch. After the
                        // single HTTP HEAD-batch gate, run the same shape as
                        // client NIXL-DAOS RDMA: num_threads_batch lanes,
                        // each owning iodepth_batch async DAOS fetch slots.
                        // The lanes pull from one shared object list, but each
                        // lane refills only its own slots on completion.
                        auto next_idx = std::make_shared<std::atomic<size_t>>(0);
                        auto issue_fetch =
                            [ops_shared, next_idx, coh_cap, T_cap, IOD_cap,
                             get_eq_cap, oid_override_ptr_cap, agg_patch_cap,
                             agg_patch_lwagg_manifest_cap,
                             agg_patch_rangeget_cap,
                             agg_patch_chunks_cap]
                            (std::shared_ptr<HashoidAsyncFetchCtx>& out)
                                mutable -> bool {
                            out.reset();
                            while (true) {
                                size_t idx = next_idx->fetch_add(
                                    1, std::memory_order_relaxed);
                                if (idx >= ops_shared->size()) return false;

                                const PendingBatchOp op = (*ops_shared)[idx];
                                auto ctx = std::make_shared<HashoidAsyncFetchCtx>();
                                ctx->promise = op.promise;

                                NIXL_OBJ_US_R("split_hashoid_open_pre", op.req_id);
                                daos_obj_id_t oid;
                                bool oid_overridden = false;
                                size_t chunk_start =
                                    idx * static_cast<size_t>(agg_patch_chunks_cap);
                                if (agg_patch_cap) {
                                    const size_t chunk_count =
                                        static_cast<size_t>(agg_patch_chunks_cap);
                                    if (chunk_count == 0 ||
                                        op.data_len % chunk_count != 0) {
                                        op.promise->set_value(NIXL_ERR_INVALID_PARAM);
                                        continue;
                                    }
                                    oid.lo = hashoid_layer_base(0) + chunk_start;
                                    const size_t source_object_len =
                                        agg_patch_rangeget_cap
                                            ? op.data_len
                                            : op.data_len / chunk_count;
                                    oid.hi = hashoid_oid_hi_user(
                                        T_cap, IOD_cap, source_object_len);
                                    oid_overridden = true;
                                } else if (oid_override_ptr_cap != nullptr) {
                                    auto it = oid_override_ptr_cap->find(op.key);
                                    if (it != oid_override_ptr_cap->end()) {
                                        oid.lo = it->second.first;
                                        oid.hi = it->second.second;
                                        oid_overridden = true;
                                    }
                                }
                                if (!oid_overridden) {
                                    oid.lo = hashoid_oid_lo(static_cast<int>(
                                        split_hashoid_fnv1a_32(op.key.data(),
                                                               op.key.size())));
                                    oid.hi = hashoid_oid_hi_user(
                                        T_cap, IOD_cap, op.data_len);
                                }
                                int rc = daos_obj_generate_oid(
                                    coh_cap, &oid, DAOS_OT_MULTI_HASHED,
                                    OC_SX, 0, 0);
                                if (rc != 0) {
                                    NIXL_OBJ_US_R("split_hashoid_genoid_failed",
                                                  op.req_id);
                                    op.promise->set_value(NIXL_ERR_BACKEND);
                                    continue;
                                }

                                rc = daos_obj_open(coh_cap, oid, DAOS_OO_RO,
                                                   &ctx->oh, nullptr);
                                NIXL_OBJ_US_R("split_hashoid_open_post",
                                              op.req_id);
                                if (rc != 0) {
                                    op.promise->set_value(NIXL_ERR_BACKEND);
                                    continue;
                                }

                                d_iov_set(&ctx->dkey,
                                          (void*)kSplitHashoidDkey,
                                          sizeof(kSplitHashoidDkey) - 1);
                                ctx->recx.rx_idx = agg_patch_cap ? 0 : op.offset;
                                ctx->recx.rx_nr  = op.data_len;
                                ctx->iod = {};
                                d_iov_set(&ctx->iod.iod_name,
                                          (void*)kSplitHashoidAkey,
                                          sizeof(kSplitHashoidAkey) - 1);
                                ctx->iod.iod_type  = DAOS_IOD_ARRAY;
                                ctx->iod.iod_size  = 1;
                                ctx->iod.iod_nr    = 1;
                                ctx->iod.iod_recxs = &ctx->recx;

                                d_iov_set(&ctx->iov,
                                          reinterpret_cast<void*>(op.data_ptr),
                                          op.data_len);
                                ctx->sgl = {};
                                ctx->sgl.sg_nr     = 1;
                                ctx->sgl.sg_iovs   = &ctx->iov;
                                ctx->sgl.sg_nr_out = 1;

                                rc = daos_event_init(&ctx->ev, get_eq_cap(),
                                                     nullptr);
                                if (rc != 0) {
                                    daos_obj_close(ctx->oh, nullptr);
                                    op.promise->set_value(NIXL_ERR_BACKEND);
                                    continue;
                                }
                                daos_event_register_comp_cb(
                                    &ctx->ev,
                                    hashoid_async_fetch_completion_cb,
                                    ctx.get());

                                daos_set_nixl_req_id(op.req_id);
                                NIXL_OBJ_US_R("split_hashoid_issue",
                                              op.req_id);
                                if (agg_patch_cap) {
                                    const uint32_t chunk_count =
                                        static_cast<uint32_t>(agg_patch_chunks_cap);
                                    const uint32_t chunk_bytes =
                                        static_cast<uint32_t>(op.data_len /
                                            static_cast<size_t>(agg_patch_chunks_cap));
                                    daos_obj_fetch_agg_param_t agg = {};
                                    agg.af_chunk_count = chunk_count;
                                    agg.af_chunk_size = chunk_bytes;
                                    agg.af_recipe_hi =
                                        agg_patch_lwagg_manifest_cap
                                            ? (0x80000000u)
                                            : static_cast<uint32_t>(T_cap);
                                    agg.af_recipe_lo =
                                        agg_patch_lwagg_manifest_cap
                                            ? static_cast<uint32_t>(chunk_start)
                                            : static_cast<uint32_t>(IOD_cap);
                                    rc = daos_obj_fetch_agg(
                                        ctx->oh, DAOS_TX_NONE, 0,
                                        &ctx->dkey, 1, &ctx->iod,
                                        &ctx->sgl, nullptr, &ctx->ev, &agg);
                                } else {
                                    rc = daos_obj_fetch(ctx->oh, DAOS_TX_NONE, 0,
                                                        &ctx->dkey, 1, &ctx->iod,
                                                        &ctx->sgl, nullptr,
                                                        &ctx->ev);
                                }
                                NIXL_OBJ_US_R("split_hashoid_issued",
                                              op.req_id);
                                daos_set_nixl_req_id(0);
                                if (rc != 0) {
                                    daos_event_fini(&ctx->ev);
                                    daos_obj_close(ctx->oh, nullptr);
                                    op.promise->set_value(NIXL_ERR_BACKEND);
                                    continue;
                                }

                                out = std::move(ctx);
                                return true;
                            }
                        };

                        int lanes = std::max(1, lanes_cap);
                        int depth = std::max(1, depth_cap);
                        int max_lanes_by_cap = std::max(1, inflight_cap / depth);
                        lanes = std::min(lanes, max_lanes_by_cap);

                        for (int lane = 0; lane < lanes; ++lane) {
                            executor_cap->Submit([issue_fetch, depth]() mutable {
                                std::vector<std::shared_ptr<HashoidAsyncFetchCtx>> slots;
                                slots.reserve(depth);

                                for (int s = 0; s < depth; ++s) {
                                    std::shared_ptr<HashoidAsyncFetchCtx> ctx;
                                    if (issue_fetch(ctx)) slots.push_back(std::move(ctx));
                                }

                                while (!slots.empty()) {
                                    bool reaped_any = false;
                                    for (auto it = slots.begin(); it != slots.end();) {
                                        auto& ctx = *it;
                                        bool done = false;
                                        {
                                            std::lock_guard<std::mutex> lk(
                                                ctx->waiter.mtx);
                                            done = ctx->waiter.done;
                                        }
                                        if (!done) {
                                            ++it;
                                            continue;
                                        }

                                        daos_event_fini(&ctx->ev);
                                        daos_obj_close(ctx->oh, nullptr);
                                        ctx->promise->set_value(
                                            ctx->waiter.err == 0
                                                ? NIXL_SUCCESS
                                                : NIXL_ERR_BACKEND);

                                        std::shared_ptr<HashoidAsyncFetchCtx> next;
                                        if (issue_fetch(next)) {
                                            *it = std::move(next);
                                            ++it;
                                        } else {
                                            it = slots.erase(it);
                                        }
                                        reaped_any = true;
                                    }

                                    if (!reaped_any && !slots.empty()) {
                                        std::unique_lock<std::mutex> lk(
                                            slots.front()->waiter.mtx);
                                        slots.front()->waiter.cv.wait_for(
                                            lk, std::chrono::microseconds(50),
                                            [&]{ return slots.front()->waiter.done; });
                                    }
                                }
                            });
                        }
                    },
                    batch_id);
            }
        }

        NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
        return NIXL_IN_PROG;
    }

    // Legacy batch mode (non-hashoid fallback): sliding-window + wait-for-all
    // barrier. Kept for the dfs_* path; hashoid uses the pipeline above.
    if (batch_mode_ && operation == NIXL_READ && local.descCount() > 0) {
        if (!s3_client_) return NIXL_ERR_BACKEND;

        // Build sliding-window key list (size N = batch_size_).
        std::vector<std::string> all_keys;
        all_keys.reserve(devIdToObjKey_.size());
        for (const auto& [devId, key] : devIdToObjKey_)
            all_keys.push_back(key);
        std::sort(all_keys.begin(), all_keys.end());
        if (all_keys.empty()) return NIXL_ERR_INVALID_PARAM;

        size_t pool_size = all_keys.size();
        size_t N = std::min(static_cast<size_t>(batch_size_), pool_size);
        std::vector<std::string> load_keys;
        load_keys.reserve(N);
        for (size_t i = 0; i < N; i++)
            load_keys.push_back(all_keys[(batch_window_offset_ + i) % pool_size]);
        batch_window_offset_ = (batch_window_offset_ + N) % pool_size;

        // Buffer layout: N contiguous slots of object_size bytes inside
        // the registered DRAM/VRAM region. The local descriptor only
        // carries one object_size, so we use the registerMem-captured
        // base to address all N slots.
        size_t    object_size = local[0].len;
        uintptr_t buf_base    = batch_buf_addr_ ? batch_buf_addr_ : local[0].addr;
        if (batch_buf_len_ && N * object_size > batch_buf_len_) {
            // Cap N to fit into the registered region.
            N = batch_buf_len_ / object_size;
            load_keys.resize(N);
        }

        // One promise covers the entire batch — its value is set after
        // all N dfs_reads complete (or the HTTP control hop fails).
        auto promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->addFuture(0, promise->get_future());

        // Capture state needed by the HTTP callback + the fan-out tasks.
        const int server_agg = server_aggregate_size_;
        int       inflight_cap = batch_inflight_cap_;
        int       lanes_cap    = std::max(1, num_threads_batch_);
        int       depth_cap    = iodepth_batch_ > 0
                                   ? iodepth_batch_
                                   : std::max(1, (inflight_cap + lanes_cap - 1) /
                                                  lanes_cap);

        // Compute override pointer in the outer scope (where `this` is
        // available); capture by value into the async callback.
        const auto *oid_override_ptr_cap2 =
            s3rdma_batch_oid_override_by_key_.empty()
                ? nullptr
                : &s3rdma_batch_oid_override_by_key_;

        NIXL_OBJ_US_R("split_batch_http_start", batch_id);
        s3_client_->headBatchControlAsync(
            load_keys, object_size, server_agg,
            [dfs_local, get_eq, coh_local, hashoid_on, hashoid_T, hashoid_IOD,
             executor, load_keys, buf_base, object_size,
             promise, batch_id, inflight_cap, lanes_cap,
             depth_cap, oid_override_ptr_cap2](bool http_ok) mutable {
                NIXL_OBJ_US_R("split_batch_http_done", batch_id);
                if (!http_ok) {
                    promise->set_value(NIXL_ERR_BACKEND);
                    return;
                }
                // Fan out N dfs_read tasks; coordinator waits for all.
                size_t N = load_keys.size();
                auto remaining = std::make_shared<std::atomic<size_t>>(N);
                auto err_seen  = std::make_shared<std::atomic<bool>>(false);

                auto finish_one = [remaining, err_seen, promise](int rc) {
                    if (rc != 0) err_seen->store(true);
                    if (remaining->fetch_sub(1) == 1) {
                        promise->set_value(err_seen->load()
                            ? NIXL_ERR_BACKEND : NIXL_SUCCESS);
                    }
                };

                if (hashoid_on) {
                    // Async hashoid data path for s3rdma_batch kvbench.
                    // This mirrors client NIXL-DAOS RDMA's NT x IOD shape:
                    // num_threads_batch lanes, each keeping iodepth_batch
                    // async daos_obj_fetch calls in flight and refilling from
                    // one shared queue after completions.
                    auto next_idx = std::make_shared<std::atomic<size_t>>(0);
                    auto issue_fetch =
                        [load_keys, buf_base, object_size, next_idx, coh_local,
                         hashoid_T, hashoid_IOD, get_eq, finish_one, batch_id]
                        (std::shared_ptr<HashoidAsyncFetchCtx>& out) mutable
                            -> bool {
                        out.reset();
                        while (true) {
                            size_t idx = next_idx->fetch_add(
                                1, std::memory_order_relaxed);
                            if (idx >= load_keys.size()) return false;

                            const std::string& key = load_keys[idx];
                            uintptr_t dst = buf_base + idx * object_size;
                            uint64_t req_id =
                                (batch_id << 16) | static_cast<uint16_t>(idx);

                            NIXL_OBJ_US_R("split_dfs_start", req_id);
                            auto ctx = std::make_shared<HashoidAsyncFetchCtx>();
                            ctx->req_id = req_id;

                            NIXL_OBJ_US_R("split_hashoid_open_pre", req_id);
                            daos_obj_id_t oid;
                            oid.lo = hashoid_oid_lo(static_cast<int>(
                                split_hashoid_fnv1a_32(key.data(),
                                                       key.size())));
                            oid.hi = hashoid_oid_hi_user(
                                hashoid_T, hashoid_IOD, object_size);
                            int rc = daos_obj_generate_oid(
                                coh_local, &oid, DAOS_OT_MULTI_HASHED,
                                OC_SX, 0, 0);
                            if (rc != 0) {
                                NIXL_OBJ_US_R("split_hashoid_genoid_failed",
                                              req_id);
                                NIXL_OBJ_US_R("split_dfs_done", req_id);
                                finish_one(rc);
                                continue;
                            }

                            rc = daos_obj_open(coh_local, oid, DAOS_OO_RO,
                                               &ctx->oh, nullptr);
                            NIXL_OBJ_US_R("split_hashoid_open_post", req_id);
                            if (rc != 0) {
                                NIXL_OBJ_US_R("split_dfs_done", req_id);
                                finish_one(rc);
                                continue;
                            }

                            d_iov_set(&ctx->dkey,
                                      (void*)kSplitHashoidDkey,
                                      sizeof(kSplitHashoidDkey) - 1);
                            ctx->recx.rx_idx = 0;
                            ctx->recx.rx_nr  = object_size;
                            ctx->iod = {};
                            d_iov_set(&ctx->iod.iod_name,
                                      (void*)kSplitHashoidAkey,
                                      sizeof(kSplitHashoidAkey) - 1);
                            ctx->iod.iod_type  = DAOS_IOD_ARRAY;
                            ctx->iod.iod_size  = 1;
                            ctx->iod.iod_nr    = 1;
                            ctx->iod.iod_recxs = &ctx->recx;

                            d_iov_set(&ctx->iov,
                                      reinterpret_cast<void*>(dst),
                                      object_size);
                            ctx->sgl = {};
                            ctx->sgl.sg_nr     = 1;
                            ctx->sgl.sg_iovs   = &ctx->iov;
                            ctx->sgl.sg_nr_out = 1;

                            rc = daos_event_init(&ctx->ev, get_eq(), nullptr);
                            if (rc != 0) {
                                daos_obj_close(ctx->oh, nullptr);
                                NIXL_OBJ_US_R("split_dfs_done", req_id);
                                finish_one(rc);
                                continue;
                            }
                            daos_event_register_comp_cb(
                                &ctx->ev, hashoid_async_fetch_completion_cb,
                                ctx.get());

                            daos_set_nixl_req_id(req_id);
                            NIXL_OBJ_US_R("split_hashoid_issue", req_id);
                            rc = daos_obj_fetch(ctx->oh, DAOS_TX_NONE, 0,
                                                &ctx->dkey, 1, &ctx->iod,
                                                &ctx->sgl, nullptr, &ctx->ev);
                            NIXL_OBJ_US_R("split_hashoid_issued", req_id);
                            daos_set_nixl_req_id(0);
                            if (rc != 0) {
                                daos_event_fini(&ctx->ev);
                                daos_obj_close(ctx->oh, nullptr);
                                NIXL_OBJ_US_R("split_dfs_done", req_id);
                                finish_one(rc);
                                continue;
                            }

                            ctx->promise = nullptr;
                            out = std::move(ctx);
                            return true;
                        }
                    };

                    int lanes = std::max(1, lanes_cap);
                    int depth = std::max(1, depth_cap);
                    int max_lanes_by_cap = std::max(1, inflight_cap / depth);
                    lanes = std::min(lanes, max_lanes_by_cap);

                    for (int lane = 0; lane < lanes; ++lane) {
                        executor->Submit([issue_fetch, depth,
                                          finish_one]() mutable {
                            std::vector<std::shared_ptr<HashoidAsyncFetchCtx>> slots;
                            slots.reserve(depth);

                            for (int s = 0; s < depth; ++s) {
                                std::shared_ptr<HashoidAsyncFetchCtx> ctx;
                                if (issue_fetch(ctx))
                                    slots.push_back(std::move(ctx));
                            }

                            while (!slots.empty()) {
                                bool reaped_any = false;
                                for (auto it = slots.begin(); it != slots.end();) {
                                    auto& ctx = *it;
                                    bool done = false;
                                    {
                                        std::lock_guard<std::mutex> lk(
                                            ctx->waiter.mtx);
                                        done = ctx->waiter.done;
                                    }
                                    if (!done) {
                                        ++it;
                                        continue;
                                    }

                                    daos_event_fini(&ctx->ev);
                                    daos_obj_close(ctx->oh, nullptr);
                                    NIXL_OBJ_US_R("split_dfs_done",
                                                  ctx->req_id);
                                    finish_one(ctx->waiter.err);

                                    std::shared_ptr<HashoidAsyncFetchCtx> next;
                                    if (issue_fetch(next)) {
                                        *it = std::move(next);
                                        ++it;
                                    } else {
                                        it = slots.erase(it);
                                    }
                                    reaped_any = true;
                                }

                                if (!reaped_any && !slots.empty()) {
                                    std::unique_lock<std::mutex> lk(
                                        slots.front()->waiter.mtx);
                                    slots.front()->waiter.cv.wait_for(
                                        lk, std::chrono::microseconds(50),
                                        [&]{ return slots.front()->waiter.done; });
                                }
                            }
                        });
                    }
                    return;
                }

                for (size_t i = 0; i < N; i++) {
                    const std::string &key = load_keys[i];
                    uintptr_t dst = buf_base + i * object_size;
                    uint64_t req_id = (batch_id << 16) | static_cast<uint16_t>(i);

                    const auto *oid_override_ptr = oid_override_ptr_cap2;
                    executor->Submit([dfs_local, get_eq, coh_local, hashoid_on,
                                      hashoid_T, hashoid_IOD, key, dst,
                                      object_size, req_id, remaining, err_seen,
                                      promise, oid_override_ptr]() {
                        NIXL_OBJ_US_R("split_dfs_start", req_id);
                        int rc = hashoid_on
                            ? do_hashoid_op(coh_local, key, hashoid_T, hashoid_IOD,
                                            NIXL_READ, dst, object_size, 0, req_id,
                                            oid_override_ptr)
                            : do_libdfs_op(dfs_local, get_eq(), key,
                                           NIXL_READ, dst, object_size, 0,
                                           req_id);
                        NIXL_OBJ_US_R("split_dfs_done", req_id);
                        if (rc != 0) {
                            std::cerr << "[batch] "
                                      << (hashoid_on ? "hashoid_read" : "dfs_read")
                                      << " FAILED key=" << key
                                      << " rc=" << rc << " dst=0x" << std::hex
                                      << dst << std::dec << " len=" << object_size
                                      << std::endl;
                            err_seen->store(true);
                        }
                        if (remaining->fetch_sub(1) == 1) {
                            promise->set_value(err_seen->load()
                                ? NIXL_ERR_BACKEND : NIXL_SUCCESS);
                        }
                    });
                }
            },
            batch_id);

        NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
        return NIXL_IN_PROG;
    }

    // Normal s3rdma_direct path: HTTP control + libdfs data.
    for (int i = 0; i < local.descCount(); ++i) {
        const auto &ld = local[i];
        const auto &rd = remote[i];

        auto it = devIdToObjKey_.find(rd.devId);
        if (it == devIdToObjKey_.end()) return NIXL_ERR_INVALID_PARAM;
        const std::string key = it->second;
        uintptr_t data_ptr = ld.addr;
        size_t    data_len = ld.len;
        size_t    offset   = rd.addr;

        auto promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->addFuture(0, promise->get_future());

        uint64_t req_id = (batch_id << 16) | static_cast<uint16_t>(i);

        const auto *oid_override_ptr =
            s3rdma_batch_oid_override_by_key_.empty()
                ? nullptr
                : &s3rdma_batch_oid_override_by_key_;
        auto on_http_done = [executor, dfs_local, get_eq, coh_local, hashoid_on,
                             hashoid_T, hashoid_IOD, key, operation,
                             data_ptr, data_len, offset, promise, req_id,
                             oid_override_ptr]
                            (bool http_ok) {
            if (!http_ok) {
                NIXL_OBJ_US_R("split_http_failed", req_id);
                promise->set_value(NIXL_ERR_BACKEND);
                return;
            }
            NIXL_OBJ_US_R("split_http_ok_dispatch_dfs", req_id);
            executor->Submit([dfs_local, get_eq, coh_local, hashoid_on, hashoid_T,
                              hashoid_IOD, key, operation, data_ptr, data_len,
                              offset, promise, req_id, oid_override_ptr]() mutable {
                NIXL_OBJ_US_R("split_dfs_start", req_id);
                int rc = hashoid_on
                    ? do_hashoid_op(coh_local, key, hashoid_T, hashoid_IOD,
                                    operation, data_ptr, data_len, offset, req_id,
                                    oid_override_ptr)
                    : do_libdfs_op(dfs_local, get_eq(), key, operation,
                                   data_ptr, data_len, offset, req_id);
                NIXL_OBJ_US_R("split_dfs_done", req_id);
                promise->set_value(rc == 0 ? NIXL_SUCCESS : NIXL_ERR_BACKEND);
            });
        };

        if (operation == NIXL_WRITE) {
            s3_client_->putObjectRdmaDirectControlAsync(key, on_http_done, req_id);
        } else {
            s3_client_->headObjectRdmaDirectControlAsync(key, on_http_done, req_id);
        }
    }

    NIXL_OBJ_US_R("split_postXfer_exit", batch_id);
    return NIXL_IN_PROG;
}

nixl_status_t
S3SplitPlaneObjEngineImpl::checkXfer(nixlBackendReqH *handle) const {
    return static_cast<S3SplitReqH*>(handle)->getOverallStatus();
}

nixl_status_t
S3SplitPlaneObjEngineImpl::getXferDoneIndices(
    nixlBackendReqH *handle, std::vector<int> &done_indices) const {
    return static_cast<S3SplitReqH*>(handle)->getDoneIndices(done_indices);
}

nixl_status_t
S3SplitPlaneObjEngineImpl::releaseReqH(nixlBackendReqH *handle) const {
    delete static_cast<S3SplitReqH*>(handle);
    return NIXL_SUCCESS;
}
