/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "engine_impl.h"
#pragma GCC diagnostic pop

#include "common/nixl_log.h"
#include "s3/obj_us_trace.h"
#include "hashoid.h"  // shared with DAOS agg_sidecar (via symlink on hsc-21)

#include <aws/core/utils/threading/Executor.h>
#include <cstdlib>
#include <cstring>
#include <condition_variable>
#include <future>
#include <iostream>
#include <algorithm>
#include <thread>

// ────────────────────────────────────────────────────────────────────────────
// Metadata classes (duplicated from s3/engine_impl.cpp — file-local there)
// ────────────────────────────────────────────────────────────────────────────
class DaosDirectObjMD : public nixlBackendMD {
public:
    DaosDirectObjMD(nixl_mem_t type, uint64_t devId, std::string key)
        : nixlBackendMD(true), type(type), devId(devId), objKey(std::move(key)) {}
    nixl_mem_t type;
    uint64_t devId;
    std::string objKey;
};

class DaosDirectDramMD : public nixlBackendMD {
public:
    DaosDirectDramMD(uintptr_t addr, size_t len)
        : nixlBackendMD(true), addr(addr), len(len) {}
    uintptr_t addr;
    size_t len;
};

// ────────────────────────────────────────────────────────────────────────────
// Request handle
// ────────────────────────────────────────────────────────────────────────────
class DaosDirectReqH : public nixlBackendReqH {
public:
    std::vector<std::future<nixl_status_t>> statusFutures_;

    nixl_status_t getOverallStatus() {
        auto it = statusFutures_.begin();
        while (it != statusFutures_.end()) {
            if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                nixl_status_t s = it->get();
                if (s != NIXL_SUCCESS) { statusFutures_.clear(); return s; }
                it = statusFutures_.erase(it);
            } else {
                return NIXL_IN_PROG;
            }
        }
        return NIXL_SUCCESS;
    }
};

// ────────────────────────────────────────────────────────────────────────────
// Per-request waiter
// ────────────────────────────────────────────────────────────────────────────
struct DaosDirectWaiter {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done = false;
    int                     err  = 0;
};

static int
daos_direct_completion_cb(void *arg, daos_event_t * /*ev*/, int ret) {
    auto *w = static_cast<DaosDirectWaiter*>(arg);
    { std::lock_guard<std::mutex> lock(w->mtx); w->done = true; w->err = ret; }
    w->cv.notify_one();
    return 0;
}

static uint64_t
daos_direct_rate_limit_bps() {
    static const uint64_t rate = []() -> uint64_t {
        const char *env = std::getenv("NIXL_DAOS_DIRECT_RATE_LIMIT_BPS");
        if (!env || !*env) return 0;
        char *end = nullptr;
        unsigned long long v = std::strtoull(env, &end, 10);
        return (end && *end == '\0') ? static_cast<uint64_t>(v) : 0;
    }();
    return rate;
}

static void
daos_direct_complete_after_rate_limit(std::shared_ptr<std::promise<nixl_status_t>> promise,
                                      nixl_status_t status,
                                      size_t bytes,
                                      uint64_t req_id) {
    uint64_t rate = daos_direct_rate_limit_bps();
    if (status != NIXL_SUCCESS || rate == 0 || bytes == 0) {
        promise->set_value(status);
        return;
    }

    using clock = std::chrono::steady_clock;
    static std::mutex sched_mtx;
    static clock::time_point next_completion = clock::now();

    const auto spacing = std::chrono::nanoseconds(
        static_cast<int64_t>((static_cast<long double>(bytes) * 1000000000.0L) /
                             static_cast<long double>(rate)));

    clock::time_point target;
    {
        std::lock_guard<std::mutex> lock(sched_mtx);
        auto now = clock::now();
        if (next_completion < now) next_completion = now;
        next_completion += spacing;
        target = next_completion;
    }

    std::thread([promise = std::move(promise), target, req_id]() mutable {
        if (target > clock::now()) {
            NIXL_OBJ_US_R("rate_limit_wait_enter", req_id);
            std::this_thread::sleep_until(target);
            NIXL_OBJ_US_R("rate_limit_wait_exit", req_id);
        }
        promise->set_value(NIXL_SUCCESS);
    }).detach();
}

// FNV-1a 32-bit: maps a key string to a deterministic int passed to
// hashoid_oid_lo(). Both WRITE and READ of the same key resolve to the
// same oid.lo within a run, giving byte-addressable object identity
// without any dentry traversal.
static inline uint32_t hashoid_fnv1a_32(const std::string &s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) { h ^= c; h *= 16777619u; }
    return h;
}

// Fixed dkey/akey used by hashoid-mode daos_obj_update/fetch.
static const char kHashoidDkey[] = "d";
static const char kHashoidAkey[] = "a";

// ────────────────────────────────────────────────────────────────────────────
// EQ pool
// ────────────────────────────────────────────────────────────────────────────
void DaosDirectObjEngineImpl::startEQPool(size_t n) {
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

void DaosDirectObjEngineImpl::stopEQPool() {
    eq_run_.store(false);
    for (size_t i = 0; i < eq_pool_size_; ++i) {
        if (eq_threads_[i].joinable()) eq_threads_[i].join();
        if (eq_pool_[i].cookie != 0) daos_eq_destroy(eq_pool_[i], 0);
    }
}

daos_handle_t DaosDirectObjEngineImpl::getEQ() const {
    if (eq_pool_size_ == 0) return DAOS_HDL_INVAL;
    return eq_pool_[eq_rr_.fetch_add(1, std::memory_order_relaxed) % eq_pool_size_];
}

// ────────────────────────────────────────────────────────────────────────────
// Construction / destruction — NO AWS SDK, only DAOS
// ────────────────────────────────────────────────────────────────────────────
DaosDirectObjEngineImpl::DaosDirectObjEngineImpl(const nixlBackendInitParams *init_params) {
    auto *p = init_params->customParams;
    pool_label_ = (p && p->count("daos_pool")) ? p->at("daos_pool") : "Pool1";
    cont_label_ = (p && p->count("daos_cont")) ? p->at("daos_cont") : "nixl_direct";

    // Hashoid mode + (T, IOD) for oid.hi encoding (shared with DAOS agg_sidecar).
    if (p && p->count("daos_hashoid") && p->at("daos_hashoid") == "true")
        hashoid_mode_ = true;
    if (p && p->count("hashoid_T"))   hashoid_T_   = std::max(1, std::atoi(p->at("hashoid_T").c_str()));
    if (p && p->count("hashoid_IOD")) hashoid_IOD_ = std::max(1, std::atoi(p->at("hashoid_IOD").c_str()));

    // Thread pool for async completion dispatch (replaces the asio executor
    // from DefaultObjEngineImpl, but without the full AWS SDK init).
    size_t num_threads = 16;
    if (p && p->count("num_threads"))
        num_threads = std::max(size_t{4}, size_t(std::stoul(p->at("num_threads"))) * 2);
    executor_ = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(num_threads);

    std::cerr << "DAOS direct engine: pool=" << pool_label_
              << " cont=" << cont_label_
              << (hashoid_mode_ ? " [HASHOID mode]" : " [DFS mode]")
              << (hashoid_mode_ ? " T=" + std::to_string(hashoid_T_) +
                                  " IOD=" + std::to_string(hashoid_IOD_) : "")
              << std::endl;
    if (daos_direct_rate_limit_bps() > 0)
        std::cerr << "DAOS direct: completion rate limit "
                  << daos_direct_rate_limit_bps() << " B/s" << std::endl;

    int rc = daos_init();
    std::cerr << "DAOS direct: daos_init() returned " << rc << std::endl;
    if (rc != 0) return;

    rc = daos_pool_connect(pool_label_.c_str(), nullptr, DAOS_PC_RW, &poh_, nullptr, nullptr);
    std::cerr << "DAOS direct: daos_pool_connect returned " << rc << std::endl;
    if (rc != 0) { daos_fini(); return; }

    rc = daos_cont_open(poh_, cont_label_.c_str(), DAOS_COO_RW, &coh_, nullptr, nullptr);
    if (rc == -DER_NONEXIST) {
        std::cerr << "DAOS direct: creating container " << cont_label_ << std::endl;
        uuid_t cont_uuid;
        rc = dfs_cont_create_with_label(poh_, cont_label_.c_str(), nullptr,
                                        &cont_uuid, &coh_, &dfs_);
    } else if (rc == 0) {
        rc = dfs_mount(poh_, coh_, O_RDWR, &dfs_);
    }
    if (rc != 0 || !dfs_) {
        std::cerr << "DAOS direct: container/mount failed: " << rc << std::endl;
        if (coh_.cookie != 0) daos_cont_close(coh_, nullptr);
        daos_pool_disconnect(poh_, nullptr);
        daos_fini();
        return;
    }
    std::cerr << "DAOS direct: DFS mounted on " << cont_label_ << std::endl;

    const char *eq_env = std::getenv("NIXL_DAOS_EQ_POOL");
    startEQPool(eq_env ? static_cast<size_t>(std::atoi(eq_env)) : 8);
}

DaosDirectObjEngineImpl::~DaosDirectObjEngineImpl() {
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
DaosDirectObjEngineImpl::registerMem(const nixlBlobDesc &mem,
                                     const nixl_mem_t &nixl_mem,
                                     nixlBackendMD *&out) {
    if (nixl_mem == OBJ_SEG) {
        auto md = std::make_unique<DaosDirectObjMD>(
            nixl_mem, mem.devId,
            mem.metaInfo.empty() ? std::to_string(mem.devId) : mem.metaInfo);
        devIdToObjKey_[mem.devId] = md->objKey;
        out = md.release();
    } else if (nixl_mem == DRAM_SEG || nixl_mem == VRAM_SEG) {
        out = new DaosDirectDramMD(mem.addr, mem.len);
    } else {
        return NIXL_ERR_NOT_SUPPORTED;
    }
    return NIXL_SUCCESS;
}

nixl_status_t
DaosDirectObjEngineImpl::deregisterMem(nixlBackendMD *meta) {
    if (auto *obj_md = dynamic_cast<DaosDirectObjMD*>(meta))
        devIdToObjKey_.erase(obj_md->devId);
    delete meta;
    return NIXL_SUCCESS;
}

nixl_status_t
DaosDirectObjEngineImpl::queryMem(const nixl_reg_dlist_t &descs,
                                  std::vector<nixl_query_resp_t> &resp) const {
    for (int i = 0; i < descs.descCount(); ++i)
        resp.push_back(nixl_b_params_t{});  // non-empty optional = found
    return NIXL_SUCCESS;
}

nixl_status_t
DaosDirectObjEngineImpl::prepXfer(const nixl_xfer_op_t &,
                                  const nixl_meta_dlist_t &,
                                  const nixl_meta_dlist_t &remote,
                                  const std::string &,
                                  const std::string &,
                                  nixlBackendReqH *&handle,
                                  const nixl_opt_b_args_t *) const {
    // Validate all remote keys are registered
    for (int i = 0; i < remote.descCount(); ++i) {
        if (devIdToObjKey_.find(remote[i].devId) == devIdToObjKey_.end())
            return NIXL_ERR_INVALID_PARAM;
    }
    handle = nullptr;
    return NIXL_SUCCESS;
}

// ────────────────────────────────────────────────────────────────────────────
// postXfer: DFS read/write, async via EQ pool
// ────────────────────────────────────────────────────────────────────────────
nixl_status_t
DaosDirectObjEngineImpl::postXfer(const nixl_xfer_op_t &operation,
                                  const nixl_meta_dlist_t &local,
                                  const nixl_meta_dlist_t &remote,
                                  const std::string &,
                                  nixlBackendReqH *&handle,
                                  const nixl_opt_b_args_t *) const {
    if (!dfs_) return NIXL_ERR_BACKEND;

    static std::atomic<uint64_t> g_req_id{0};
    uint64_t req_id = g_req_id.fetch_add(1, std::memory_order_relaxed);
    NIXL_OBJ_US_R("postXfer_enter", req_id);

    auto *req_h = new DaosDirectReqH();
    handle = req_h;

    for (int i = 0; i < local.descCount(); ++i) {
        const auto &ld = local[i];
        const auto &rd = remote[i];

        auto it = devIdToObjKey_.find(rd.devId);
        if (it == devIdToObjKey_.end()) return NIXL_ERR_INVALID_PARAM;
        const std::string &key = it->second;

        uintptr_t data_ptr = ld.addr;
        size_t data_len = ld.len;
        size_t offset = rd.addr;

        auto promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->statusFutures_.push_back(promise->get_future());

        // ────── hashoid mode ──────
        // Skip dfs_open(); synthesize OID client-side via hashoid.h helpers
        // (byte-identical to DAOS agg_sidecar). RPC/fabric path preserved —
        // daos_obj_update/fetch are the same transport layer dfs_write/read
        // lower onto.
        if (hashoid_mode_) {
            daos_obj_id_t oid;
            oid.lo = hashoid_oid_lo(static_cast<int>(hashoid_fnv1a_32(key)));
            oid.hi = hashoid_oid_hi_user(hashoid_T_, hashoid_IOD_, data_len);
            int rc = daos_obj_generate_oid(coh_, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
            if (rc != 0) { promise->set_value(NIXL_ERR_BACKEND); continue; }

            daos_handle_t oh = DAOS_HDL_INVAL;
            rc = daos_obj_open(coh_, oid,
                               (operation == NIXL_WRITE) ? DAOS_OO_RW : DAOS_OO_RO,
                               &oh, nullptr);
            if (rc != 0) { promise->set_value(NIXL_ERR_BACKEND); continue; }

            auto dkey = std::make_shared<daos_key_t>();
            d_iov_set(dkey.get(), (void*)kHashoidDkey, sizeof(kHashoidDkey) - 1);
            auto recx = std::make_shared<daos_recx_t>();
            recx->rx_idx = offset;
            recx->rx_nr  = data_len;
            auto iod = std::make_shared<daos_iod_t>();
            *iod = {};
            d_iov_set(&iod->iod_name, (void*)kHashoidAkey, sizeof(kHashoidAkey) - 1);
            iod->iod_type  = DAOS_IOD_ARRAY;
            iod->iod_size  = 1;
            iod->iod_nr    = 1;
            iod->iod_recxs = recx.get();

            auto iov = std::make_shared<d_iov_t>();
            d_iov_set(iov.get(), reinterpret_cast<void*>(data_ptr), data_len);
            auto sgl = std::make_shared<d_sg_list_t>();
            *sgl = {};
            sgl->sg_nr     = 1;
            sgl->sg_iovs   = iov.get();
            sgl->sg_nr_out = 1;

            auto waiter = std::make_shared<DaosDirectWaiter>();
            auto ev = std::make_shared<daos_event_t>();
            *ev = {};
            daos_event_init(ev.get(), getEQ(), nullptr);
            daos_event_register_comp_cb(ev.get(), daos_direct_completion_cb, waiter.get());

            if (operation == NIXL_WRITE) {
                rc = daos_obj_update(oh, DAOS_TX_NONE, 0, dkey.get(), 1,
                                     iod.get(), sgl.get(), ev.get());
            } else {
                rc = daos_obj_fetch(oh, DAOS_TX_NONE, 0, dkey.get(), 1,
                                    iod.get(), sgl.get(), nullptr, ev.get());
            }

            if (rc != 0) {
                daos_event_fini(ev.get());
                daos_obj_close(oh, nullptr);
                promise->set_value(NIXL_ERR_BACKEND);
                continue;
            }

            executor_->Submit([promise, waiter, oh, ev, iov, sgl, dkey, iod, recx,
                               operation, data_len, req_id]() mutable {
                { std::unique_lock<std::mutex> lk(waiter->mtx);
                  waiter->cv.wait(lk, [&]{ return waiter->done; }); }
                daos_event_fini(ev.get());
                daos_obj_close(oh, nullptr);
                daos_direct_complete_after_rate_limit(
                    promise,
                    waiter->err == 0 ? NIXL_SUCCESS : NIXL_ERR_BACKEND,
                    operation == NIXL_READ ? data_len : 0,
                    req_id);
            });
            continue;
        }
        // ────── end hashoid mode ──────

        int flags = (operation == NIXL_WRITE) ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDONLY;
        dfs_obj_t *obj = nullptr;
        int rc = dfs_open(dfs_, nullptr, key.c_str(),
                          S_IFREG | DEFFILEMODE, flags, 0, 0, nullptr, &obj);
        if (rc != 0) {
            promise->set_value(NIXL_ERR_BACKEND);
            continue;
        }

        // Heap-allocate ALL async state (iov, sgl, event) so they outlive this
        // stack frame. The EQ progress thread fires the completion callback
        // asynchronously and all pointers embedded in the DAOS RPC must be valid.
        auto iov = std::make_shared<d_iov_t>();
        d_iov_set(iov.get(), reinterpret_cast<void*>(data_ptr), data_len);
        auto sgl = std::make_shared<d_sg_list_t>();
        *sgl = {};
        sgl->sg_nr     = 1;
        sgl->sg_iovs   = iov.get();
        sgl->sg_nr_out = 1;

        auto waiter = std::make_shared<DaosDirectWaiter>();
        auto ev = std::make_shared<daos_event_t>();
        *ev = {};
        daos_event_init(ev.get(), getEQ(), nullptr);
        daos_event_register_comp_cb(ev.get(), daos_direct_completion_cb, waiter.get());

        if (operation == NIXL_WRITE) {
            rc = dfs_write(dfs_, obj, sgl.get(), offset, ev.get());
        } else {
            daos_size_t got = data_len;
            rc = dfs_read(dfs_, obj, sgl.get(), offset, &got, ev.get());
        }

        if (rc != 0) {
            daos_event_fini(ev.get());
            dfs_release(obj);
            promise->set_value(NIXL_ERR_BACKEND);
            continue;
        }

        executor_->Submit([promise, waiter, obj, ev, iov, sgl,
                           operation, data_len, req_id]() mutable {
            { std::unique_lock<std::mutex> lk(waiter->mtx);
              waiter->cv.wait(lk, [&]{ return waiter->done; }); }
            daos_event_fini(ev.get());
            dfs_release(obj);
            daos_direct_complete_after_rate_limit(
                promise,
                waiter->err == 0 ? NIXL_SUCCESS : NIXL_ERR_BACKEND,
                operation == NIXL_READ ? data_len : 0,
                req_id);
        });
    }

    NIXL_OBJ_US_R("postXfer_exit", req_id);
    return NIXL_IN_PROG;
}

nixl_status_t
DaosDirectObjEngineImpl::checkXfer(nixlBackendReqH *handle) const {
    return static_cast<DaosDirectReqH*>(handle)->getOverallStatus();
}

nixl_status_t
DaosDirectObjEngineImpl::releaseReqH(nixlBackendReqH *handle) const {
    delete static_cast<DaosDirectReqH*>(handle);
    return NIXL_SUCCESS;
}
