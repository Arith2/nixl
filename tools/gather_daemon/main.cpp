// gather_daemon — NIXL server-side aggregation service (Option Y push-RDMA)
//
// Runs colocated with the DAOS server on hsc-21. Receives aggregated-read
// requests from NIXL (forwarded through Ceph), reads chunks from DAOS
// asynchronously into an MR-pinned arena, then pushes per-layer slices into
// the client's pre-registered target buffer via RDMA_WRITE_WITH_IMM.

#include "agg_policy.h"
#include "agg_proto.h"
#include "rdma_common.h"
#include "third_party/httplib.h"

#include <daos.h>
#include <daos_fs.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <sys/syscall.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono;
using aggproto::OpenReq;
using aggproto::OpenResp;
using aggproto::ChunkRef;
using aggproto::LayerRange;
using aggproto::TargetBuf;
using aggproto::QpInfo;

// ── Config ──────────────────────────────────────────────────────────────────
struct Config {
    std::string source_pool = "Pool1";
    std::string source_cont = "lmcache";
    std::string rdma_dev;               // empty = auto-pick first active port
    int    rdma_port_num = -1;
    int    rdma_gid_idx  = -1;
    int    port       = 8080;
    int    n_workers  = 16;
    int    daos_ct    = 16;  // DAOS concurrency cap (--ct)
    size_t arena_size = 256ULL * 1024 * 1024;  // 256 MiB total by default
    size_t slot_size  = 16ULL  * 1024 * 1024;  // 16 MiB per slot by default
};

// ── Trace ───────────────────────────────────────────────────────────────────
static inline bool trace_enabled() {
    static const bool en = (std::getenv("NIXL_OBJ_TRACE") != nullptr);
    return en;
}
static inline FILE *trace_fp() {
    static FILE *f = nullptr;
    static std::once_flag once;
    std::call_once(once, [] {
        const char *p = std::getenv("NIXL_OBJ_TRACE_FILE");
        if (!p) p = "/tmp/gather_daemon_trace.log";
        f = std::fopen(p, "a");
        if (f) std::setvbuf(f, nullptr, _IOLBF, 0);
    });
    return f;
}
static inline void trace_emit(const char *evt, uint64_t req_id) {
    if (!trace_enabled()) return;
    FILE *f = trace_fp();
    if (!f) return;
    auto now = system_clock::now().time_since_epoch();
    double ts = duration_cast<microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %lu %s\n", ts, tid, (unsigned long)req_id, evt);
}
#define TR(ev, rid) trace_emit(ev, rid)

static inline int64_t us(high_resolution_clock::time_point a,
                         high_resolution_clock::time_point b) {
    return duration_cast<microseconds>(b - a).count();
}

// ── DAOS context ────────────────────────────────────────────────────────────
struct DaosCtx {
    daos_handle_t poh{};
    std::unordered_map<std::string, daos_handle_t> coh;
    std::unordered_map<std::string, dfs_t*>       dfs;
    std::mutex mtx;

    int ensure_cont(const std::string &cont) {
        std::lock_guard<std::mutex> lk(mtx);
        if (dfs.count(cont)) return 0;
        daos_handle_t c{};
        int rc = daos_cont_open(poh, cont.c_str(), DAOS_COO_RO, &c, nullptr, nullptr);
        if (rc) { std::cerr << "daos_cont_open(" << cont << ") rc=" << rc << std::endl; return rc; }
        dfs_t *d = nullptr;
        rc = dfs_mount(poh, c, O_RDONLY, &d);
        if (rc) { std::cerr << "dfs_mount(" << cont << ") rc=" << rc << std::endl; return rc; }
        coh[cont] = c; dfs[cont] = d;
        std::cout << "mounted cont='" << cont << "'" << std::endl;
        return 0;
    }
    dfs_t *get(const std::string &cont) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = dfs.find(cont);
        return (it != dfs.end()) ? it->second : nullptr;
    }
};

static int daos_bringup(const Config &cfg, DaosCtx &ctx) {
    int rc = daos_init();
    if (rc) { std::cerr << "daos_init rc=" << rc << std::endl; return rc; }
    rc = daos_pool_connect(cfg.source_pool.c_str(), nullptr, DAOS_PC_RO,
                           &ctx.poh, nullptr, nullptr);
    if (rc) { std::cerr << "pool_connect(" << cfg.source_pool << ") rc=" << rc << std::endl; return rc; }
    std::cout << "DAOS ready — pool=" << cfg.source_pool << std::endl;
    return ctx.ensure_cont(cfg.source_cont);
}

static void daos_teardown(DaosCtx &ctx) {
    for (auto &kv : ctx.dfs) dfs_umount(kv.second);
    for (auto &kv : ctx.coh) daos_cont_close(kv.second, nullptr);
    daos_pool_disconnect(ctx.poh, nullptr);
    daos_fini();
}

// ── RDMA state (shared across sessions) ─────────────────────────────────────
struct RdmaState {
    rdmac::PortInfo pi{};
    ibv_pd *pd = nullptr;
    ibv_cq *send_cq = nullptr;
    ibv_mr *arena_mr = nullptr;

    bool init(const Config &cfg, void *arena_base, size_t arena_size) {
        if (!rdmac::open_first_port(pi, cfg.rdma_dev, cfg.rdma_port_num, cfg.rdma_gid_idx)) {
            return false;
        }
        std::cout << "RDMA: dev=" << pi.dev_name << " port=" << (int)pi.port
                  << " gid_idx=" << (int)pi.gid_idx << " gid=" << rdmac::gid_to_str(pi.gid)
                  << " mtu=" << (1 << (pi.pattr.active_mtu + 7)) << std::endl;

        pd = ibv_alloc_pd(pi.ctx);
        if (!pd) { std::cerr << "ibv_alloc_pd failed" << std::endl; return false; }

        send_cq = ibv_create_cq(pi.ctx, 4096, nullptr, nullptr, 0);
        if (!send_cq) { std::cerr << "ibv_create_cq(send) failed" << std::endl; return false; }

        arena_mr = ibv_reg_mr(pd, arena_base, arena_size,
                              IBV_ACCESS_LOCAL_WRITE);
        if (!arena_mr) {
            std::cerr << "ibv_reg_mr(arena) failed, errno=" << errno << std::endl;
            return false;
        }
        std::cout << "RDMA: arena MR registered  base=" << arena_base
                  << " size=" << arena_size << " lkey=0x" << std::hex << arena_mr->lkey
                  << std::dec << std::endl;
        return true;
    }

    // Allocate a new RC QP for a session; fills out `qp` and populates
    // server_qp (our local view) for return to the client.
    bool alloc_qp(ibv_qp *&qp_out, QpInfo &server_qp) {
        ibv_qp_init_attr ia{};
        ia.send_cq = send_cq;
        ia.recv_cq = send_cq;            // daemon never receives; reuse cq to simplify
        ia.cap.max_send_wr  = 256;
        ia.cap.max_recv_wr  = 16;
        ia.cap.max_send_sge = 1;
        ia.cap.max_recv_sge = 1;
        ia.qp_type = IBV_QPT_RC;
        ibv_qp *qp = ibv_create_qp(pd, &ia);
        if (!qp) { std::cerr << "ibv_create_qp failed" << std::endl; return false; }
        if (!rdmac::modify_qp_init(qp, pi.port)) { ibv_destroy_qp(qp); return false; }

        server_qp.qpn = qp->qp_num;
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        server_qp.psn = (uint32_t)(rng() & 0xFFFFFF);
        server_qp.lid = pi.pattr.lid;
        server_qp.mtu = (1u << (pi.pattr.active_mtu + 7));
        server_qp.port_num = pi.port;
        server_qp.gid = rdmac::gid_to_str(pi.gid);

        qp_out = qp;
        return true;
    }

    bool connect_qp(ibv_qp *qp, const QpInfo &client_qp, uint32_t local_psn) {
        if (!rdmac::modify_qp_rtr(qp, pi.port, pi.gid_idx, client_qp)) return false;
        if (!rdmac::modify_qp_rts(qp, local_psn)) return false;
        return true;
    }

    // Drain up to n completions from send_cq (non-blocking).
    int drain_send_cq(int n) {
        ibv_wc wcs[32]; if (n > 32) n = 32;
        int got = ibv_poll_cq(send_cq, n, wcs);
        for (int i = 0; i < got; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                std::cerr << "send_cq wr_id=" << wcs[i].wr_id
                          << " status=" << ibv_wc_status_str(wcs[i].status)
                          << " opcode=" << wcs[i].opcode << std::endl;
            }
        }
        return got;
    }

    void teardown() {
        if (arena_mr) ibv_dereg_mr(arena_mr);
        if (send_cq)  ibv_destroy_cq(send_cq);
        if (pd)       ibv_dealloc_pd(pd);
        if (pi.ctx)   ibv_close_device(pi.ctx);
    }
};

// ── Arena: 256 MiB, 16 slots × 16 MiB ──────────────────────────────────────
class Arena {
public:
    bool init(size_t total, size_t slot_sz) {
        total_ = total; slot_sz_ = slot_sz;
        n_slots_ = total / slot_sz;
        base_ = aligned_alloc(4096, total);
        if (!base_) { std::cerr << "arena aligned_alloc failed" << std::endl; return false; }
        std::memset(base_, 0, total);
        free_slots_.reserve(n_slots_);
        for (size_t i = 0; i < n_slots_; ++i) free_slots_.push_back(i);
        std::cout << "arena: " << total_ << " B (" << n_slots_ << " × "
                  << slot_sz_ << " B) at " << base_ << std::endl;
        return true;
    }
    int acquire() {
        std::lock_guard<std::mutex> lk(mtx_);
        if (free_slots_.empty()) return -1;
        int s = free_slots_.back(); free_slots_.pop_back();
        return s;
    }
    void release(int idx) {
        std::lock_guard<std::mutex> lk(mtx_);
        free_slots_.push_back(idx);
    }
    void *slot_ptr(int idx) const { return (char*)base_ + (size_t)idx * slot_sz_; }
    size_t slot_size() const { return slot_sz_; }
    void *base() const { return base_; }
    size_t total() const { return total_; }
    ~Arena() { if (base_) free(base_); }
private:
    void  *base_ = nullptr;
    size_t total_ = 0, slot_sz_ = 0, n_slots_ = 0;
    std::vector<int> free_slots_;
    std::mutex mtx_;
};

// ── Session + aggregation state ────────────────────────────────────────────
struct Session {
    std::string session_id;
    QpInfo      client_qp;
    QpInfo      server_qp;
    ibv_qp     *qp = nullptr;
    bool        qp_ready = false;
};

// Shared across all batches of one logical load; last batch to finish
// releases the arena slot.
struct RequestShared {
    int              slot_idx = -1;
    std::atomic<int> batches_remaining{0};
    std::string      agg_handle;
    // RDMA destination (same across all batches of this request)
    uint64_t         target_addr = 0;
    uint32_t         target_rkey = 0;
    uint64_t         target_len  = 0;     // ring size for dst_off wrap
    ibv_qp          *qp = nullptr;
    uint64_t         req_id_num  = 0;     // for wr_id / trace
};

// Shared across all CHUNKS of one batch; last chunk posts the batch's RDMA.
struct BatchShared {
    uint32_t          batch_idx   = 0;     // imm_data
    uint64_t          rdma_src_off = 0;    // offset in arena slot
    uint64_t          rdma_dst_off = 0;    // offset in target ring
    uint32_t          rdma_len    = 0;     // batch total bytes
    std::atomic<int>  remaining{0};        // init = chunks-per-batch
    std::shared_ptr<RequestShared> request;
};

// One task per WORKER SLICE. Each worker gets a contiguous range
// [chunk_lo, chunk_hi) of the request's chunk list and processes them
// sequentially on its own thread — zero cross-worker dequeue contention,
// matching NIXL-DFS / dfs_bench's static-partition pattern.
struct SliceTask {
    std::shared_ptr<std::vector<ChunkRef>> chunks;   // full request chunk list
    std::shared_ptr<std::vector<std::shared_ptr<BatchShared>>> batches; // per-batch state, indexed by batch_idx
    std::shared_ptr<RequestShared> request;
    uint32_t chunks_lo     = 0;
    uint32_t chunks_hi     = 0;      // exclusive
    uint32_t chunks_per_batch = 0;   // = agg_size
    uint64_t per_chunk_bytes  = 0;
};

// Legacy OpenState — kept only for the deprecated single-chunk "custom" path
// that the earlier L1 test_client used. kv_layer mode uses ChunkTask.
struct OpenState {
    std::string             agg_handle;
    std::string             session_id;
    std::string             req_id;
    int                     slot_idx = -1;
    ibv_qp                 *qp = nullptr;
    std::vector<ChunkRef>   chunks;
    TargetBuf               target;
    std::vector<LayerRange> layer_map;
    high_resolution_clock::time_point t_submit;
    std::shared_ptr<RequestShared> request;
};

// Semaphore for DAOS-concurrency cap (`ct`). C++17 compatible.
class Semaphore {
public:
    explicit Semaphore(int n) : count_(n) {}
    void acquire() {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [this]{ return count_ > 0; });
        --count_;
    }
    void release() {
        std::lock_guard<std::mutex> lk(mtx_);
        ++count_;
        cv_.notify_one();
    }
private:
    std::mutex               mtx_;
    std::condition_variable  cv_;
    int                      count_;
};

class Registry {
public:
    // Create a session with an already-allocated QP.
    std::string create_session(const QpInfo &client_qp, const QpInfo &server_qp, ibv_qp *qp) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string sid = "s-" + random_hex(8);
        Session s;
        s.session_id = sid;
        s.client_qp  = client_qp;
        s.server_qp  = server_qp;
        s.qp         = qp;
        s.qp_ready   = false;
        sessions_[sid] = std::move(s);
        return sid;
    }
    bool mark_session_ready(const std::string &sid) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = sessions_.find(sid);
        if (it == sessions_.end()) return false;
        it->second.qp_ready = true; return true;
    }
    bool get_session(const std::string &sid, Session &out) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = sessions_.find(sid);
        if (it == sessions_.end()) return false;
        out = it->second; return true;
    }
    // Opens
    std::string register_open(OpenState st) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string h = "h-" + random_hex(8);
        st.agg_handle = h;
        opens_[h] = std::move(st);
        return h;
    }
    bool take_open(const std::string &h, OpenState &out) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = opens_.find(h);
        if (it == opens_.end()) return false;
        out = std::move(it->second);
        opens_.erase(it);
        return true;
    }
    bool release_open(const std::string &h) {
        std::lock_guard<std::mutex> lk(mtx_);
        return opens_.erase(h) > 0;
    }

private:
    static std::string random_hex(int n) {
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        static const char *hex = "0123456789abcdef";
        std::string s(n, '0');
        for (int i = 0; i < n; ++i) s[i] = hex[rng() & 0xF];
        return s;
    }
    std::mutex mtx_;
    std::unordered_map<std::string, Session>   sessions_;
    std::unordered_map<std::string, OpenState> opens_;
};

// ── Worker pool (per-worker slice — static partitioning like NIXL-DFS) ────
class WorkerPool {
public:
    void start(int n, int daos_concurrency, DaosCtx *ctx, Arena *arena, RdmaState *rdma) {
        ctx_ = ctx; arena_ = arena; rdma_ = rdma;
        sem_ = std::make_unique<Semaphore>(daos_concurrency);
        n_ = n;
        std::cout << "worker pool: " << n << " threads up, DAOS ct=" << daos_concurrency << std::endl;
        slices_.resize(n);
        slice_mtx_.resize(n);
        slice_cv_.resize(n);
        for (int i = 0; i < n; ++i) {
            slice_mtx_[i] = std::make_unique<std::mutex>();
            slice_cv_[i]  = std::make_unique<std::condition_variable>();
        }
        for (int i = 0; i < n; ++i)
            workers_.emplace_back([this, i] { loop(i); });
    }
    // Submit to a specific worker's private queue; no contention with others.
    void submit_to(int worker_idx, SliceTask t) {
        worker_idx %= n_;
        { std::lock_guard<std::mutex> lk(*slice_mtx_[worker_idx]);
          slices_[worker_idx].push(std::move(t)); }
        slice_cv_[worker_idx]->notify_one();
    }
    int size() const { return n_; }
    void stop() {
        shutdown_ = true;
        for (int i = 0; i < n_; ++i) slice_cv_[i]->notify_all();
        for (auto &t : workers_) if (t.joinable()) t.join();
    }

private:
    void loop(int wid) {
        while (!shutdown_) {
            SliceTask t;
            {
                std::unique_lock<std::mutex> lk(*slice_mtx_[wid]);
                slice_cv_[wid]->wait(lk, [&] { return !slices_[wid].empty() || shutdown_; });
                if (shutdown_) break;
                t = std::move(slices_[wid].front()); slices_[wid].pop();
            }
            run(std::move(t), wid);
        }
    }

    // Worker processes its own disjoint chunk range sequentially. Same thread
    // handles lookup + read for every chunk in [chunks_lo, chunks_hi). When
    // any chunk's completion causes its batch counter to hit 0, the worker
    // that finished it posts the batch's RDMA_WRITE_WITH_IMM.
    void run(SliceTask t, int wid) {
        auto &chunks  = *t.chunks;
        auto &batches = *t.batches;
        auto req      = t.request;
        void *slot_buf = arena_->slot_ptr(req->slot_idx);

        for (uint32_t i = t.chunks_lo; i < t.chunks_hi; ++i) {
            const ChunkRef &c = chunks[i];
            const uint64_t dst_off = (uint64_t)i * t.per_chunk_bytes;
            const uint32_t batch_idx = i / t.chunks_per_batch;

            // dfs_lookup_rel + sync dfs_read (sem caps global DAOS concurrency)
            sem_->acquire();
            dfs_t *d = ctx_->get(c.cont);
            dfs_obj_t *obj = nullptr;
            int rc = dfs_lookup_rel(d, nullptr, c.key.c_str(),
                                    O_RDONLY, &obj, nullptr, nullptr);
            if (rc || !obj) {
                std::cerr << "w" << wid << ": dfs_lookup_rel('" << c.key
                          << "') rc=" << rc << std::endl;
                sem_->release();
                continue;
            }
            d_iov_t iov{}; d_sg_list_t sgl{};
            d_iov_set(&iov, (char*)slot_buf + dst_off, (size_t)c.len);
            sgl.sg_nr = 1; sgl.sg_iovs = &iov;
            daos_size_t got = c.len;
            rc = dfs_read(d, obj, &sgl, c.off, &got, /*event*/nullptr);  // SYNC
            dfs_release(obj);
            sem_->release();

            if (rc != 0) {
                std::cerr << "w" << wid << ": dfs_read('" << c.key << "') rc=" << rc << std::endl;
                continue;
            }

            auto &bshared = batches[batch_idx];
            if (bshared->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                // This was the last chunk of its batch → post RDMA
                void     *src      = (char*)slot_buf + bshared->rdma_src_off;
                uint64_t  dst_ring = (req->target_len > 0)
                                   ? (bshared->rdma_dst_off % req->target_len)
                                   : bshared->rdma_dst_off;
                uint64_t  dst      = req->target_addr + dst_ring;
                rdmac::post_write_imm(req->qp, rdma_->arena_mr->lkey,
                                      /*wr_id=*/(req->req_id_num << 16) | bshared->batch_idx,
                                      src, bshared->rdma_len,
                                      dst, req->target_rkey, bshared->batch_idx);

                if (req->batches_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    arena_->release(req->slot_idx);
                }
            }
        }
    }

    DaosCtx   *ctx_ = nullptr;
    Arena     *arena_ = nullptr;
    RdmaState *rdma_ = nullptr;
    std::unique_ptr<Semaphore> sem_;
    int n_ = 0;
    std::vector<std::queue<SliceTask>>                slices_;
    std::vector<std::unique_ptr<std::mutex>>              slice_mtx_;
    std::vector<std::unique_ptr<std::condition_variable>> slice_cv_;
    std::vector<std::thread>             workers_;
    std::atomic<bool>                    shutdown_{false};
};

// ── HTTP handlers ──────────────────────────────────────────────────────────

// POST /agg/_session — QP handshake only. Body must include client_qp.
// No arena acquisition, no DAOS work, no RDMA push. Lets the client reach
// RTR/RTS on its QP before triggering any writes.
static void handle_session(const httplib::Request &req, httplib::Response &res,
                           Registry *reg, RdmaState *rdma) {
    OpenReq r;
    try { json::parse(req.body).get_to(r); }
    catch (const std::exception &e) {
        res.status = 400;
        res.set_content(std::string(R"({"error":"parse: )") + e.what() + "\"}", "application/json");
        return;
    }
    if (!r.include_client_qp) {
        res.status = 400;
        res.set_content(R"({"error":"client_qp required"})", "application/json");
        return;
    }
    ibv_qp *qp = nullptr;
    QpInfo server_qp{};
    if (!rdma->alloc_qp(qp, server_qp)) {
        res.status = 500;
        res.set_content(R"({"error":"qp_alloc_failed"})", "application/json");
        return;
    }
    if (!rdma->connect_qp(qp, r.client_qp, server_qp.psn)) {
        ibv_destroy_qp(qp);
        res.status = 500;
        res.set_content(R"({"error":"qp_connect_failed"})", "application/json");
        return;
    }
    std::string sid = reg->create_session(r.client_qp, server_qp, qp);
    reg->mark_session_ready(sid);
    std::cout << "session " << sid << ": QP connected  client.qpn=" << r.client_qp.qpn
              << " server.qpn=" << server_qp.qpn << std::endl;

    OpenResp resp;
    resp.agg_handle = "";              // no open yet
    resp.expected_writes = 0;
    resp.ttl_ms = 30000;
    resp.session_id = sid;
    resp.include_server_qp = true;
    resp.server_qp = server_qp;
    json out = resp;
    res.set_content(out.dump(), "application/json");
}

// POST /agg/_open — requires an existing session_id. Triggers DAOS read +
// per-layer RDMA push.
static void handle_open(const httplib::Request &req, httplib::Response &res,
                        DaosCtx *ctx, Arena *arena, Registry *reg,
                        WorkerPool *pool) {
    OpenReq r;
    try { json::parse(req.body).get_to(r); }
    catch (const std::exception &e) {
        res.status = 400;
        res.set_content(std::string(R"({"error":"parse: )") + e.what() + "\"}", "application/json");
        return;
    }

    Session sess;
    if (r.session_id.empty() || !reg->get_session(r.session_id, sess)) {
        res.status = 400;
        res.set_content(R"JSON({"error":"session_id required (call /agg/_session first)"})JSON",
                        "application/json");
        return;
    }

    for (const auto &c : r.chunks) {
        if (ctx->ensure_cont(c.cont) != 0) {
            res.status = 500;
            res.set_content(R"({"error":"cont_mount_failed"})", "application/json");
            return;
        }
    }

    int slot = arena->acquire();
    if (slot < 0) {
        res.status = 503;
        res.set_content(R"({"error":"arena_full"})", "application/json");
        return;
    }

    // Build the full chunk list + layer_map first (as before), then FAN OUT:
    // one OpenState per aggregation group, so workers process groups in
    // parallel across the thread pool.
    std::vector<ChunkRef>   all_chunks  = std::move(r.chunks);
    std::vector<LayerRange> layer_map   = std::move(r.layer_map);
    uint64_t                per_chunk_bytes = 0;
    uint32_t                agg_size_val    = 0;

    if (r.agg_mode == "kv_layer") {
        const uint32_t num_layers       = r.agg_params.value("num_layers",       0u);
        const uint32_t chunks_per_layer = r.agg_params.value("chunks_per_layer", 0u);
        agg_size_val                    = r.agg_params.value("agg_size",         0u);
        if (num_layers == 0 || chunks_per_layer == 0 || agg_size_val == 0 || all_chunks.empty()) {
            arena->release(slot);
            res.status = 400;
            res.set_content(R"JSON({"error":"kv_layer requires num_layers, chunks_per_layer, agg_size"})JSON",
                            "application/json");
            return;
        }
        per_chunk_bytes                = all_chunks.front().len;
        const uint64_t group_bytes     = (uint64_t)agg_size_val * per_chunk_bytes;
        const uint32_t groups_total    = (uint32_t)((all_chunks.size() + agg_size_val - 1) / agg_size_val);
        layer_map.clear();
        layer_map.reserve(groups_total);
        for (uint32_t g = 0; g < groups_total; ++g) {
            LayerRange lr;
            lr.idx = g;
            lr.off = (uint64_t)g * group_bytes;
            lr.len = group_bytes;
            uint64_t remaining_chunks = all_chunks.size() - (uint64_t)g * agg_size_val;
            if (remaining_chunks < agg_size_val)
                lr.len = remaining_chunks * per_chunk_bytes;
            layer_map.push_back(lr);
        }
        std::cout << "kv_layer dispatch: chunks=" << all_chunks.size()
                  << " agg_size=" << agg_size_val << " groups=" << groups_total
                  << " group_bytes=" << group_bytes << std::endl;
    } else if (r.agg_mode != "custom" && !r.agg_mode.empty()) {
        arena->release(slot);
        res.status = 400;
        std::string msg = R"JSON({"error":"unknown agg_mode: )JSON" + r.agg_mode + "\"}";
        res.set_content(msg, "application/json");
        return;
    }

    uint32_t expected   = (uint32_t)layer_map.size();

    // Chunk-level task pool: one ChunkTask per chunk. Worker pool has 16
    // threads, DAOS-concurrency cap enforced by a semaphore. Per-batch
    // atomic counter tracks when the batch's agg_size chunks are all done;
    // the finishing worker posts the batch's RDMA.
    std::string h = "h-" + r.req_id + "-" + std::to_string((uintptr_t)&r);
    OpenState placeholder; placeholder.slot_idx = slot; placeholder.agg_handle = h;
    reg->register_open(std::move(placeholder));

    auto req_shared = std::make_shared<RequestShared>();
    req_shared->slot_idx  = slot;
    req_shared->agg_handle = h;
    req_shared->target_addr = r.target.addr;
    req_shared->target_rkey = r.target.rkey;
    req_shared->target_len  = r.target.len;
    req_shared->qp          = sess.qp;
    try { req_shared->req_id_num = std::stoull(r.req_id, nullptr, 16); }
    catch (...) { req_shared->req_id_num = 0; }
    req_shared->batches_remaining.store((int)expected);

    const size_t chunks_per_group = agg_size_val > 0 ? agg_size_val : all_chunks.size();
    per_chunk_bytes = all_chunks.empty() ? 0 : all_chunks.front().len;

    // Build per-batch shared state
    auto batches = std::make_shared<std::vector<std::shared_ptr<BatchShared>>>(expected);
    for (uint32_t g = 0; g < expected; ++g) {
        auto bshared = std::make_shared<BatchShared>();
        bshared->batch_idx    = g;
        bshared->rdma_src_off = layer_map[g].off;
        bshared->rdma_dst_off = layer_map[g].off;
        bshared->rdma_len     = (uint32_t)layer_map[g].len;
        bshared->request      = req_shared;
        const size_t lo = (size_t)g * chunks_per_group;
        const size_t hi = std::min(lo + chunks_per_group, all_chunks.size());
        bshared->remaining.store((int)(hi - lo));
        (*batches)[g] = bshared;
    }

    // Static-partition the chunk range across the worker pool. Each worker
    // gets [chunks_lo, chunks_hi) and processes sequentially on its own
    // thread — no cross-worker dequeue contention, matches NIXL-DFS / dfs_bench.
    auto chunks_shared = std::make_shared<std::vector<ChunkRef>>(std::move(all_chunks));
    const uint32_t total = (uint32_t)chunks_shared->size();
    const int n_workers = pool->size();
    const uint32_t per_worker = (total + n_workers - 1) / n_workers;
    for (int w = 0; w < n_workers; ++w) {
        const uint32_t lo = std::min((uint32_t)w * per_worker, total);
        const uint32_t hi = std::min(lo + per_worker, total);
        if (lo >= hi) continue;
        SliceTask st;
        st.chunks          = chunks_shared;
        st.batches         = batches;
        st.request         = req_shared;
        st.chunks_lo       = lo;
        st.chunks_hi       = hi;
        st.chunks_per_batch = (uint32_t)chunks_per_group;
        st.per_chunk_bytes  = per_chunk_bytes;
        pool->submit_to(w, std::move(st));
    }

    OpenResp resp;
    resp.agg_handle = h;
    resp.expected_writes = expected;
    resp.ttl_ms = 30000;
    resp.session_id = r.session_id;
    resp.include_server_qp = false;   // session already established
    // Generate synth_names[] one per aggregation group; order matches imm_data.
    resp.synth_names.reserve(expected);
    for (uint32_t i = 0; i < expected; ++i) {
        resp.synth_names.push_back("agg_" + r.req_id + "_" + std::to_string(i));
    }
    json out = resp;
    res.set_content(out.dump(), "application/json");
}

static void handle_delete(const httplib::Request &req, httplib::Response &res, Registry *reg) {
    if (req.matches.size() < 2) { res.status = 404; return; }
    res.status = reg->release_open(req.matches[1].str()) ? 204 : 404;
}

static void handle_health(const httplib::Request &, httplib::Response &res) {
    res.set_content(R"({"status":"ok"})", "application/json");
}

// ── main ───────────────────────────────────────────────────────────────────
int main(int argc, char **argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w) -> const char * {
            if (i + 1 >= argc) { std::cerr << "missing arg for " << w << std::endl; std::exit(1); }
            return argv[++i];
        };
        if      (a == "--port")         cfg.port        = std::atoi(nxt("--port"));
        else if (a == "--workers")      cfg.n_workers   = std::atoi(nxt("--workers"));
        else if (a == "--ct")           cfg.daos_ct     = std::atoi(nxt("--ct"));
        else if (a == "--source-pool")  cfg.source_pool = nxt("--source-pool");
        else if (a == "--source-cont")  cfg.source_cont = nxt("--source-cont");
        else if (a == "--arena-size")   cfg.arena_size  = std::atol(nxt("--arena-size"));
        else if (a == "--slot-size")    cfg.slot_size   = std::atol(nxt("--slot-size"));
        else if (a == "--rdma-dev")     cfg.rdma_dev    = nxt("--rdma-dev");
        else if (a == "--rdma-port")    cfg.rdma_port_num = std::atoi(nxt("--rdma-port"));
        else if (a == "--rdma-gid-idx") cfg.rdma_gid_idx  = std::atoi(nxt("--rdma-gid-idx"));
        else if (a == "-h" || a == "--help") {
            std::cout << "Usage: gather_daemon [--port N] [--workers N]\n"
                         "                     [--source-pool NAME] [--source-cont NAME]\n"
                         "                     [--arena-size BYTES] [--slot-size BYTES]\n"
                         "                     [--rdma-dev NAME] [--rdma-port N] [--rdma-gid-idx N]\n";
            return 0;
        }
    }

    DaosCtx ctx;
    if (daos_bringup(cfg, ctx) != 0) return 2;

    Arena arena;
    if (!arena.init(cfg.arena_size, cfg.slot_size)) return 3;

    RdmaState rdma;
    if (!rdma.init(cfg, arena.base(), arena.total())) return 4;

    Registry reg;
    WorkerPool pool;
    pool.start(cfg.n_workers, cfg.daos_ct, &ctx, &arena, &rdma);

    httplib::Server server;
    server.Get("/health", handle_health);
    server.Post("/agg/_session",
                [&](const httplib::Request &req, httplib::Response &res) {
                    handle_session(req, res, &reg, &rdma);
                });
    server.Post("/agg/_open",
                [&](const httplib::Request &req, httplib::Response &res) {
                    handle_open(req, res, &ctx, &arena, &reg, &pool);
                });
    server.Delete(R"(/agg/(h-[0-9a-f]+))",
                  [&](const httplib::Request &req, httplib::Response &res) {
                      handle_delete(req, res, &reg);
                  });

    std::cout << "listening on 0.0.0.0:" << cfg.port << std::endl;
    server.listen("0.0.0.0", cfg.port);

    pool.stop();
    rdma.teardown();
    daos_teardown(ctx);
    return 0;
}
