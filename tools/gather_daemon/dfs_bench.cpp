// dfs_bench.cpp — 16-thread DAOS DFS cold-read latency benchmark.
//
// Mirrors the kvbench/nixl_dfs READ phase: N threads each open+read a
// unique 64 KiB key in turn, cycling over a prepop set. Reports p50/p99/max
// per-op latency.
//
//   dfs_bench --pool Pool1 --cont lmcache_bench_64k \
//             --key-prefix kv_65536B_ --count 4096 --size 65536 \
//             --num-threads 16 --iter 256 --warmup 64

#include <daos.h>
#include <daos_fs.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <vector>

using clk = std::chrono::steady_clock;
using us  = std::chrono::microseconds;

int main(int argc, char **argv) {
    std::string pool = "Pool1", cont = "lmcache_bench_64k";
    std::string key_prefix = "kv_65536B_";
    size_t      sz         = 65536;
    int         count      = 4096;   // keys in prepop set
    int         width      = 7;
    int         num_threads = 16;
    int         iter        = 256;   // ops per thread (timed)
    int         warmup      = 64;    // ops per thread (untimed)

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w) -> const char * {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing arg %s\n", w); std::exit(1); }
            return argv[++i];
        };
        if      (a == "--pool")        pool = nxt("--pool");
        else if (a == "--cont")        cont = nxt("--cont");
        else if (a == "--key-prefix")  key_prefix = nxt("--key-prefix");
        else if (a == "--size")        sz = std::atol(nxt("--size"));
        else if (a == "--count")       count = std::atoi(nxt("--count"));
        else if (a == "--width")       width = std::atoi(nxt("--width"));
        else if (a == "--num-threads") num_threads = std::atoi(nxt("--num-threads"));
        else if (a == "--iter")        iter = std::atoi(nxt("--iter"));
        else if (a == "--warmup")      warmup = std::atoi(nxt("--warmup"));
    }

    if (daos_init() != 0) { std::fprintf(stderr, "daos_init failed\n"); return 1; }
    daos_handle_t poh{}, coh{};
    if (daos_pool_connect(pool.c_str(), nullptr, DAOS_PC_RO, &poh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "pool_connect failed\n"); return 2;
    }
    if (daos_cont_open(poh, cont.c_str(), DAOS_COO_RO, &coh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "cont_open failed\n"); return 3;
    }
    dfs_t *dfs = nullptr;
    if (dfs_mount(poh, coh, O_RDONLY, &dfs) != 0) {
        std::fprintf(stderr, "dfs_mount failed\n"); return 4;
    }

    std::vector<std::vector<int64_t>> per_thread_lat(num_threads);
    std::atomic<uint64_t> bytes_read{0};
    auto t_start = clk::now();

    std::vector<std::thread> workers;
    for (int t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t] {
            std::vector<char> buf(sz);
            char key[64];
            auto &lats = per_thread_lat[t];
            lats.reserve(iter);

            // Each thread reads a disjoint slice of the prepop set, matching
            // the DFS template's per-thread key partitioning.
            const int total_ops = warmup + iter;
            const int per_thread_slice = count / num_threads;
            const int base = t * per_thread_slice;

            for (int i = 0; i < total_ops; ++i) {
                int idx = base + (i % per_thread_slice);
                std::snprintf(key, sizeof(key), "%s%0*d", key_prefix.c_str(), width, idx);

                auto t_op = clk::now();
                dfs_obj_t *obj = nullptr;
                int rc = dfs_lookup_rel(dfs, nullptr, key, O_RDONLY, &obj, nullptr, nullptr);
                if (rc || !obj) { std::fprintf(stderr, "[t%d] lookup('%s') rc=%d\n", t, key, rc); continue; }

                d_iov_t iov{}; d_sg_list_t sgl{};
                d_iov_set(&iov, buf.data(), sz);
                sgl.sg_nr = 1; sgl.sg_iovs = &iov;
                daos_size_t got = sz;
                rc = dfs_read(dfs, obj, &sgl, 0, &got, nullptr);  // SYNC read
                dfs_release(obj);
                auto t_done = clk::now();

                if (rc != 0) { std::fprintf(stderr, "[t%d] read('%s') rc=%d\n", t, key, rc); continue; }

                if (i >= warmup) {
                    int64_t lat = std::chrono::duration_cast<us>(t_done - t_op).count();
                    lats.push_back(lat);
                    bytes_read.fetch_add(sz, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &w : workers) w.join();

    auto t_end = clk::now();
    double wall_s = std::chrono::duration<double>(t_end - t_start).count();

    // Aggregate
    std::vector<int64_t> all_lat;
    for (auto &v : per_thread_lat) all_lat.insert(all_lat.end(), v.begin(), v.end());
    std::sort(all_lat.begin(), all_lat.end());

    auto pct = [&](double p) -> int64_t {
        if (all_lat.empty()) return 0;
        size_t idx = std::min<size_t>(all_lat.size() - 1, (size_t)(p * all_lat.size()));
        return all_lat[idx];
    };

    uint64_t total_bytes = bytes_read.load();
    double bw_mbps = (total_bytes / 1048576.0) / wall_s;

    std::printf("\n=== dfs_bench 64 KiB cold read ===\n");
    std::printf("pool=%s cont=%s key_prefix=%s size=%zu count=%d\n",
                pool.c_str(), cont.c_str(), key_prefix.c_str(), sz, count);
    std::printf("num_threads=%d  iter=%d  warmup=%d  total_ops=%zu\n",
                num_threads, iter, warmup, all_lat.size());
    std::printf("wall=%.3fs  bytes=%lu MB  throughput=%.1f MB/s\n",
                wall_s, (unsigned long)(total_bytes / 1048576), bw_mbps);
    std::printf("per-op latency (us):\n");
    std::printf("  min    = %ld\n", all_lat.empty() ? 0 : all_lat.front());
    std::printf("  p50    = %ld\n", pct(0.50));
    std::printf("  p90    = %ld\n", pct(0.90));
    std::printf("  p99    = %ld\n", pct(0.99));
    std::printf("  max    = %ld\n", all_lat.empty() ? 0 : all_lat.back());
    std::printf("===================================\n\n");

    dfs_umount(dfs);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();
    return 0;
}
