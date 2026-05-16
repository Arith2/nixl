// prepop_key.cpp — write one or many DFS keys with per-key random data.
//
// Single key:
//   prepop_key --pool Pool1 --cont lmcache --key kv_65536B_0000000 --size 65536
//
// Batch:
//   prepop_key --pool Pool1 --cont lmcache --size 65536 \
//              --key-prefix kv_65536B_ --start 0 --count 2048
//
// Requires daos_agent running on the same host.

#include <daos.h>
#include <daos_fs.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <unistd.h>
#include <vector>

int main(int argc, char **argv) {
    std::string pool = "Pool1", cont = "lmcache";
    std::string single_key;                    // legacy single-key path
    std::string key_prefix = "kv_65536B_";     // for batch mode
    std::string parent_name;                   // optional subdir to write under
    size_t sz = 65536;
    int    start = 0;
    int    count = 1;
    int    width = 7;                          // key_prefix + printf("%07d", idx)
    int    num_threads = 1;                    // parallel worker threads
    bool   read_mode = false;                  // read instead of write
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w) -> const char * {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing arg for %s\n", w); std::exit(1); }
            return argv[++i];
        };
        if      (a == "--pool")       pool       = nxt("--pool");
        else if (a == "--cont")       cont       = nxt("--cont");
        else if (a == "--key")        single_key = nxt("--key");
        else if (a == "--key-prefix") key_prefix = nxt("--key-prefix");
        else if (a == "--parent")     parent_name= nxt("--parent");
        else if (a == "--size")       sz         = std::atol(nxt("--size"));
        else if (a == "--start")      start      = std::atoi(nxt("--start"));
        else if (a == "--count")      count      = std::atoi(nxt("--count"));
        else if (a == "--width")      width      = std::atoi(nxt("--width"));
        else if (a == "--threads")    num_threads= std::atoi(nxt("--threads"));
        else if (a == "--read")       read_mode  = true;
        else if (a == "-h" || a == "--help") {
            std::cout << argv[0] << " [--pool NAME] [--cont NAME]\n"
                      << "  single: --key NAME --size N\n"
                      << "  batch:  --key-prefix P --size N --start K --count N [--width 7]\n";
            return 0;
        }
    }

    // Read one block of random data from /dev/urandom and reuse it for every
    // key (cheap and fine for dry-run reads; we don't need per-key uniqueness).
    std::vector<char> buf(sz);
    int fd = ::open("/dev/urandom", O_RDONLY);
    if (fd < 0) { std::perror("/dev/urandom"); return 2; }
    size_t got = 0;
    while (got < sz) {
        ssize_t n = ::read(fd, buf.data() + got, sz - got);
        if (n <= 0) { std::perror("read /dev/urandom"); return 3; }
        got += (size_t)n;
    }
    ::close(fd);

    int rc = daos_init();
    if (rc) { std::fprintf(stderr, "daos_init rc=%d\n", rc); return 4; }

    daos_handle_t poh{};
    rc = daos_pool_connect(pool.c_str(), nullptr, DAOS_PC_RW, &poh, nullptr, nullptr);
    if (rc) { std::fprintf(stderr, "pool_connect rc=%d\n", rc); return 5; }

    daos_handle_t coh{};
    rc = daos_cont_open(poh, cont.c_str(), DAOS_COO_RW, &coh, nullptr, nullptr);
    if (rc) { std::fprintf(stderr, "cont_open rc=%d\n", rc); return 6; }

    dfs_t *dfs = nullptr;
    rc = dfs_mount(poh, coh, O_RDWR, &dfs);
    if (rc) { std::fprintf(stderr, "dfs_mount rc=%d\n", rc); return 7; }

    /* Optional: lookup a subdirectory under root to use as the parent for
     * every created/read file. Shared across threads (dfs_obj_t is safe for
     * concurrent dfs_open/dfs_lookup callers within one process). */
    dfs_obj_t *parent_obj = nullptr;
    if (!parent_name.empty()) {
        mode_t pmode = 0;
        int pr = dfs_lookup_rel(dfs, nullptr, parent_name.c_str(), O_RDWR,
                                &parent_obj, &pmode, nullptr);
        if (pr) { std::fprintf(stderr, "dfs_lookup_rel('%s') rc=%d\n", parent_name.c_str(), pr); return 8; }
        std::fprintf(stdout, "using parent='%s'\n", parent_name.c_str());
    }

    auto write_one_timed = [&](const std::string &key, uint64_t *open_us, uint64_t *write_us) -> int {
        struct timespec t0, t1, t2;
        dfs_obj_t *obj = nullptr;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        int r = dfs_open(dfs, parent_obj, key.c_str(),
                         S_IFREG | 0644, O_CREAT | O_RDWR | O_TRUNC,
                         0, 0, nullptr, &obj);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (r) { std::fprintf(stderr, "dfs_open(create '%s') rc=%d\n", key.c_str(), r); return r; }
        d_iov_t iov{}; d_sg_list_t sgl{};
        d_iov_set(&iov, buf.data(), sz);
        sgl.sg_nr = 1; sgl.sg_iovs = &iov;
        r = dfs_write(dfs, obj, &sgl, 0, nullptr);
        clock_gettime(CLOCK_MONOTONIC, &t2);
        if (r) { std::fprintf(stderr, "dfs_write('%s') rc=%d\n", key.c_str(), r); }
        dfs_release(obj);
        *open_us  = (t1.tv_sec - t0.tv_sec) * 1000000ULL + (t1.tv_nsec - t0.tv_nsec) / 1000;
        *write_us = (t2.tv_sec - t1.tv_sec) * 1000000ULL + (t2.tv_nsec - t1.tv_nsec) / 1000;
        return r;
    };
    std::vector<char> rbuf(sz);
    auto read_one_timed = [&](const std::string &key, uint64_t *open_us, uint64_t *read_us) -> int {
        struct timespec t0, t1, t2;
        dfs_obj_t *obj = nullptr;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        int r = dfs_open(dfs, parent_obj, key.c_str(),
                         S_IFREG | 0644, O_RDONLY, 0, 0, nullptr, &obj);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (r) { std::fprintf(stderr, "dfs_open(read '%s') rc=%d\n", key.c_str(), r); return r; }
        d_iov_t iov{}; d_sg_list_t sgl{};
        d_iov_set(&iov, rbuf.data(), sz);
        sgl.sg_nr = 1; sgl.sg_iovs = &iov;
        daos_size_t got = 0;
        r = dfs_read(dfs, obj, &sgl, 0, &got, nullptr);
        clock_gettime(CLOCK_MONOTONIC, &t2);
        if (r) { std::fprintf(stderr, "dfs_read('%s') rc=%d\n", key.c_str(), r); }
        dfs_release(obj);
        *open_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL + (t1.tv_nsec - t0.tv_nsec) / 1000;
        *read_us = (t2.tv_sec - t1.tv_sec) * 1000000ULL + (t2.tv_nsec - t1.tv_nsec) / 1000;
        return r;
    };
    auto write_one = [&](const std::string &key) -> int {
        uint64_t ou, wu; return write_one_timed(key, &ou, &wu);
    };

    using clk = std::chrono::steady_clock;
    auto t0 = clk::now();

    if (!single_key.empty()) {
        if (write_one(single_key) != 0) return 8;
        std::fprintf(stdout, "ok  pool=%s cont=%s key=%s size=%zu  first16=",
                     pool.c_str(), cont.c_str(), single_key.c_str(), sz);
        for (size_t i = 0; i < 16 && i < sz; ++i) std::fprintf(stdout, "%02x", (unsigned char)buf[i]);
        std::fprintf(stdout, "\n");
    } else {
        std::fprintf(stdout, "PER_FILE idx,open_us,%s_us,tid\n", read_mode ? "read" : "write");
        std::mutex m_stdout;
        std::vector<std::thread> workers;
        std::atomic<int> ok{0}, fail{0};
        int per_thread = count / num_threads;

        for (int tid = 0; tid < num_threads; ++tid) {
            int lo = tid * per_thread;
            int hi = (tid == num_threads - 1) ? count : lo + per_thread;
            workers.emplace_back([&, tid, lo, hi]() {
                char kbuf[64];
                for (int i = lo; i < hi; ++i) {
                    std::snprintf(kbuf, sizeof(kbuf), "%s%0*d", key_prefix.c_str(), width, start + i);
                    uint64_t ou = 0, iu = 0;
                    int r = read_mode ? read_one_timed(kbuf, &ou, &iu)
                                      : write_one_timed(kbuf, &ou, &iu);
                    if (r == 0) {
                        ok++;
                        std::lock_guard<std::mutex> g(m_stdout);
                        std::fprintf(stdout, "PER_FILE %d,%lu,%lu,%d\n", i, ou, iu, tid);
                    } else { fail++; }
                }
            });
        }
        for (auto &w : workers) w.join();

        auto t1 = clk::now();
        double secs = std::chrono::duration<double>(t1 - t0).count();
        std::fprintf(stdout, "batch mode=%s threads=%d ok=%d fail=%d  size=%zu  time=%.2fs  rate=%.1f keys/s\n",
                     read_mode ? "read" : "write",
                     num_threads, ok.load(), fail.load(), sz, secs, ok.load() / secs);
    }

    dfs_umount(dfs);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();
    return 0;
}
