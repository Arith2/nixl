// test_client.cpp — standalone dry-run client for gather_daemon.
//
// Exercises the two planes end-to-end with no NIXL and no Ceph in the loop:
//   1. Control plane: POST /agg/_open (with client QP params + chunk list)
//   2. Data  plane:   receive daemon's RDMA_WRITE_WITH_IMM into a
//                      locally-registered target buffer
//
// Usage:
//   test_client --daemon http://hsc-21:8080 --cont lmcache
//               --key kv_65536B_0000000 --len 65536
//
// Prints the layer completions it receives and (optionally) a hex dump.

#include "agg_policy.h"
#include "agg_proto.h"
#include "rdma_common.h"
#include "third_party/httplib.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <curl/curl.h>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;
using namespace std::chrono;
using aggproto::OpenReq;
using aggproto::OpenResp;
using aggproto::ChunkRef;
using aggproto::LayerRange;
using aggproto::TargetBuf;
using aggproto::QpInfo;

// Small CLI parser
struct Args {
    std::string daemon      = "http://hsc-21:8080";
    std::string cont        = "lmcache";
    std::string key         = "kv_65536B_0000000";
    uint64_t    len         = 65536;
    std::string rdma_dev;
    int         rdma_port   = -1;
    int         rdma_gid_idx = -1;
    bool        hexdump     = false;
    // L2 via-Ceph mode
    bool        via_ceph     = false;
    std::string ceph_url     = "http://localhost:8000";
    std::string ceph_bucket  = "lmcache";
    std::string access_key   = "0555b35654ad1656d804";
    std::string secret_key   = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
    std::string region       = "default";
    // kv_layer workload mode
    std::string agg_mode     = "custom";    // "custom" (1 chunk) or "kv_layer"
    uint32_t    isl          = 1024;
    uint32_t    chunk_tokens = 16;
    uint32_t    num_layers   = 32;
    uint32_t    kv_bytes_per_tok_layer = 4096;
    int32_t     agg_size_override = -1;     // -1 = policy-driven
    std::string key_prefix   = "";           // "" = auto-derive from chunk size
};

// ── HTTP helpers: direct or via-Ceph (AWS sigv4 PUT) ───────────────────────
static size_t curl_write_cb(char* p, size_t s, size_t n, void* ud) {
    auto* out = static_cast<std::string*>(ud);
    out->append(p, s * n);
    return s * n;
}

// Direct HTTP POST via cpp-httplib (L1 mode).
static long http_post_direct(httplib::Client& client, const std::string& path,
                             const std::string& body, std::string& out_body) {
    auto r = client.Post(path.c_str(), body, "application/json");
    if (!r) return 0;
    out_body = r->body;
    return r->status;
}

// AWS sigv4-signed S3 PUT via libcurl (L2 mode). The Ceph RGW option-C
// handler will detect the x-amz-rdma-batch: 1 marker, parse the JSON body,
// forward to the gather daemon, and return the daemon's response body.
static long http_put_via_ceph(const Args& a, const std::string& s3_key,
                              const std::string& body, std::string& out_body) {
    std::string url = a.ceph_url + "/" + a.ceph_bucket + "/" + s3_key;
    CURL* c = curl_easy_init();
    if (!c) return 0;
    struct curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, "x-amz-rdma-batch: 1");
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)body.size());
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &out_body);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 30L);
    std::string provider = "aws:amz:" + a.region + ":s3";
    curl_easy_setopt(c, CURLOPT_AWS_SIGV4, provider.c_str());
    std::string userpwd = a.access_key + ":" + a.secret_key;
    curl_easy_setopt(c, CURLOPT_USERPWD, userpwd.c_str());
    CURLcode rc = curl_easy_perform(c);
    long code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
    if (rc != CURLE_OK) {
        std::fprintf(stderr, "curl_easy_perform rc=%d (%s) url=%s\n",
                     rc, curl_easy_strerror(rc), url.c_str());
    }
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(c);
    return code;
}

static void usage() {
    std::cout <<
        "test_client [--daemon URL] [--cont NAME] [--key NAME] [--len N]\n"
        "            [--rdma-dev NAME] [--rdma-port N] [--rdma-gid-idx N]\n"
        "            [--hexdump]\n";
}

static bool parse(int argc, char **argv, Args &a) {
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        auto nxt = [&](const char *w) -> const char * {
            if (i + 1 >= argc) { std::cerr << "missing arg for " << w << std::endl; return nullptr; }
            return argv[++i];
        };
        if      (s == "--daemon")       { auto v = nxt("--daemon"); if (!v) return false; a.daemon = v; }
        else if (s == "--cont")         { auto v = nxt("--cont"); if (!v) return false; a.cont = v; }
        else if (s == "--key")          { auto v = nxt("--key"); if (!v) return false; a.key = v; }
        else if (s == "--len")          { auto v = nxt("--len"); if (!v) return false; a.len = std::atol(v); }
        else if (s == "--rdma-dev")     { auto v = nxt("--rdma-dev"); if (!v) return false; a.rdma_dev = v; }
        else if (s == "--rdma-port")    { auto v = nxt("--rdma-port"); if (!v) return false; a.rdma_port = std::atoi(v); }
        else if (s == "--rdma-gid-idx") { auto v = nxt("--rdma-gid-idx"); if (!v) return false; a.rdma_gid_idx = std::atoi(v); }
        else if (s == "--hexdump")      { a.hexdump = true; }
        else if (s == "--via-ceph")     { a.via_ceph = true; }
        else if (s == "--ceph-url")     { auto v = nxt("--ceph-url"); if (!v) return false; a.ceph_url = v; }
        else if (s == "--ceph-bucket")  { auto v = nxt("--ceph-bucket"); if (!v) return false; a.ceph_bucket = v; }
        else if (s == "--access-key")   { auto v = nxt("--access-key"); if (!v) return false; a.access_key = v; }
        else if (s == "--secret-key")   { auto v = nxt("--secret-key"); if (!v) return false; a.secret_key = v; }
        else if (s == "--agg-mode")     { auto v = nxt("--agg-mode"); if (!v) return false; a.agg_mode = v; }
        else if (s == "--isl")          { auto v = nxt("--isl"); if (!v) return false; a.isl = (uint32_t)std::atol(v); }
        else if (s == "--chunk-tokens") { auto v = nxt("--chunk-tokens"); if (!v) return false; a.chunk_tokens = (uint32_t)std::atol(v); }
        else if (s == "--num-layers")   { auto v = nxt("--num-layers"); if (!v) return false; a.num_layers = (uint32_t)std::atol(v); }
        else if (s == "--agg-size")     { auto v = nxt("--agg-size"); if (!v) return false; a.agg_size_override = std::atoi(v); }
        else if (s == "--key-prefix")   { auto v = nxt("--key-prefix"); if (!v) return false; a.key_prefix = v; }
        else if (s == "-h" || s == "--help") { usage(); return false; }
        else { std::cerr << "unknown flag: " << s << std::endl; return false; }
    }
    return true;
}

// Split http://host:port into {host, port}
static bool split_url(const std::string &url, std::string &host, int &port) {
    auto pos = url.find("://");
    std::string rest = (pos == std::string::npos) ? url : url.substr(pos + 3);
    auto colon = rest.find(':');
    if (colon == std::string::npos) { host = rest; port = 80; return true; }
    host = rest.substr(0, colon);
    port = std::atoi(rest.c_str() + colon + 1);
    return true;
}

int main(int argc, char **argv) {
    Args a;
    if (!parse(argc, argv, a)) return 1;

    // 1. Open RDMA device + port
    rdmac::PortInfo pi;
    if (!rdmac::open_first_port(pi, a.rdma_dev, a.rdma_port, a.rdma_gid_idx)) return 2;
    std::cout << "RDMA: dev=" << pi.dev_name << " port=" << (int)pi.port
              << " gid_idx=" << (int)pi.gid_idx << " gid=" << rdmac::gid_to_str(pi.gid)
              << std::endl;

    ibv_pd *pd = ibv_alloc_pd(pi.ctx);
    if (!pd) { std::cerr << "ibv_alloc_pd failed\n"; return 3; }

    ibv_cq *cq = ibv_create_cq(pi.ctx, 1024, nullptr, nullptr, 0);
    if (!cq) { std::cerr << "ibv_create_cq failed\n"; return 4; }

    // 2. Target buffer
    //    kv_layer: fixed 512 MiB ring (= 32 × 16 MiB groups). When the logical
    //    load is larger than the ring, the daemon overwrites earlier groups
    //    (benchmark-mode: we only measure timing, not accumulated bytes).
    size_t target_sz;
    if (a.agg_mode == "kv_layer") {
        target_sz = 512ULL * 1024 * 1024;
    } else {
        target_sz = a.len + 4096;
    }
    void *target_buf = aligned_alloc(4096, target_sz);
    if (!target_buf) { std::cerr << "aligned_alloc(" << target_sz << ") failed\n"; return 5; }
    std::memset(target_buf, 0xAA, target_sz);  // poison so we can detect the write
    ibv_mr *mr = ibv_reg_mr(pd, target_buf, target_sz,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) { std::cerr << "ibv_reg_mr rc=" << errno << " (" << strerror(errno) << ")\n"; return 5; }
    std::cout << "target: " << target_sz << " B at " << target_buf
              << " rkey=0x" << std::hex << mr->rkey << std::dec << std::endl;

    // 3. Create client QP
    ibv_qp_init_attr ia{};
    ia.send_cq = cq; ia.recv_cq = cq;
    ia.cap.max_send_wr  = 16; ia.cap.max_recv_wr  = 1024;
    ia.cap.max_send_sge = 1;  ia.cap.max_recv_sge = 1;
    ia.qp_type = IBV_QPT_RC;
    ibv_qp *qp = ibv_create_qp(pd, &ia);
    if (!qp) { std::cerr << "ibv_create_qp failed\n"; return 6; }
    if (!rdmac::modify_qp_init(qp, pi.port)) return 7;

    QpInfo client_qp;
    client_qp.qpn = qp->qp_num;
    std::mt19937_64 rng{std::random_device{}()};
    client_qp.psn = (uint32_t)(rng() & 0xFFFFFF);
    client_qp.lid = pi.pattr.lid;
    client_qp.mtu = (1u << (pi.pattr.active_mtu + 7));
    client_qp.port_num = pi.port;
    client_qp.gid = rdmac::gid_to_str(pi.gid);

    std::cout << "client QP: qpn=" << client_qp.qpn << " psn=" << client_qp.psn << std::endl;

    // 4. Pre-post 256 recv WRs so we can drain inbound WRITE_WITH_IMM completions.
    //    (Posting in INIT is legal; they go live once the QP reaches RTR.)
    for (int i = 0; i < 256; ++i) rdmac::post_dummy_recv(qp, 0x1000 + i);

    // 5. Two-phase protocol:
    //    (a) POST /agg/_session with client_qp → daemon allocates QP, replies server_qp
    //    (b) Transition our QP to RTR/RTS
    //    (c) POST /agg/_open with session_id → daemon dispatches work + pushes
    std::string host; int port = 8080;
    split_url(a.daemon, host, port);
    httplib::Client http(host, port);
    http.set_connection_timeout(10, 0);
    http.set_read_timeout(10, 0);

    OpenResp resp;

    // Helper lambda: do one HTTP round-trip, picking transport based on --via-ceph.
    auto do_post = [&](const std::string& daemon_path, const std::string& s3_key,
                       const std::string& body, std::string& out_body) -> long {
        if (a.via_ceph) {
            return http_put_via_ceph(a, s3_key, body, out_body);
        } else {
            return http_post_direct(http, daemon_path, body, out_body);
        }
    };

    // (a) Session handshake
    {
        OpenReq sreq;
        sreq.type = "agg_session_setup";
        sreq.req_id = "0";
        sreq.client_qp = client_qp;
        sreq.include_client_qp = true;
        json body = sreq;
        std::cout << (a.via_ceph ? "PUT " + a.ceph_url + "/" + a.ceph_bucket + "/agg_session"
                                 : std::string("POST /agg/_session"))
                  << "  body=" << body.dump() << std::endl;
        std::string rbody;
        long code = do_post("/agg/_session", "agg_session", body.dump(), rbody);
        if (code != 200) { std::cerr << "session HTTP " << code << " body=" << rbody << std::endl; return 9; }
        std::cout << "session resp:   " << rbody << std::endl;
        OpenResp sr; json::parse(rbody).get_to(sr);
        if (!sr.include_server_qp) { std::cerr << "no server_qp in _session reply\n"; return 10; }

        // (b) Finalize QP
        if (!rdmac::modify_qp_rtr(qp, pi.port, pi.gid_idx, sr.server_qp)) return 11;
        if (!rdmac::modify_qp_rts(qp, client_qp.psn)) return 12;
        std::cout << "client QP → RTS (peer qpn=" << sr.server_qp.qpn
                  << " psn=" << sr.server_qp.psn << ")" << std::endl;

        // (c) Trigger work
        OpenReq oreq;
        oreq.type = "agg_open";
        oreq.session_id = sr.session_id;
        oreq.req_id = "00000001";
        oreq.target = TargetBuf{(uint64_t)(uintptr_t)target_buf, mr->rkey, target_sz};
        oreq.include_client_qp = false;

        if (a.agg_mode == "kv_layer") {
            const uint32_t per_chunk_bytes    = a.chunk_tokens * a.kv_bytes_per_tok_layer;
            const uint32_t chunks_per_layer   = a.isl / a.chunk_tokens;
            const uint32_t total_chunks       = chunks_per_layer * a.num_layers;
            aggpolicy::KvcacheDesc kv{a.num_layers, a.kv_bytes_per_tok_layer,
                                     a.chunk_tokens, a.isl};
            uint32_t agg_sz = (a.agg_size_override > 0) ? (uint32_t)a.agg_size_override
                                                         : aggpolicy::pick_agg_size(kv);
            std::cout << "kv_layer: isl=" << a.isl << " ct=" << a.chunk_tokens
                      << " per_chunk=" << per_chunk_bytes << " B"
                      << " total_chunks=" << total_chunks
                      << " agg_size=" << agg_sz
                      << " groups=" << aggpolicy::num_groups(kv, agg_sz)
                      << " group_bytes=" << aggpolicy::group_bytes(kv, agg_sz) << std::endl;
            // Build chunk list with auto-derived key prefix
            char prefix[32];
            std::snprintf(prefix, sizeof(prefix), "kv_%dB_", (int)per_chunk_bytes);
            std::string kp = a.key_prefix.empty() ? prefix : a.key_prefix;
            oreq.chunks.reserve(total_chunks);
            char kbuf[64];
            for (uint32_t i = 0; i < total_chunks; ++i) {
                std::snprintf(kbuf, sizeof(kbuf), "%s%07u", kp.c_str(), i);
                oreq.chunks.push_back(ChunkRef{a.cont, kbuf, 0, per_chunk_bytes});
            }
            oreq.agg_mode = "kv_layer";
            oreq.agg_params = {
                {"num_layers",       a.num_layers},
                {"chunks_per_layer", chunks_per_layer},
                {"agg_size",         agg_sz}
            };
        } else {
            // Legacy 1-chunk path
            oreq.chunks.push_back(ChunkRef{a.cont, a.key, 0, a.len});
            oreq.layer_map.push_back(LayerRange{0, 0, a.len});
            oreq.agg_mode = "custom";
        }

        json obody = oreq;
        std::cout << (a.via_ceph ? "PUT " + a.ceph_url + "/" + a.ceph_bucket + "/agg_open"
                                 : std::string("POST /agg/_open"))
                  << "   body=" << obody.dump() << std::endl;
        std::string rbody2;
        long code2 = do_post("/agg/_open", "agg_open", obody.dump(), rbody2);
        if (code2 != 200) { std::cerr << "open HTTP " << code2 << " body=" << rbody2 << std::endl; return 14; }
        std::cout << "open resp:      " << rbody2 << std::endl;
        json::parse(rbody2).get_to(resp);
    }

    // 7. Drain CQ waiting for expected_writes WRITE_WITH_IMM completions
    int expected = (int)resp.expected_writes;
    int got = 0;
    auto deadline = steady_clock::now() + seconds(120);
    auto t_poll_start = steady_clock::now();
    double ttfl_us = 0, tfl_us = 0;
    while (got < expected && steady_clock::now() < deadline) {
        ibv_wc wcs[32];
        int n = ibv_poll_cq(cq, 32, wcs);
        if (n < 0) { std::cerr << "ibv_poll_cq rc=" << n << std::endl; return 13; }
        for (int i = 0; i < n; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                std::cerr << "wr_id=" << wcs[i].wr_id
                          << " status=" << ibv_wc_status_str(wcs[i].status)
                          << " opcode=" << wcs[i].opcode << std::endl;
                continue;
            }
            if (wcs[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                uint32_t imm = ntohl(wcs[i].imm_data);
                double elapsed_us = duration_cast<microseconds>(steady_clock::now() - t_poll_start).count();
                if (got == 0) ttfl_us = elapsed_us;
                std::cout << "  ← imm=" << imm << " byte_len=" << wcs[i].byte_len
                          << "  t=" << (int)elapsed_us << "us" << std::endl;
                ++got;
                if (got == expected) tfl_us = elapsed_us;
                rdmac::post_dummy_recv(qp, wcs[i].wr_id);  // keep pool non-empty
            }
        }
        if (n == 0) std::this_thread::sleep_for(microseconds(100));
    }

    if (got != expected) {
        std::cerr << "TIMEOUT  got " << got << "/" << expected << " layer completions\n";
        return 14;
    }
    std::cout << "all " << got << " layer(s) received.  TTFL=" << (int)ttfl_us
              << "us  TFL=" << (int)tfl_us << "us  (wall from open-resp)\n";

    if (a.hexdump) {
        const uint8_t *p = (const uint8_t*)target_buf;
        size_t n = std::min<size_t>(a.len, 128);
        std::cout << "first " << n << " bytes:";
        for (size_t i = 0; i < n; ++i) {
            if (i % 16 == 0) std::printf("\n  %04zx:", i);
            std::printf(" %02x", p[i]);
        }
        std::printf("\n");
    }

    // Sanity: at least one byte should not be 0xAA (our poison value)
    {
        size_t poison = 0;
        const uint8_t *p = (const uint8_t*)target_buf;
        for (size_t i = 0; i < a.len; ++i) if (p[i] == 0xAA) ++poison;
        std::cout << "poison-byte count: " << poison << " / " << a.len << std::endl;
    }

    // Cleanup
    ibv_destroy_qp(qp);
    ibv_dereg_mr(mr);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    ibv_close_device(pi.ctx);
    free(target_buf);
    return 0;
}
