// Standalone test for x-amz-kvcache streaming descriptor.
// Compile inside nixl-gpu container:
//   g++ -std=c++17 -O2 -o test_kvcache test_kvcache.cpp \
//       -I/workspace/nixl/src/plugins/obj -I/workspace/nixl/src/plugins/obj/s3 \
//       -I/usr/local/nixl/include -I/workspace/nixl/src/include \
//       -L/usr/local/nixl/lib/x86_64-linux-gnu \
//       $(pkg-config --cflags --libs aws-cpp-sdk-s3) \
//       -lnixl -lpthread
//
// Usage:
//   # First populate chunks with nixlbench PUT, then:
//   ./test_kvcache <num_chunks> <num_layers> <kv_per_token_per_layer> <tokens_per_chunk> <rdma_port>
//
// Example (Llama 70B TP=8, 16 chunks of 256 tokens):
//   ./test_kvcache 16 80 256 256 7471

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <future>

#include "client.h"
#include "rdma_ctx.h"
#include "nixl_types.h"

int main(int argc, char** argv) {
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0]
                  << " <num_chunks> <num_layers> <kv_per_tok_layer> <tok_per_chunk> <rdma_port>"
                  << std::endl;
        return 1;
    }

    int num_chunks = std::atoi(argv[1]);
    int num_layers = std::atoi(argv[2]);
    size_t kv_per_token_per_layer = std::atoi(argv[3]);
    size_t tokens_per_chunk = std::atoi(argv[4]);
    int rdma_port = std::atoi(argv[5]);

    size_t layer_slice = kv_per_token_per_layer * tokens_per_chunk;
    size_t layer_total = layer_slice * num_chunks;
    size_t full_chunk = num_layers * layer_slice;
    size_t total_data = num_layers * layer_total;

    std::cout << "KVCache test: chunks=" << num_chunks << " layers=" << num_layers
              << " kv/tok/layer=" << kv_per_token_per_layer
              << " tok/chunk=" << tokens_per_chunk << std::endl;
    std::cout << "  layer_slice=" << layer_slice << " layer_total=" << layer_total
              << " full_chunk=" << full_chunk
              << " total=" << total_data / 1024 / 1024 << " MB" << std::endl;

    // Create S3 client
    nixl_b_params_t params;
    params["obj_endpoint_override"] = "http://10.93.244.74:8000";
    params["obj_scheme"] = "http";
    params["obj_bucket_name"] = "lmcache";
    params["obj_region"] = "us-east-1";
    params["obj_access_key"] = "0555b35654ad1656d804";
    params["obj_secret_key"] = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";

    awsS3Client client(&params);

    // Setup RDMA context
    std::unique_ptr<RdmaContext> rdma_ctx;
    if (rdma_port > 0) {
        rdma_ctx = std::make_unique<RdmaContext>("10.93.244.74", rdma_port);
        if (!rdma_ctx->isEnabled()) {
            std::cerr << "RDMA connection failed" << std::endl;
            return 1;
        }
    }

    // Allocate receive buffer
    void* buf = aligned_alloc(4096, total_data);
    if (!buf) { std::cerr << "malloc failed" << std::endl; return 1; }
    memset(buf, 0, total_data);

    // Register with RDMA
    std::optional<std::string> rdma_token;
    if (rdma_ctx) {
        rdma_ctx->registerMR(buf, total_data);
        rdma_token = rdma_ctx->buildToken(buf, total_data, 0);
    }

    // Generate chunk keys (matching nixlbench prepop pattern)
    std::vector<std::string> chunk_keys;
    for (int i = 0; i < num_chunks; i++) {
        char key[128];
        snprintf(key, sizeof(key), "prepop_%zuB_0_0_%06d",
                 full_chunk, i);
        chunk_keys.push_back(key);
    }

    std::cout << "Chunk keys: " << chunk_keys[0] << " ... " << chunk_keys.back() << std::endl;

    // ===== Benchmark 1: Bulk fetch (N separate GETs) =====
    {
        std::cout << "\n=== Bulk fetch (" << num_chunks << " separate GETs) ===" << std::endl;
        auto t0 = std::chrono::steady_clock::now();

        for (int i = 0; i < num_chunks; i++) {
            std::promise<bool> p;
            auto f = p.get_future();
            client.getObjectAsync(
                chunk_keys[i],
                reinterpret_cast<uintptr_t>(buf) + i * full_chunk,
                full_chunk, 0,
                [&p](bool ok) { p.set_value(ok); },
                rdma_token);
            if (!f.get()) {
                std::cerr << "Bulk GET failed for chunk " << i << std::endl;
                break;
            }
        }

        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << "  Total: " << ms << " ms ("
                  << (total_data / 1024.0 / 1024.0 / 1024.0) / (ms / 1000.0)
                  << " GB/s)" << std::endl;
        std::cout << "  TTFL (first layer available): " << ms << " ms (must wait for all)" << std::endl;
    }

    // ===== Benchmark 2: Streaming (1 x-amz-kvcache request) =====
    {
        std::cout << "\n=== Streaming (1 x-amz-kvcache request) ===" << std::endl;
        memset(buf, 0, total_data);

        auto t0 = std::chrono::steady_clock::now();

        std::promise<bool> p;
        auto f = p.get_future();
        client.getKVCacheAsync(
            chunk_keys, num_layers,
            kv_per_token_per_layer, tokens_per_chunk,
            reinterpret_cast<uintptr_t>(buf), total_data,
            [&p](bool ok) { p.set_value(ok); },
            rdma_token);
        bool ok = f.get();

        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if (ok) {
            std::cout << "  Total: " << ms << " ms ("
                      << (total_data / 1024.0 / 1024.0 / 1024.0) / (ms / 1000.0)
                      << " GB/s)" << std::endl;
            // First layer is available after 1 HTTP RTT + server reads + 1 layer RDMA
            double est_first_layer = 2.5 + layer_total / (5.0 * 1024 * 1024 * 1024) * 1000;
            std::cout << "  TTFL (estimated first layer): ~" << est_first_layer << " ms" << std::endl;
        } else {
            std::cout << "  FAILED" << std::endl;
        }
    }

    // Cleanup
    if (rdma_ctx) rdma_ctx->deregisterMR(buf);
    free(buf);

    return 0;
}
