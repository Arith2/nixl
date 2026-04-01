// Benchmark cudaMemcpy Host-to-Device for various block sizes.
// Compile: nvcc -o bench_cudamemcpy bench_cudamemcpy.cu
// Run:     ./bench_cudamemcpy

#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#define CHECK_CUDA(call) do { \
    cudaError_t err = (call); \
    if (err != cudaSuccess) { \
        fprintf(stderr, "CUDA error %s:%d: %s\n", __FILE__, __LINE__, \
                cudaGetErrorString(err)); \
        exit(1); \
    } \
} while(0)

int main() {
    size_t sizes[] = {
        64UL << 10,   // 64KB
        256UL << 10,  // 256KB
        1UL << 20,    // 1MB
        4UL << 20,    // 4MB
        16UL << 20,   // 16MB
        64UL << 20,   // 64MB
        256UL << 20,  // 256MB
    };
    const char* names[] = {"64KB", "256KB", "1MB", "4MB", "16MB", "64MB", "256MB"};
    int n_sizes = sizeof(sizes) / sizeof(sizes[0]);
    int warmup = 10;
    int iters = 100;

    // Allocate pinned host memory and device memory for max size
    size_t max_size = 256UL << 20;
    void* h_buf;
    void* d_buf;
    CHECK_CUDA(cudaMallocHost(&h_buf, max_size));
    CHECK_CUDA(cudaMalloc(&d_buf, max_size));
    memset(h_buf, 0xAB, max_size);

    cudaEvent_t start, stop;
    CHECK_CUDA(cudaEventCreate(&start));
    CHECK_CUDA(cudaEventCreate(&stop));

    printf("BlockSize,BW_GBs,Avg_Lat_us,Direction\n");

    for (int s = 0; s < n_sizes; s++) {
        size_t sz = sizes[s];

        // Host-to-Device
        for (int i = 0; i < warmup; i++)
            CHECK_CUDA(cudaMemcpy(d_buf, h_buf, sz, cudaMemcpyHostToDevice));
        CHECK_CUDA(cudaDeviceSynchronize());

        CHECK_CUDA(cudaEventRecord(start));
        for (int i = 0; i < iters; i++)
            CHECK_CUDA(cudaMemcpy(d_buf, h_buf, sz, cudaMemcpyHostToDevice));
        CHECK_CUDA(cudaEventRecord(stop));
        CHECK_CUDA(cudaEventSynchronize(stop));

        float ms_h2d;
        CHECK_CUDA(cudaEventElapsedTime(&ms_h2d, start, stop));
        float avg_h2d = ms_h2d / iters;
        float bw_h2d = (float)sz / (avg_h2d / 1000.0f) / (1024.0f * 1024.0f * 1024.0f);

        printf("%s,%.3f,%.1f,H2D\n", names[s], bw_h2d, avg_h2d * 1000.0f);

        // Device-to-Host
        for (int i = 0; i < warmup; i++)
            CHECK_CUDA(cudaMemcpy(h_buf, d_buf, sz, cudaMemcpyDeviceToHost));
        CHECK_CUDA(cudaDeviceSynchronize());

        CHECK_CUDA(cudaEventRecord(start));
        for (int i = 0; i < iters; i++)
            CHECK_CUDA(cudaMemcpy(h_buf, d_buf, sz, cudaMemcpyDeviceToHost));
        CHECK_CUDA(cudaEventRecord(stop));
        CHECK_CUDA(cudaEventSynchronize(stop));

        float ms_d2h;
        CHECK_CUDA(cudaEventElapsedTime(&ms_d2h, start, stop));
        float avg_d2h = ms_d2h / iters;
        float bw_d2h = (float)sz / (avg_d2h / 1000.0f) / (1024.0f * 1024.0f * 1024.0f);

        printf("%s,%.3f,%.1f,D2H\n", names[s], bw_d2h, avg_d2h * 1000.0f);
    }

    CHECK_CUDA(cudaEventDestroy(start));
    CHECK_CUDA(cudaEventDestroy(stop));
    CHECK_CUDA(cudaFreeHost(h_buf));
    CHECK_CUDA(cudaFree(d_buf));

    return 0;
}
