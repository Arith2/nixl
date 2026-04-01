# Section 5.6: KV Cache Streaming Benchmark

## Overview

Benchmarks x-amz-kvcache streaming vs baseline S3 access patterns for KV cache loading.
Measures per-request latency (cold reads from 1.28 TB dataset), then calculates per-KV-cache-load time.

**Model**: Llama 70B (TP=8), 80 layers, 256 B/tok/layer/GPU, chunk=256 tokens
**Block size**: 5,242,880 bytes (5 MB per chunk)
**Layer slice**: 65,536 bytes (64 KB per layer per chunk)

## Methods

| # | Method | Description | Requests per load (N chunks) |
|---|--------|-------------|------------------------------|
| 1 | Chunk-wise TCP | N sequential full GETs over TCP | N |
| 2 | Chunk-wise RDMA | N sequential full GETs over RDMA | N |
| 3 | Layer-wise TCP | N×80 range-GETs (64KB) over TCP | N × 80 |
| 4 | Layer-wise RDMA | N×80 range-GETs (64KB) over RDMA | N × 80 |
| 5 | x-amz-kvcache (agg=80) | 1 request, chunk-wise emulation | 1 |
| 6 | x-amz-kvcache (agg=1) | 1 request, true layer-wise streaming | 1 |

## Chunk counts: N = 1, 4, 16, 64, 256

| N | Context tokens | Total KV per GPU |
|---|---------------|-----------------|
| 1 | 256 | 5 MB |
| 4 | 1,024 | 20 MB |
| 16 | 4,096 | 80 MB |
| 64 | 16,384 | 320 MB |
| 256 | 65,536 | 1,280 MB |

## Measurement Approach

- **Prepopulate**: 262,144 chunks × 5 MB = 1.28 TB (exceeds DAOS SCM cache, ensures cold reads)
- **Methods 1-4**: measure per-request latency (1000 iterations), calculate per-load = N × per_request
- **Methods 5-6**: measure per-load latency directly (1 request = 1 full load)
- **Single thread, iodepth=1** for all methods (fair comparison)
- Prepop rotation cycles through 262K keys — each request reads a unique chunk (cold)

## Prerequisites

1. DAOS RDMA server on hsc-21
2. Ceph RGW (kvcache-enabled) on hsc-14 with **3 TB pool**
3. nixl-gpu container on hsc-12
4. etcd on hsc-12

## Running

### Full run (including prepopulation):
```bash
bash benchmark/scripts/run_kvbench.sh
```
Estimated time: ~60 minutes (35 min prepop + 25 min benchmarks)

### Skip prepopulation (if chunks already exist):
```bash
bash benchmark/scripts/run_kvbench.sh skip_put
```

### Monitor:
```bash
tail -f logs/kvbench/benchmark_run.log
```

## Output

Logs saved to `logs/kvbench/`:
- `put_all.log` — prepopulation
- `chunkwise_tcp.log` — Method 1 per-request latency
- `chunkwise_rdma.log` — Method 2 per-request latency
- `layerwise_tcp.log` — Method 3 per-request latency
- `layerwise_rdma.log` — Method 4 per-request latency
- `kvcache_agg80_Nchunks.log` — Method 5 per-load latency (N = 1,4,16,64,256)
- `kvcache_agg1_Nchunks.log` — Method 6 per-load latency (N = 1,4,16,64,256)

## Expected Results

Per-request latency:
- Chunk-wise TCP: ~20 ms (5 MB over TCP)
- Chunk-wise RDMA: ~5 ms (5 MB over RDMA)
- Layer-wise TCP: ~3 ms (64 KB over TCP, HTTP overhead dominates)
- Layer-wise RDMA: ~3 ms (64 KB over RDMA, HTTP overhead dominates)

Per-KV-cache-load (N=16):

| Method | Requests | Calculated load time |
|--------|----------|---------------------|
| Chunk-wise TCP | 16 | ~320 ms |
| Chunk-wise RDMA | 16 | ~80 ms |
| Layer-wise TCP | 1280 | ~3840 ms |
| Layer-wise RDMA | 1280 | ~3840 ms |
| x-amz-kvcache (agg=80) | 1 | ~48 ms |
| x-amz-kvcache (agg=1) | 1 | ~48 ms |

Key insight: Layer-wise standard S3 is **48× slower** than x-amz-kvcache (3840 ms vs 48 ms)
because each of 1280 range-GETs incurs 2.5 ms HTTP overhead.
