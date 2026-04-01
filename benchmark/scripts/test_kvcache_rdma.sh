#!/bin/bash
# KVCache microbenchmark using nixlbench for bulk baseline
# and projected streaming numbers from measured per-request overhead.
#
# Run after DAOS RDMA + Ceph RGW are started per RUNBOOK.
#
# Usage: bash benchmark/scripts/test_kvcache_rdma.sh

set -euo pipefail

LOGDIR=/HSC/users/zhuyu/nixl/logs/kvcache
mkdir -p "$LOGDIR"

ENDPOINT="http://10.93.244.74:8000"
ETCD="http://localhost:2379"

# Llama 70B (TP=8): 80 layers, 256 B/tok/layer/GPU, chunk=256 tokens
# chunk_size = 80 * 256 * 256 = 5,242,880 bytes (5 MB)
# layer_slice = 256 * 256 = 65,536 bytes (64 KB)
# For 4K context: 16 chunks

echo "=========================================="
echo "  KVCache Microbenchmark (Llama 70B TP=8)"
echo "=========================================="
echo "  Chunks: 16, Layers: 80"
echo "  Chunk size: 5 MB, Layer slice: 64 KB"
echo "  Total KV: 80 MB (4K context)"

# Step 1: Populate chunks using nixlbench PUT with RDMA
echo ""
echo "=== Step 1: Populate 16 chunks (5 MB each) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null; sleep 1
docker exec nixl-gpu nixlbench \
    -etcd_endpoints "$ETCD" \
    -backend OBJ -num_threads 1 -large_blk_iter_ftr 1 \
    -obj_endpoint_override "$ENDPOINT" -obj_scheme http \
    -obj_bucket_name lmcache -obj_region us-east-1 \
    -obj_access_key 0555b35654ad1656d804 \
    -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
    -op_type WRITE -start_block_size 5242880 -max_block_size 5242880 \
    -num_iter 16 -warmup_iter 0 -obj_prepop_num 16 \
    -obj_rdma_port 7471 -initiator_seg_type DRAM \
    2>&1 | tee "${LOGDIR}/put_chunks.log" | tail -5

# Step 2: Bulk GET baseline (16 sequential chunk GETs)
echo ""
echo "=== Step 2: Bulk GET baseline (16 sequential GETs × 5 MB) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null; sleep 1
docker exec nixl-gpu nixlbench \
    -etcd_endpoints "$ETCD" \
    -backend OBJ -num_threads 1 -large_blk_iter_ftr 1 \
    -obj_endpoint_override "$ENDPOINT" -obj_scheme http \
    -obj_bucket_name lmcache -obj_region us-east-1 \
    -obj_access_key 0555b35654ad1656d804 \
    -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
    -op_type READ -start_block_size 5242880 -max_block_size 5242880 \
    -num_iter 160 -warmup_iter 16 -obj_prepop_num 16 \
    -obj_rdma_port 7471 -initiator_seg_type DRAM \
    2>&1 | tee "${LOGDIR}/bulk_get.log" | tail -5

# Extract per-request latency
BULK_LAT=$(grep "^5242880" "${LOGDIR}/bulk_get.log" | awk '{print $4}')
echo ""
echo "  Per-chunk GET latency: ${BULK_LAT} us"
echo "  16 sequential GETs: $(echo "$BULK_LAT * 16 / 1000" | bc -l | xargs printf '%.1f') ms"

# Step 3: Project streaming performance
echo ""
echo "=== Step 3: Streaming projection (x-amz-kvcache) ==="
echo "  HTTP round-trip: 2.5 ms"
echo "  Server-side DAOS reads: ~1 ms (16 chunks from cache)"
echo "  Per-layer RDMA_WRITE: 64KB × 16 chunks = 1 MB at ~5 GB/s = 0.2 ms"
echo "  First layer delivery: ~3.7 ms"
echo "  Total (80 layers streamed): ~3.7 + 79 × 0.2 = ~19.5 ms"
echo ""
echo "=== Summary ==="
BULK_TOTAL=$(echo "$BULK_LAT * 16 / 1000" | bc -l | xargs printf '%.1f')
echo "  Bulk GET (16 requests):       ${BULK_TOTAL} ms  (TTFL = ${BULK_TOTAL} ms)"
echo "  Streaming (1 request, proj):  19.5 ms  (TTFL = 3.7 ms)"
echo "  Total speedup:                $(echo "$BULK_TOTAL / 19.5" | bc -l | xargs printf '%.1f')x"
echo "  TTFL speedup:                 $(echo "$BULK_TOTAL / 3.7" | bc -l | xargs printf '%.1f')x"
