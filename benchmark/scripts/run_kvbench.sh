#!/bin/bash
# Section 5.6: KV Cache Streaming Benchmark
#
# Prepopulates 262144 chunks (1.28 TB) to ensure cold reads.
# Then benchmarks 6 methods × 5 chunk counts.
# Per-request latency measured; per-KV-cache-load = N × per_request.
#
# Model: Llama 70B (TP=8), 80 layers, 256 B/tok/layer, chunk=256 tokens
# Block: 5,242,880 bytes (5 MB), Layer slice: 65,536 bytes (64 KB)
#
# Usage:
#   bash benchmark/scripts/run_kvbench.sh           # full run
#   bash benchmark/scripts/run_kvbench.sh skip_put  # skip prepopulation

set -euo pipefail

BLOCK=5242880
LAYER_SLICE=65536
NUM_LAYERS=80
KV_PER_TOKEN=256
TOTAL_PREPOP=262144    # 262K chunks × 5MB = 1.28 TB
ITERS=1000             # per-request iterations for all configs
LOGDIR=/HSC/users/zhuyu/nixl/logs/kvbench
CONTAINER=nixl-gpu

ENDPOINT="http://10.93.244.74:8000"
ETCD="http://localhost:2379"

BASE_FLAGS=(
    -etcd_endpoints "$ETCD"
    -backend OBJ
    -num_threads 1
    -large_blk_iter_ftr 1
    -obj_endpoint_override "$ENDPOINT"
    -obj_scheme http
    -obj_bucket_name lmcache
    -obj_region us-east-1
    -obj_access_key 0555b35654ad1656d804
    -obj_secret_key "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
)

CHUNK_COUNTS=(1 4 16 64 256)

mkdir -p "$LOGDIR"

echo "============================================================"
echo "  Section 5.6: KV Cache Streaming Benchmark"
echo "  Model: Llama 70B (TP=8), 80 layers, 256 B/tok/layer"
echo "  Block: 5 MB, Layer: 64 KB, Total prepop: $TOTAL_PREPOP chunks"
echo "  Iterations: $ITERS per-request (cold, sequential)"
echo "============================================================"

# ─── Step 0: Prepopulate 262K chunks ──────────────────────────────────────
if [ "${1:-}" != "skip_put" ]; then
    echo ""
    echo "=== Step 0: Prepopulate $TOTAL_PREPOP chunks ($(( TOTAL_PREPOP * 5 / 1024 )) GB) ==="
    echo "  Estimated time: ~35 minutes via RDMA"
    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true
    sleep 2

    docker exec "$CONTAINER" nixlbench \
        "${BASE_FLAGS[@]}" \
        -op_type WRITE \
        -start_block_size $BLOCK -max_block_size $BLOCK \
        -num_iter $TOTAL_PREPOP -warmup_iter 0 \
        -obj_prepop_num $TOTAL_PREPOP \
        -obj_rdma_port 7471 \
        2>&1 | tee "${LOGDIR}/put_all.log" | tail -5

    echo "Prepopulation complete."
else
    echo ""
    echo "=== Skipping prepopulation (skip_put) ==="
fi

# ─── Step 1: Chunk-wise TCP ──────────────────────────────────────────────
echo ""
echo "=== Step 1: Chunk-wise TCP (per-request latency) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
echo "--- Measuring per-request latency for 5 MB chunk GETs over TCP ---"
docker exec "$CONTAINER" nixlbench \
    "${BASE_FLAGS[@]}" \
    -op_type READ \
    -start_block_size $BLOCK -max_block_size $BLOCK \
    -num_iter $ITERS -warmup_iter 0 \
    -obj_prepop_num $TOTAL_PREPOP \
    2>&1 | tee "${LOGDIR}/chunkwise_tcp.log" | grep "^$BLOCK"

# ─── Step 2: Chunk-wise RDMA ─────────────────────────────────────────────
echo ""
echo "=== Step 2: Chunk-wise RDMA (per-request latency) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
docker exec "$CONTAINER" nixlbench \
    "${BASE_FLAGS[@]}" \
    -op_type READ \
    -start_block_size $BLOCK -max_block_size $BLOCK \
    -num_iter $ITERS -warmup_iter 0 \
    -obj_prepop_num $TOTAL_PREPOP \
    -obj_rdma_port 7471 \
    2>&1 | tee "${LOGDIR}/chunkwise_rdma.log" | grep "^$BLOCK"

# ─── Step 3: Layer-wise TCP ──────────────────────────────────────────────
echo ""
echo "=== Step 3: Layer-wise TCP (per-request latency for 64 KB range-GETs) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
docker exec "$CONTAINER" nixlbench \
    "${BASE_FLAGS[@]}" \
    -op_type READ \
    -start_block_size $LAYER_SLICE -max_block_size $LAYER_SLICE \
    -num_iter $ITERS -warmup_iter 0 \
    -obj_prepop_num $TOTAL_PREPOP \
    -layerwise_mode \
    -kvcache_num_layers $NUM_LAYERS \
    -kvcache_kv_per_token $KV_PER_TOKEN \
    2>&1 | tee "${LOGDIR}/layerwise_tcp.log" | grep "^$LAYER_SLICE"

# ─── Step 4: Layer-wise RDMA ─────────────────────────────────────────────
echo ""
echo "=== Step 4: Layer-wise RDMA (per-request latency for 64 KB range-GETs) ==="
docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
docker exec "$CONTAINER" nixlbench \
    "${BASE_FLAGS[@]}" \
    -op_type READ \
    -start_block_size $LAYER_SLICE -max_block_size $LAYER_SLICE \
    -num_iter $ITERS -warmup_iter 0 \
    -obj_prepop_num $TOTAL_PREPOP \
    -obj_rdma_port 7471 \
    -layerwise_mode \
    -kvcache_num_layers $NUM_LAYERS \
    -kvcache_kv_per_token $KV_PER_TOKEN \
    2>&1 | tee "${LOGDIR}/layerwise_rdma.log" | grep "^$LAYER_SLICE"

# ─── Step 5: x-amz-kvcache (layer_aggregate = num_layers, chunk-wise emulation) ─
echo ""
echo "=== Step 5: x-amz-kvcache RDMA (layer_aggregate=80, chunk-wise) ==="
for N in "${CHUNK_COUNTS[@]}"; do
    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
    echo "--- N=$N chunks ---"
    docker exec "$CONTAINER" nixlbench \
        "${BASE_FLAGS[@]}" \
        -op_type READ \
        -start_block_size $BLOCK -max_block_size $BLOCK \
        -num_iter $ITERS -warmup_iter 0 \
        -obj_prepop_num $N \
        -obj_rdma_port 7471 \
        -kvcache_mode -kvcache_num_layers $NUM_LAYERS \
        -kvcache_kv_per_token $KV_PER_TOKEN \
        -kvcache_layer_aggregate $NUM_LAYERS \
        2>&1 | tee "${LOGDIR}/kvcache_agg80_${N}chunks.log" | grep "^$BLOCK"
done

# ─── Step 6: x-amz-kvcache (layer_aggregate = 1, true layer-wise) ───────
echo ""
echo "=== Step 6: x-amz-kvcache RDMA (layer_aggregate=1, layer-wise) ==="
for N in "${CHUNK_COUNTS[@]}"; do
    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true; sleep 1
    echo "--- N=$N chunks ---"
    docker exec "$CONTAINER" nixlbench \
        "${BASE_FLAGS[@]}" \
        -op_type READ \
        -start_block_size $BLOCK -max_block_size $BLOCK \
        -num_iter $ITERS -warmup_iter 0 \
        -obj_prepop_num $N \
        -obj_rdma_port 7471 \
        -kvcache_mode -kvcache_num_layers $NUM_LAYERS \
        -kvcache_kv_per_token $KV_PER_TOKEN \
        -kvcache_layer_aggregate 1 \
        2>&1 | tee "${LOGDIR}/kvcache_agg1_${N}chunks.log" | grep "^$BLOCK"
done

# ─── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Per-Request Latency (measured)"
echo "============================================================"
echo ""

# Extract per-request latencies
for f in chunkwise_tcp chunkwise_rdma layerwise_tcp layerwise_rdma; do
    result=$(grep -E "^[0-9]+" "${LOGDIR}/${f}.log" 2>/dev/null | head -1)
    if [ -n "$result" ]; then
        lat=$(echo "$result" | awk '{print $4}')
        printf "  %-25s %s us/request\n" "$f" "$lat"
    fi
done

echo ""
echo "============================================================"
echo "  x-amz-kvcache Per-Load Latency (measured directly)"
echo "============================================================"
echo ""
for agg in 80 1; do
    for N in "${CHUNK_COUNTS[@]}"; do
        f="${LOGDIR}/kvcache_agg${agg}_${N}chunks.log"
        result=$(grep -E "^[0-9]+" "$f" 2>/dev/null | head -1)
        if [ -n "$result" ]; then
            lat=$(echo "$result" | awk '{print $4}')
            printf "  kvcache_agg%-3s N=%-4s %s us/load\n" "$agg" "$N" "$lat"
        fi
    done
done

echo ""
echo "============================================================"
echo "  Calculated Per-KV-Cache-Load Time (Option B)"
echo "============================================================"
echo ""

TCP_LAT=$(grep -E "^[0-9]+" "${LOGDIR}/chunkwise_tcp.log" 2>/dev/null | head -1 | awk '{print $4}')
RDMA_LAT=$(grep -E "^[0-9]+" "${LOGDIR}/chunkwise_rdma.log" 2>/dev/null | head -1 | awk '{print $4}')
LTCP_LAT=$(grep -E "^[0-9]+" "${LOGDIR}/layerwise_tcp.log" 2>/dev/null | head -1 | awk '{print $4}')
LRDMA_LAT=$(grep -E "^[0-9]+" "${LOGDIR}/layerwise_rdma.log" 2>/dev/null | head -1 | awk '{print $4}')

printf "  %-30s" "N chunks"
for N in "${CHUNK_COUNTS[@]}"; do printf "%10s" "N=$N"; done
echo ""
echo "  $(printf '%0.s-' {1..80})"

for label_lat in "Chunk TCP:$TCP_LAT:1" "Chunk RDMA:$RDMA_LAT:1" "Layer TCP:$LTCP_LAT:80" "Layer RDMA:$LRDMA_LAT:80"; do
    IFS=: read -r label lat mult <<< "$label_lat"
    printf "  %-30s" "$label"
    for N in "${CHUNK_COUNTS[@]}"; do
        if [ -n "$lat" ]; then
            load_us=$(echo "$lat * $N * $mult" | bc -l 2>/dev/null | xargs printf "%.0f" 2>/dev/null || echo "N/A")
            load_ms=$(echo "scale=1; $load_us / 1000" | bc -l 2>/dev/null || echo "N/A")
            printf "%8s ms" "$load_ms"
        else
            printf "%10s" "N/A"
        fi
    done
    echo ""
done

# kvcache rows from measured data
for agg in 80 1; do
    label="kvcache agg=$agg"
    printf "  %-30s" "$label"
    for N in "${CHUNK_COUNTS[@]}"; do
        f="${LOGDIR}/kvcache_agg${agg}_${N}chunks.log"
        result=$(grep -E "^[0-9]+" "$f" 2>/dev/null | head -1)
        if [ -n "$result" ]; then
            lat_us=$(echo "$result" | awk '{print $4}')
            lat_ms=$(echo "scale=1; $lat_us / 1000" | bc -l 2>/dev/null || echo "N/A")
            printf "%8s ms" "$lat_ms"
        else
            printf "%10s" "N/A"
        fi
    done
    echo ""
done

echo ""
echo "Logs saved to: $LOGDIR"
