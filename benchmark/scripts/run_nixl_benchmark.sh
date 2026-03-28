#!/bin/bash
# NIXL End-to-End Benchmark Script
#
# Runs PUT + GET per block size (4KB→64MB) for all 4 configurations:
#   NIXL_TCP_DAOS_RDMA, NIXL_TCP_DAOS_TCP,
#   NIXL_RDMA_DAOS_RDMA, NIXL_RDMA_DAOS_TCP
#
# Each block size runs as a separate nixlbench invocation so that the
# prepop object key (prepop_{size}B_...) matches the actual block size,
# preventing key collisions and object overwrites across block sizes.
#
# Run from hsc-12. Requires passwordless SSH to hsc-14 and hsc-21.
#
# Usage:
#   ./benchmark/scripts/run_nixl_benchmark.sh              # all 4 configs
#   ./benchmark/scripts/run_nixl_benchmark.sh DAOS_RDMA    # RDMA configs only
#   ./benchmark/scripts/run_nixl_benchmark.sh DAOS_TCP     # TCP configs only

set -euo pipefail

LOGDIR=/HSC/users/zhuyu/nixl/logs

BLOCK_SIZES=(4096 65536 1048576 8388608 67108864)
BLOCK_NAMES=(4KB   64KB  1MB     8MB     64MB   )

# ── nixlbench flags ─────────────────────────────────────────────────────────
ETCD=http://localhost:2379
ENDPOINT=http://10.93.244.74:8000

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
    -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
)

# ── teardown helpers ─────────────────────────────────────────────────────────
teardown_hsc12() {
    echo "[hsc-12] stopping etcd..."
    docker stop etcd-server || true
}

teardown_hsc14() {
    echo "[hsc-14] stopping Ceph RGW and DAOS agent..."
    ssh hsc-14 bash -s <<'EOF'
        cd ~/ceph_hsc14_daos_v18.2.7/build && ../src/stop.sh 2>/dev/null || true
        screen -S daos -X quit 2>/dev/null || true
        sleep 2
EOF
}

teardown_hsc21() {
    echo "[hsc-21] stopping DAOS server..."
    ssh hsc-21 bash -s <<'EOF'
        screen -S daos -X quit 2>/dev/null || true
        sleep 2
        sudo umount /mnt/daos0 2>/dev/null || true
        sudo rm -rvf /mnt/daos* 2>/dev/null || true
        bash ~/GDOS/spdk_erase.sh
EOF
}

teardown_all() {
    echo "=== Teardown ==="
    teardown_hsc12
    teardown_hsc14
    teardown_hsc21
    echo "=== Teardown complete ==="
    sleep 3
}

# ── startup helpers ──────────────────────────────────────────────────────────
start_daos_rdma() {
    echo "[hsc-21] starting DAOS server (RDMA/ucx+dc_x)..."
    ssh hsc-21 bash -s <<'EOF'
        screen -dmS daos bash -c '
            sudo env HWLOC_COMPONENTS=-opencl \
                ~/daos/install/bin/daos_server start \
                -o ~/GDOS/daos_config/daos_server_4_SSD_rdma_UCX.yml \
                --auto-format 2>&1 | tee /tmp/daos_server.log'
        echo "Waiting for DAOS storage ready..."
        for i in $(seq 1 60); do
            grep -q "storage ready" /tmp/daos_server.log 2>/dev/null && break
            sleep 5
        done
        grep "storage ready" /tmp/daos_server.log
EOF
}

start_daos_tcp() {
    echo "[hsc-21] starting DAOS server (TCP/ucx+tcp)..."
    ssh hsc-21 bash -s <<'EOF'
        screen -dmS daos bash -c '
            sudo env HWLOC_COMPONENTS=-opencl \
                ~/daos/install/bin/daos_server start \
                -o ~/GDOS/daos_config/daos_server_4_SSD_tcp_UCX.yml \
                --auto-format 2>&1 | tee /tmp/daos_server.log'
        echo "Waiting for DAOS storage ready..."
        for i in $(seq 1 60); do
            grep -q "storage ready" /tmp/daos_server.log 2>/dev/null && break
            sleep 5
        done
        grep "storage ready" /tmp/daos_server.log
EOF
}

start_ceph_rdma() {
    echo "[hsc-14] starting DAOS agent (RDMA) + Ceph RGW..."
    ssh hsc-14 bash -s <<'EOF'
        screen -dmS daos bash -c '
            ~/daos/install/bin/daos_agent start \
                -o ~/GDOS/daos_config/daos_agent_hsc14_rdma_UCX.yml \
                2>&1 | tee /tmp/daos_agent.log'
        sleep 5
        ~/daos/install/bin/dmg -o ~/GDOS/daos_config/daos_control.yml \
            pool create --size 2078G Pool1
        cd ~/ceph_hsc14_daos_v18.2.7/build
        MON=1 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n \
            -o "rgw backend store = daos" \
            -o "daos pool = Pool1" \
            -o "mon data avail crit = 0" \
            -o "rgw max chunk size = 67108864"
        sleep 5
        aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://lmcache
        echo "[hsc-14] Ceph RGW ready"
EOF
}

start_ceph_tcp() {
    echo "[hsc-14] starting DAOS agent (TCP) + Ceph RGW..."
    ssh hsc-14 bash -s <<'EOF'
        screen -dmS daos bash -c '
            ~/daos/install/bin/daos_agent start \
                -o ~/GDOS/daos_config/daos_agent_hsc14_tcp_UCX.yml \
                2>&1 | tee /tmp/daos_agent.log'
        sleep 5
        dmg -o ~/GDOS/daos_config/daos_control.yml \
            pool create --size 2078G Pool1
        cd ~/ceph_hsc14_daos_v18.2.7/build
        MON=1 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n \
            -o "rgw backend store = daos" \
            -o "daos pool = Pool1" \
            -o "mon data avail crit = 0" \
            -o "rgw max chunk size = 67108864"
        sleep 5
        aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://lmcache
        echo "[hsc-14] Ceph RGW ready"
EOF
}

start_hsc12() {
    echo "[hsc-12] starting etcd..."
    docker restart etcd-server
    sleep 3
}

# ── benchmark runner ─────────────────────────────────────────────────────────
# Runs PUT then GET for every block size as separate invocations.
# Each size uses start_block_size=max_block_size=BS so that prepop keys
# are prepop_{BS}B_... — no cross-size overwrites in a single bucket.
run_benchmark() {
    local daos_tag=$1   # DAOS_RDMA or DAOS_TCP
    local nixl_tag=$2   # NIXL_TCP  or NIXL_RDMA
    local rdma_flag=$3  # "" or "-obj_rdma_port 7471"

    local config="${nixl_tag}_${daos_tag}"
    local put_log="${LOGDIR}/PUT_${config}.log"
    local get_log="${LOGDIR}/GET_${config}.log"

    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "  Config: $config"
    echo "  PUT log: $put_log"
    echo "  GET log: $get_log"
    echo "╚══════════════════════════════════════════════╝"

    # ── PUT: one invocation per block size (12K objects kept, no cleanup) ──
    echo "[hsc-12] running PUT per block size..."
    : > "$put_log"   # truncate/create log
    for idx in "${!BLOCK_SIZES[@]}"; do
        local bs=${BLOCK_SIZES[$idx]}
        local name=${BLOCK_NAMES[$idx]}
        echo "--- PUT $name ($bs B) ---" | tee -a "$put_log"
        start_hsc12   # fresh etcd metadata for each size
        # shellcheck disable=SC2086
        docker exec nixl-dev nixlbench \
            "${BASE_FLAGS[@]}" \
            -op_type WRITE \
            -start_block_size "$bs" -max_block_size "$bs" \
            -num_iter 12000 \
            -warmup_iter 0 \
            $rdma_flag \
            2>&1 | tee -a "$put_log"
    done

    # ── GET: one invocation per block size (reads pre-existing objects) ────
    echo "[hsc-12] running GET per block size..."
    : > "$get_log"
    for idx in "${!BLOCK_SIZES[@]}"; do
        local bs=${BLOCK_SIZES[$idx]}
        local name=${BLOCK_NAMES[$idx]}
        echo "--- GET $name ($bs B) ---" | tee -a "$get_log"
        start_hsc12
        # shellcheck disable=SC2086
        docker exec nixl-dev nixlbench \
            "${BASE_FLAGS[@]}" \
            -op_type READ \
            -start_block_size "$bs" -max_block_size "$bs" \
            -num_iter 8192 \
            -warmup_iter 1024 \
            $rdma_flag \
            2>&1 | tee -a "$get_log"
    done

    echo "[done] $config"
}

# ── per-config startup + benchmark ──────────────────────────────────────────
run_daos_rdma_configs() {
    echo ""
    echo "████████████████████████████████████████████████"
    echo "  DAOS RDMA (ucx+dc_x over ens1np0/mlx5_0)"
    echo "████████████████████████████████████████████████"

    teardown_all
    start_daos_rdma
    start_ceph_rdma
    start_hsc12

    run_benchmark DAOS_RDMA NIXL_TCP  ""
    run_benchmark DAOS_RDMA NIXL_RDMA "-obj_rdma_port 7471"
}

run_daos_tcp_configs() {
    echo ""
    echo "████████████████████████████████████████████████"
    echo "  DAOS TCP (ucx+tcp)"
    echo "████████████████████████████████████████████████"

    teardown_all
    start_daos_tcp
    start_ceph_tcp
    start_hsc12

    run_benchmark DAOS_TCP NIXL_TCP  ""
    run_benchmark DAOS_TCP NIXL_RDMA "-obj_rdma_port 7471"
}

# ── step 0: rebuild nixlbench with --obj_prepop_num flag ────────────────────
rebuild_nixlbench() {
    echo "=== Rebuilding nixlbench inside nixl-dev container ==="
    docker exec nixl-dev bash -c \
        "cd /workspace/nixl && ninja -C build && ninja -C build install && ldconfig"
    echo "=== Rebuild complete ==="
}

# ── main ─────────────────────────────────────────────────────────────────────
mkdir -p "$LOGDIR"

TARGET=${1:-ALL}

rebuild_nixlbench

case "$TARGET" in
    DAOS_RDMA)
        run_daos_rdma_configs
        ;;
    DAOS_TCP)
        run_daos_tcp_configs
        ;;
    ALL|*)
        run_daos_rdma_configs
        run_daos_tcp_configs
        ;;
esac

echo ""
echo "=== All benchmarks complete. Logs in $LOGDIR ==="
ls -lh "${LOGDIR}"/PUT_*.log "${LOGDIR}"/GET_*.log 2>/dev/null || true
