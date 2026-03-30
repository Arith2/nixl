#!/bin/bash
# NIXL 256MB Benchmark Script
#
# Runs PUT (3000 iter) + GET (warmup 256, 2048 iter) for 256MB objects.
# Tests NIXL_TCP + DAOS_RDMA and NIXL_TCP + DAOS_TCP configurations.
#
# Data budget: 3000 × 256MB = 750GB < 2TB DAOS pool limit
#
# Run from hsc-12. Requires passwordless SSH to hsc-14 and hsc-21.
#
# Usage:
#   ./benchmark/scripts/run_nixl_benchmark_256MB.sh              # both configs
#   ./benchmark/scripts/run_nixl_benchmark_256MB.sh DAOS_RDMA    # RDMA only
#   ./benchmark/scripts/run_nixl_benchmark_256MB.sh DAOS_TCP     # TCP only

set -euo pipefail

LOGDIR=/HSC/users/zhuyu/nixl/logs

BLOCK_SIZE=268435456
BLOCK_NAME=256MB

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
    docker stop etcd-server 2>/dev/null || true
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
        echo "Waiting 15s for DAOS engine to register ranks..."
        sleep 15
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
        echo "Waiting 15s for DAOS engine to register ranks..."
        sleep 15
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

start_hsc12() {
    echo "[hsc-12] ensuring etcd is running and flushing keys..."
    docker start etcd-server 2>/dev/null || true
    sleep 2
    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null || true
    sleep 1
}

# ── benchmark runner ─────────────────────────────────────────────────────────
run_benchmark() {
    local daos_tag=$1   # DAOS_RDMA or DAOS_TCP
    local nixl_tag=$2   # NIXL_TCP or NIXL_RDMA
    local rdma_flag=$3  # "" or "-obj_rdma_port 7471"

    local config="${nixl_tag}_${daos_tag}"

    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "  Config: $config  Block: $BLOCK_NAME"
    echo "  PUT: 3000 iter   GET: 256 warmup + 2048 iter"
    echo "╚══════════════════════════════════════════════╝"

    # ── PUT: 6000 iterations ──
    local log="${LOGDIR}/PUT_${config}_${BLOCK_NAME}.log"
    echo "--- PUT $config $BLOCK_NAME → $log ---"
    start_hsc12
    # shellcheck disable=SC2086
    docker exec nixl-dev nixlbench \
        "${BASE_FLAGS[@]}" \
        -op_type WRITE \
        -start_block_size "$BLOCK_SIZE" -max_block_size "$BLOCK_SIZE" \
        -num_iter 3000 \
        -warmup_iter 0 \
        -obj_prepop_num 3000 \
        $rdma_flag \
        2>&1 | tee "$log"

    # ── GET: warmup 512, 4096 iterations ──
    log="${LOGDIR}/GET_${config}_${BLOCK_NAME}.log"
    echo "--- GET $config $BLOCK_NAME → $log ---"
    start_hsc12
    # shellcheck disable=SC2086
    docker exec nixl-dev nixlbench \
        "${BASE_FLAGS[@]}" \
        -op_type READ \
        -start_block_size "$BLOCK_SIZE" -max_block_size "$BLOCK_SIZE" \
        -num_iter 2048 \
        -warmup_iter 256 \
        -obj_prepop_num 3000 \
        $rdma_flag \
        2>&1 | tee "$log"

    echo "[done] $config"
}

# ── per-config startup + benchmark ──────────────────────────────────────────
run_daos_rdma_configs() {
    echo ""
    echo "████████████████████████████████████████████████"
    echo "  DAOS RDMA (ucx+dc_x) — NIXL TCP baseline"
    echo "████████████████████████████████████████████████"

    teardown_all
    start_daos_rdma
    start_ceph_rdma
    start_hsc12

    run_benchmark DAOS_RDMA NIXL_TCP ""
}

run_daos_tcp_configs() {
    echo ""
    echo "████████████████████████████████████████████████"
    echo "  DAOS TCP (ucx+tcp) — NIXL TCP baseline"
    echo "████████████████████████████████████████████████"

    teardown_all
    start_daos_tcp
    start_ceph_tcp
    start_hsc12

    run_benchmark DAOS_TCP NIXL_TCP ""
}

# ── main ─────────────────────────────────────────────────────────────────────
mkdir -p "$LOGDIR"

TARGET=${1:-ALL}

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
ls -lh "${LOGDIR}"/PUT_*_${BLOCK_NAME}.log "${LOGDIR}"/GET_*_${BLOCK_NAME}.log 2>/dev/null | sort || true
