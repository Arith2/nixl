#!/bin/bash
# Master script: runs 256MB TCP benchmarks first, then 4KB-64MB DAOS_TCP benchmarks.
# Skips NIXL_RDMA + DAOS_RDMA (already measured in ./logs).

set -euo pipefail

LOGDIR=/HSC/users/zhuyu/nixl/logs
SCRIPTDIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  Step 1: 256MB TCP benchmarks"
echo "  NIXL_TCP + DAOS_RDMA, NIXL_TCP + DAOS_TCP"
echo "============================================"
bash "${SCRIPTDIR}/run_nixl_benchmark_256MB.sh"

echo ""
echo "============================================"
echo "  Step 2: 4KB-64MB DAOS_TCP benchmarks"
echo "  NIXL_TCP + DAOS_TCP, NIXL_RDMA + DAOS_TCP"
echo "============================================"
bash "${SCRIPTDIR}/run_nixl_benchmark.sh" DAOS_TCP

echo ""
echo "=== All benchmarks complete ==="
ls -1 "${LOGDIR}"/PUT_*.log "${LOGDIR}"/GET_*.log 2>/dev/null | wc -l
echo "log files in ${LOGDIR}"
