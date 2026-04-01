#!/bin/bash
# FIO DFS Baseline Benchmark
#
# Measures raw DAOS sequential write/read throughput via DFS (no S3/Ceph).
# For each block size:
#   Phase 1: Benchmark WRITE (numjobs=1, iodepth=1, timed)
#   Phase 2: Prepopulate 500GB (numjobs=4, iodepth=32, fast, NOT recorded)
#   Phase 3: Benchmark READ  (numjobs=1, iodepth=1, timed, cold storage)
#
# Run on hsc-14 (DAOS agent node). Requires DAOS pool Pool1 to exist.
#
# Usage:
#   ssh hsc-14
#   bash /HSC/users/zhuyu/nixl/benchmark/scripts/run_fio_dfs_baseline.sh

set -euo pipefail
trap "exit" INT

export LD_LIBRARY_PATH="$HOME/daos/install/lib64:$HOME/daos/install/prereq/release/mercury/lib64:${LD_LIBRARY_PATH:-}"
export PATH="$HOME/daos/install/bin:$PATH"
FIO_PATH="$HOME/fio/fio"
DFS_POOL="Pool1"
DFS_CONT="fio_bench"
LOG_DIR="/HSC/users/zhuyu/nixl/logs/fio_dfs_tcp"

BLOCK_SIZES=(65536 262144 1048576 4194304 16777216 67108864 268435456)
BLOCK_NAMES=("64KB" "256KB" "1MB" "4MB" "16MB" "64MB" "256MB")

# Write benchmark: 10GB per job, 60s runtime
WRITE_SIZE="10G"
WRITE_RUNTIME="60s"

# Prepopulate: 500GB to flush cache (DAOS SCM = 125GB)
PREPOP_SIZE="500G"
PREPOP_RUNTIME="120s"
PREPOP_JOBS=4
PREPOP_IODEPTH=32

# Read benchmark: 10GB per job, 60s runtime
READ_SIZE="10G"
READ_RUNTIME="60s"

CSV="${LOG_DIR}/fio_dfs_baseline.csv"
echo "BlockSize,BlockName,Op,IOPS,BW_MBs,Avg_Lat_us,P99_Lat_us" > "$CSV"

echo "=== FIO DFS Baseline Benchmark ==="
echo "Log dir: $LOG_DIR"
echo "CSV: $CSV"
echo ""

for idx in "${!BLOCK_SIZES[@]}"; do
    BS=${BLOCK_SIZES[$idx]}
    NAME=${BLOCK_NAMES[$idx]}

    echo "============================================"
    echo "  Block Size: $NAME ($BS bytes)"
    echo "============================================"

    # Clean container
    daos cont destroy "$DFS_POOL" "$DFS_CONT" 2>/dev/null || true
    sleep 1
    daos cont create --type POSIX "$DFS_POOL" "$DFS_CONT"
    sleep 1

    # Phase 1: Benchmark WRITE
    echo "--- Phase 1: WRITE benchmark ($NAME) ---"
    LOGFILE="${LOG_DIR}/write_${NAME}.json"
    $FIO_PATH --name="data_${NAME}" \
        --rw=write \
        --bs="$BS" \
        --direct=1 \
        --ioengine=dfs \
        --iodepth=1 \
        --numjobs=1 \
        --size="$WRITE_SIZE" \
        --runtime="$WRITE_RUNTIME" \
        --time_based \
        --group_reporting \
        --pool="$DFS_POOL" \
        --cont="$DFS_CONT" \
        --output-format=json \
        --output="$LOGFILE"

    # Extract write results
    W_IOPS=$(jq ".jobs[0].write.iops_mean" "$LOGFILE")
    W_BW=$(jq ".jobs[0].write.bw_mean / 1024" "$LOGFILE")
    W_LAT=$(jq ".jobs[0].write.lat_ns.mean / 1000" "$LOGFILE")
    W_P99=$(jq '.jobs[0].write.clat_ns.percentile["99.000000"] / 1000' "$LOGFILE")
    W_BW=$(printf "%.2f" "$W_BW")
    W_LAT=$(printf "%.2f" "$W_LAT")
    W_P99=$(printf "%.2f" "$W_P99")
    echo "$BS,$NAME,WRITE,$W_IOPS,$W_BW,$W_LAT,$W_P99" >> "$CSV"
    echo "  WRITE: IOPS=$W_IOPS  BW=${W_BW} MB/s  Lat=${W_LAT}us  P99=${W_P99}us"

    # Phase 2: Prepopulate 500GB (not recorded, same filename as write)
    echo "--- Phase 2: Prepopulate 500GB ($NAME, fast, not recorded) ---"
    $FIO_PATH --name="data_${NAME}" \
        --rw=write \
        --bs="$BS" \
        --direct=1 \
        --ioengine=dfs \
        --iodepth="$PREPOP_IODEPTH" \
        --numjobs="$PREPOP_JOBS" \
        --size="$PREPOP_SIZE" \
        --runtime="$PREPOP_RUNTIME" \
        --time_based \
        --group_reporting \
        --pool="$DFS_POOL" \
        --cont="$DFS_CONT" \
        --output-format=normal \
        --output=/dev/null
    echo "  Prepopulate done"

    # Phase 3: Benchmark READ (cold)
    echo "--- Phase 3: READ benchmark ($NAME, cold storage) ---"
    LOGFILE="${LOG_DIR}/read_${NAME}.json"
    $FIO_PATH --name="data_${NAME}" \
        --rw=read \
        --bs="$BS" \
        --direct=1 \
        --ioengine=dfs \
        --iodepth=1 \
        --numjobs=1 \
        --size="$WRITE_SIZE" \
        --runtime="$READ_RUNTIME" \
        --time_based \
        --group_reporting \
        --pool="$DFS_POOL" \
        --cont="$DFS_CONT" \
        --output-format=json \
        --output="$LOGFILE"

    # Extract read results
    R_IOPS=$(jq ".jobs[0].read.iops_mean" "$LOGFILE")
    R_BW=$(jq ".jobs[0].read.bw_mean / 1024" "$LOGFILE")
    R_LAT=$(jq ".jobs[0].read.lat_ns.mean / 1000" "$LOGFILE")
    R_P99=$(jq '.jobs[0].read.clat_ns.percentile["99.000000"] / 1000' "$LOGFILE")
    R_BW=$(printf "%.2f" "$R_BW")
    R_LAT=$(printf "%.2f" "$R_LAT")
    R_P99=$(printf "%.2f" "$R_P99")
    echo "$BS,$NAME,READ,$R_IOPS,$R_BW,$R_LAT,$R_P99" >> "$CSV"
    echo "  READ:  IOPS=$R_IOPS  BW=${R_BW} MB/s  Lat=${R_LAT}us  P99=${R_P99}us"

    echo ""
done

# Cleanup
daos cont destroy "$DFS_POOL" "$DFS_CONT" 2>/dev/null || true

echo "=== All FIO DFS Benchmarks Complete ==="
echo "Results: $CSV"
cat "$CSV"
