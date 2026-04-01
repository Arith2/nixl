# NIXL GPUDirect RDMA Benchmark — DAOS RDMA

Benchmarks NIXL S3 over RDMA with **GPU VRAM** as the initiator buffer (GPUDirect RDMA).
Data flows: `GPU VRAM (hsc-12) ↔ RDMA ↔ Ceph RGW (hsc-14) ↔ DAOS RDMA ↔ NVMe (hsc-21)`

Requires `nvidia_peermem` kernel module on hsc-12 and the `nixl-gpu` container
(created with `--runtime=nvidia --gpus all --device /dev/infiniband/*`).

## Prerequisites

- `nixl-gpu` container with GPU + RDMA access (see below)
- `VRAM_SEG` support in NIXL OBJ plugin (`engine_impl.cpp` patch applied)
- DAOS pool size: **3 TB** (total data ~1.74 TB across all block sizes)

### Create nixl-gpu container (one-time)

```bash
# On hsc-12:
docker run -dit --name nixl-gpu \
    --runtime=nvidia --gpus all \
    --network host --ipc=host \
    --ulimit memlock=-1 --ulimit stack=67108864 \
    --device /dev/infiniband/rdma_cm \
    --device /dev/infiniband/uverbs0 \
    -v /HSC/users/zhuyu/nixl:/workspace/nixl \
    nixl-dev-rdma:latest bash

# Rebuild inside container:
docker exec nixl-gpu bash -c '
    cp /workspace/nixl/benchmark/nixlbench/src/utils/utils.h /workspace/nixlbench/src/utils/utils.h
    cp /workspace/nixl/benchmark/nixlbench/src/utils/utils.cpp /workspace/nixlbench/src/utils/utils.cpp
    cp /workspace/nixl/benchmark/nixlbench/src/worker/nixl/nixl_worker.h /workspace/nixlbench/src/worker/nixl/nixl_worker.h
    cp /workspace/nixl/benchmark/nixlbench/src/worker/nixl/nixl_worker.cpp /workspace/nixlbench/src/worker/nixl/nixl_worker.cpp
    cd /workspace/nixl && ninja -C build && ninja -C build install
    ninja -C /workspace/nixlbench/build && ninja -C /workspace/nixlbench/build install
    ldconfig'

# Verify:
docker exec nixl-gpu nvidia-smi -L
docker exec nixl-gpu ls /dev/infiniband/
```

---

## Step 1: Teardown (hsc-12 → hsc-14 → hsc-21)

```bash
# hsc-12
docker exec nixl-gpu pkill -f nixlbench 2>/dev/null
docker stop etcd-server 2>/dev/null

# hsc-14
ssh hsc-14
  cd ~/ceph_hsc14_daos_v18.2.7/build && ../src/stop.sh
  screen -S daos -X quit

# hsc-21
ssh hsc-21
  screen -S daos -X quit
  sudo umount /mnt/daos0
  sudo rm -rvf /mnt/daos*
  bash ~/GDOS/spdk_erase.sh
```

## Step 2: Restart (hsc-21 → hsc-14 → hsc-12)

**hsc-21** — Start DAOS server (RDMA):
```bash
ssh hsc-21
screen -S daos
sudo env HWLOC_COMPONENTS=-opencl ~/daos/install/bin/daos_server start \
    -o ~/GDOS/daos_config/daos_server_4_SSD_rdma_UCX.yml \
    --auto-format
# Ctrl+A D to detach
# Wait for "storage ready" in log, then wait 20s more
```

**hsc-14** — Start agent + pool (3 TB) + RGW:
```bash
ssh hsc-14

# Start DAOS agent
screen -S daos
~/daos/install/bin/daos_agent start -o ~/GDOS/daos_config/daos_agent_hsc14_rdma_UCX.yml
# Ctrl+A D to detach

# Create 3 TB pool
~/daos/install/bin/dmg -o ~/GDOS/daos_config/daos_control.yml pool create --size 3078G Pool1

# Start Ceph RGW
cd ~/ceph_hsc14_daos_v18.2.7/build
MON=1 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n \
    -o "rgw backend store = daos" \
    -o "daos pool = Pool1" \
    -o "mon data avail crit = 0" \
    -o "rgw max chunk size = 67108864"

# Create bucket
aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://lmcache
```

**hsc-12** — Start etcd:
```bash
docker start etcd-server
```

## Step 3: Run Benchmark

All PUTs first (populates objects), then all GETs (reads from storage).

### Storage budget
| Block Size | PUT Iter | Data (GB) |
|------------|----------|-----------|
| 64KB       | 12000    | 0.75      |
| 256KB      | 12000    | 3.0       |
| 1MB        | 12000    | 12.0      |
| 4MB        | 12000    | 48.0      |
| 16MB       | 12000    | 192.0     |
| 64MB       | 12000    | 768.0     |
| 256MB      | 3000     | 750.0     |
| **Total**  |          | **1774**  |

### PUT phase (all block sizes)
```bash
LOGDIR=/HSC/users/zhuyu/nixl/logs/nixl_gpu

for BS_NAME in "65536 64KB 12000" "262144 256KB 12000" "1048576 1MB 12000" \
               "4194304 4MB 12000" "16777216 16MB 12000" "67108864 64MB 12000" \
               "268435456 256MB 3000"; do
    BS=$(echo $BS_NAME | awk '{print $1}')
    NAME=$(echo $BS_NAME | awk '{print $2}')
    ITER=$(echo $BS_NAME | awk '{print $3}')

    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null; sleep 1
    echo "=== PUT ${NAME} VRAM RDMA+RDMA ==="
    docker exec nixl-gpu nixlbench \
        -etcd_endpoints http://localhost:2379 \
        -backend OBJ -num_threads 1 -large_blk_iter_ftr 1 \
        -obj_endpoint_override http://10.93.244.74:8000 -obj_scheme http \
        -obj_bucket_name lmcache -obj_region us-east-1 \
        -obj_access_key 0555b35654ad1656d804 \
        -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
        -op_type WRITE -start_block_size "$BS" -max_block_size "$BS" \
        -num_iter "$ITER" -warmup_iter 0 -obj_prepop_num "$ITER" \
        -obj_rdma_port 7471 -initiator_seg_type VRAM \
        2>&1 | tee "${LOGDIR}/PUT_NIXL_VRAM_RDMA_DAOS_RDMA_${NAME}.log"
done
```

### GET phase (all block sizes, reads objects from PUT phase)
```bash
LOGDIR=/HSC/users/zhuyu/nixl/logs/nixl_gpu

for BS_NAME in "65536 64KB 12000 8192 1024" "262144 256KB 12000 8192 1024" \
               "1048576 1MB 12000 8192 1024" "4194304 4MB 12000 8192 1024" \
               "16777216 16MB 12000 8192 1024" "67108864 64MB 12000 8192 1024" \
               "268435456 256MB 3000 2048 256"; do
    BS=$(echo $BS_NAME | awk '{print $1}')
    NAME=$(echo $BS_NAME | awk '{print $2}')
    PREPOP=$(echo $BS_NAME | awk '{print $3}')
    ITER=$(echo $BS_NAME | awk '{print $4}')
    WARMUP=$(echo $BS_NAME | awk '{print $5}')

    docker exec etcd-server etcdctl del "" --from-key=true 2>/dev/null; sleep 1
    echo "=== GET ${NAME} VRAM RDMA+RDMA ==="
    docker exec nixl-gpu nixlbench \
        -etcd_endpoints http://localhost:2379 \
        -backend OBJ -num_threads 1 -large_blk_iter_ftr 1 \
        -obj_endpoint_override http://10.93.244.74:8000 -obj_scheme http \
        -obj_bucket_name lmcache -obj_region us-east-1 \
        -obj_access_key 0555b35654ad1656d804 \
        -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
        -op_type READ -start_block_size "$BS" -max_block_size "$BS" \
        -num_iter "$ITER" -warmup_iter "$WARMUP" -obj_prepop_num "$PREPOP" \
        -obj_rdma_port 7471 -initiator_seg_type VRAM \
        2>&1 | tee "${LOGDIR}/GET_NIXL_VRAM_RDMA_DAOS_RDMA_${NAME}.log"
done
```

## Step 4: Verify Results

```bash
for f in logs/nixl_gpu/PUT_*.log logs/nixl_gpu/GET_*.log; do
    name=$(basename "$f" .log)
    result=$(grep -E "^[0-9]+\s+" "$f" | head -1)
    [ -n "$result" ] && echo "$name: $(echo $result | awk '{print "BW="$3" GB/s, Lat="$4" us"}')"
done
```

---

## Notes

- **PCIe topology**: GPU0 and mlx5_0 (RDMA NIC) are on different NUMA nodes (`SYS` interconnect).
  GPUDirect RDMA traverses QPI/UPI, reducing bandwidth vs DRAM path.
  NIC2/NIC3 (mlx5_2/mlx5_3) are GPU-local (`PXB`) but not network-connected.
- **nvidia_peermem**: must be loaded on hsc-12 (`lsmod | grep nvidia_peermem`)
- **DAOS_BULK_LIMIT**: currently set to 4096 (from earlier 16KB investigation)
