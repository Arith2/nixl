# NIXL Development Test Cycle — DAOS RDMA

Development runbook for iterative NIXL + Ceph RGW RDMA implementation testing.
Uses DAOS RDMA transport (hsc-14 ↔ hsc-21, `ucx+dc_x` over `ens1np0`/`mlx5_0`).
Full teardown + restart every cycle to avoid stale DAOS pool state, orphaned bucket
metadata, and lingering handles.

3-node setup: **hsc-21** (DAOS storage) → **hsc-14** (Ceph RGW S3 gateway) → **hsc-12** (nixlbench dev container)

Key details:
- S3 gateway: `http://10.93.244.74:8000` (hsc-14)
- S3 bucket: `lmcache`
- DAOS pool: `Pool1` (2078G, 4 SSDs)
- DAOS transport: **RDMA / UCX `ucx+dc_x`** (hsc-21: `daos_server_4_SSD_rdma_UCX.yml`, hsc-14: `daos_agent_hsc14_rdma_UCX.yml`)
- NIXL → Ceph: **RDMA_READ** via RC QP on port 7471 (x-amz-rdma-token header)
- etcd: `localhost:2379` on hsc-12
- nixlbench dev container: `nixl-dev` (long-running, source mounted)

---

## Development Source Directories

| Node | Path | Role |
|---|---|---|
| hsc-12 | `/HSC/users/zhuyu/nixl/src/plugins/obj/s3/` | NIXL OBJ S3 plugin (edit on host) |
| hsc-14 | `/HSC/users/zhuyu/ceph_hsc14_daos_v18.2.7/src/rgw/` | Ceph RGW handler (edit on hsc-14) |
| hsc-21 | — | No changes needed (DAOS storage backend only) |

> `/HSC/users/zhuyu/` is a shared network filesystem. `/home/zhuyu/` is local to each machine.

---

## One-time Setup: nixl-dev container (hsc-12)

Run once. Keep the container alive across test cycles — do not remove it.

```bash
docker run -dit --name nixl-dev \
  --gpus all --network host \
  --ipc=host --ulimit memlock=-1 --ulimit stack=67108864 \
  -e AWS_DEFAULT_BUCKET=lmcache \
  -v /HSC/users/zhuyu/nixl:/workspace/nixl \
  nixlbench:v1.0.0.4071a53 bash
```

---

## Development Iteration Loop

### Step 1 — Edit source (hsc-12 host or hsc-14)

```bash
# NIXL changes: edit on hsc-12 host (auto-visible inside container via mount)
vim /HSC/users/zhuyu/nixl/src/plugins/obj/s3/client.cpp
vim /HSC/users/zhuyu/nixl/src/plugins/obj/s3/engine_impl.cpp

# Ceph RGW changes: edit on hsc-14
ssh hsc-14
vim /HSC/users/zhuyu/ceph_hsc14_daos_v18.2.7/src/rgw/rgw_rest_s3.cc
vim /HSC/users/zhuyu/ceph_hsc14_daos_v18.2.7/src/rgw/rgw_rdma.cc
```

### Step 2 — Recompile

```bash
# Recompile NIXL (hsc-12, inside dev container)
docker exec -it nixl-dev bash
  cd /workspace/nixl && ninja -C build && ninja -C build install && ldconfig

# Recompile Ceph RGW (hsc-14)
ssh hsc-14
  cd ~/ceph_hsc14_daos_v18.2.7/build && ninja radosgw
```

> Full reconfigure (only when adding new source files or deps):
> ```bash
> # NIXL
> cd /workspace/nixl && rm -rf build
> meson setup build --prefix=/usr/local/nixl --buildtype=release
> ninja -C build && ninja -C build install && ldconfig
>
> # Ceph RGW
> cd ~/ceph_hsc14_daos_v18.2.7/build && cmake .. && ninja radosgw
> ```

### Step 3 — Teardown (hsc-12 → hsc-14 → hsc-21)

```bash
# hsc-12
docker stop etcd-server

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

### Step 4 — Restart (hsc-21 → hsc-14 → hsc-12)

**hsc-21** — wait until log shows "storage ready" before proceeding to hsc-14:
```bash
ssh hsc-21
screen -S daos
sudo env HWLOC_COMPONENTS=-opencl ~/daos/install/bin/daos_server start \
  -o ~/GDOS/daos_config/daos_server_4_SSD_rdma_UCX.yml \
  --auto-format
# Ctrl+A D to detach

# Verify:
tail -f /tmp/daos_server.log | grep -i "storage ready"
```

**hsc-14** — wait until RGW is serving before proceeding to hsc-12:
```bash
ssh hsc-14

# Start DAOS agent (RDMA config)
screen -S daos
~/daos/install/bin/daos_agent start -o ~/GDOS/daos_config/daos_agent_hsc14_rdma_UCX.yml
# Ctrl+A D to detach

# Create DAOS pool
~/daos/install/bin/dmg -o ~/GDOS/daos_config/daos_control.yml pool create --size 2078G Pool1

# Start Ceph RGW (newly compiled binary)
cd ~/ceph_hsc14_daos_v18.2.7/build
MON=1 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n \
  -o "rgw backend store = daos" \
  -o "daos pool = Pool1" \
  -o "mon data avail crit = 0" \
  -o "rgw max chunk size = 67108864"

# Create S3 bucket
aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://lmcache
```

**hsc-12** — run benchmark inside the persistent dev container:
```bash
docker restart etcd-server

docker exec -it nixl-dev \
  nixlbench \
  -etcd_endpoints http://localhost:2379 \
  -backend OBJ \
  -op_type WRITE \
  -num_iter 8192 \
  -warmup_iter 1024 \
  -num_threads 1 \
  -large_blk_iter_ftr 32 \
  -start_block_size 67108864 \
  -max_block_size 67108864 \
  -obj_endpoint_override http://10.93.244.74:8000 \
  -obj_scheme http \
  -obj_bucket_name lmcache \
  -obj_region us-east-1 \
  -obj_access_key 0555b35654ad1656d804 \
  -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
  -obj_rdma_port 7471
```

---

> **Order dependency:** hsc-21 must show "storage ready" before Step 4/hsc-14, and
> hsc-14 gateway must be serving before Step 4/hsc-12.
>
> **RDMA connection:** the RGW RDMA CM server starts eagerly at RGW init (`rgw_main.cc`).
> NIXL connects at engine init time when `-obj_rdma_port` is provided. Both must be up
> before nixlbench starts. The `TIMING rdma_read()` line in `radosgw.8000.log` confirms
> the RDMA path is active.

---

## Verify RDMA is active

After the benchmark starts, confirm both legs on hsc-14:

```bash
# NIXL → Ceph RDMA (should show ~10ms for 64MB, not ~400ms)
grep "TIMING rdma_read" ~/ceph_hsc14_daos_v18.2.7/build/out/radosgw.8000.log | tail -3

# Ceph → DAOS transport (should show ucx+dc_x)
grep "provider" ~/GDOS/daos_config/daos_server_4_SSD_rdma_UCX.yml
# expected: provider: ucx+dc_x

# Full pipeline breakdown
grep "TIMING pipeline" ~/ceph_hsc14_daos_v18.2.7/build/out/radosgw.8000.log | tail -3
```

---

## Snapshot image after stable milestone

```bash
docker commit nixl-dev nixlbench:rdma-dev-v1

# OR rebuild cleanly from source
cd /HSC/users/zhuyu/nixl/benchmark/nixlbench/contrib
./build.sh --nixl /HSC/users/zhuyu/nixl --tag nixlbench:rdma-dev-v1
```
