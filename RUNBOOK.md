# NIXL 3-Node Benchmark Runbook

3-node setup: **hsc-21** (DAOS storage) → **hsc-14** (Ceph RGW S3 gateway) → **hsc-12** (nixlbench client)

Key details:
- S3 gateway: `http://10.93.244.74:8000` (hsc-14)
- S3 bucket: `lmcache`
- DAOS pool: `Pool1` (2078G, 4 SSDs)
- etcd: `localhost:2379` on hsc-12
- nixlbench image: `nixlbench:v1.0.0.4071a53`
- Config files on hsc-14/hsc-21: `~/GDOS/daos_config/`

## Key Source Directories

| Node | Path | Role |
|---|---|---|
| hsc-21 | `/home/zhuyu/daos` | DAOS server binary |
| hsc-21 | `/home/zhuyu/GDOS/` | Server config YAMLs + spdk_erase script |
| hsc-14 | `/home/zhuyu/daos` | DAOS client tools (daos_agent, dmg) |
| hsc-14 | `/home/zhuyu/GDOS/` | Agent + control config YAMLs |
| hsc-14 | `/HSC/users/zhuyu/ceph_hsc14_daos_v18.2.7` | Ceph RGW with DAOS backend (S3 gateway) |
| hsc-12 | `/HSC/users/zhuyu/nixl` | NIXL source + nixlbench (S3 client) |

> `/HSC/users/zhuyu/` is a shared network filesystem accessible from multiple nodes.
> `/home/zhuyu/` is local to each machine.

For cuObject-style RDMA implementation, changes span:
1. `/HSC/users/zhuyu/nixl` (hsc-12) — client: memory registration, embed RDMA token in HTTP header
2. `/HSC/users/zhuyu/ceph_hsc14_daos_v18.2.7` (hsc-14) — gateway: parse RDMA token, trigger RDMA_WRITE/READ
3. `/home/zhuyu/daos` (hsc-21) — likely no changes needed (DAOS is storage backend only)

---

## Step 1 — hsc-21 (Storage node: DAOS server)

```bash
# Clean up previous state
sudo umount /mnt/daos0
sudo rm -rvf /mnt/daos*
bash ~/GDOS/spdk_erase.sh

# Start DAOS server with 4 SSDs (run inside screen)
screen -S daos
sudo env HWLOC_COMPONENTS=-opencl ~/daos/install/bin/daos_server start \
  -o ~/GDOS/daos_config/daos_server_4_SSD_rdma_UCX.yml \
  --auto-format

# TCP/UCX:
sudo env HWLOC_COMPONENTS=-opencl ~/daos/install/bin/daos_server start \
  -o ~/GDOS/daos_config/daos_server_4_SSD_tcp_UCX.yml \
  --auto-format
```

---

## Step 2 — hsc-14 (Gateway node: DAOS agent + Ceph RGW S3)

```bash
# Start DAOS agent (run inside screen)
screen -S daos

# RDMA/UCX:
daos_agent start -o ~/GDOS/daos_config/daos_agent_hsc14_rdma_UCX.yml

# TCP/UCX:
daos_agent start -o ~/GDOS/daos_config/daos_agent_hsc14_tcp_UCX.yml

# Create DAOS pool
dmg -o ~/GDOS/daos_config/daos_control.yml pool create --size 2078G Pool1

# Start Ceph RGW backed by DAOS
cd ~/ceph_hsc14_daos_v18.2.7/build/
MON=1 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n \
  -o "rgw backend store = daos" \
  -o "daos pool = Pool1" \
  -o "mon data avail crit = 0" \
  -o "rgw max chunk size = 67108864"

# Create S3 bucket
aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://lmcache
```

---

## Step 3 — hsc-12 (Client node: etcd + nixlbench)

```bash
# Start etcd (first time: docker run; subsequent: docker restart)
docker restart etcd-server
# OR first-time:
docker run -d --name etcd-server \
  -p 2379:2379 -p 2380:2380 \
  quay.io/coreos/etcd:v3.5.18 \
  /usr/local/bin/etcd \
  --data-dir=/etcd-data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://0.0.0.0:2379 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --initial-advertise-peer-urls=http://0.0.0.0:2380 \
  --initial-cluster=default=http://0.0.0.0:2380

# Run nixlbench
docker run -it --gpus all --network host \
  --ipc=host --ulimit memlock=-1 --ulimit stack=67108864 \
  -e AWS_DEFAULT_BUCKET=lmcache \
  nixlbench:v1.0.0.4071a53 \
  nixlbench \
  -etcd_endpoints http://localhost:2379 \
  -backend OBJ \
  -op_type WRITE \
  -num_iter 8192 \
  -warmup_iter 1024 \
  -num_threads 1 \
  -large_blk_iter_ftr 32 \
  -obj_endpoint_override http://10.93.244.74:8000 \
  -obj_scheme http \
  -obj_bucket_name lmcache \
  -obj_region us-east-1 \
  -obj_access_key 0555b35654ad1656d804 \
  -obj_secret_key h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
```

---

> **Order dependency:** hsc-21 must be fully up before Step 2, and hsc-14 gateway must be serving before Step 3.

---

## Teardown (reverse order)

### hsc-12
```bash
docker stop etcd-server
```

### hsc-14
```bash
# Stop Ceph RGW
pkill -u zhuyu -f ceph

# Stop DAOS agent (running in screen session "daos")
screen -S daos -X quit
# OR attach and Ctrl+C manually:
# screen -r daos
```

### hsc-21
```bash
# Stop DAOS server (running in screen session "daos")
screen -S daos -X quit
# OR attach and Ctrl+C manually:
# screen -r daos

# Optional: full cleanup before next run
sudo umount /mnt/daos0
sudo rm -rvf /mnt/daos*
bash ~/GDOS/spdk_erase.sh
```