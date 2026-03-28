# DAOS Storage Baseline (hsc-21, 4× NVMe SSD via SPDK)

Measured on 2026-03-28. All tests run from hsc-14 via fio DFS ioengine
(direct DAOS POSIX API, `--direct=1`, no Linux page cache involvement).

## Setup

| Component | Config |
|-----------|--------|
| DAOS pool | Pool1, 2.1 TB total |
| Metadata tier | 125 GB memory-file (DRAM-backed SCM emulation) |
| Data tier | 2.0 TB NVMe (4× SSDs, SPDK) |
| Transport (hsc-14 ↔ hsc-21) | 100 GbE |
| DAOS container | `lmcache`, layout=POSIX (fresh, empty) |
| fio command base | `--ioengine=dfs --pool=Pool1 --cont=lmcache --direct=1 --numjobs=1` |

---

## WRITE Baseline

### 4 KB, iodepth=1

```
fio --bs=4KB --iodepth=1 --rw=write --size=100G --runtime=30s
```

| Metric | Value |
|--------|-------|
| Throughput | 23.4 MB/s |
| IOPS | 5718 |
| Avg latency | 174 µs |
| P50 latency | 155 µs |
| P99 latency | 215 µs |
| P99.9 latency | 322 µs |

**Note:** 4 KB write is metadata/IOPS-bound. 87% CPU usr — DAOS client overhead dominates.

---

### 64 MB, iodepth=1

```
fio --bs=64M --iodepth=1 --rw=write --size=1000G --runtime=120s
```

| Metric | Value |
|--------|-------|
| Throughput | 7507 MB/s (7.5 GB/s) |
| IOPS | 111 |
| Avg latency | 8.94 ms |
| P50 latency | 8 ms |
| P99 latency | 12 ms |
| P99.9 latency | 23 ms |
| P99.99 latency | 124 ms |
| Total written | 839 GiB (confirmed on NVMe, not DRAM cache) |

**Note:** Sustained 7.5 GB/s with 839 GiB written — confirmed real NVMe writes
(pool NVMe free dropped from 2.0 TB to 1.1 TB). Network utilization ~60 Gbps
(well below 100 GbE ceiling).

---

### 64 MB, iodepth=8

```
fio --bs=64M --iodepth=8 --rw=write --size=1000G --runtime=60s
```

| Metric | Value |
|--------|-------|
| Throughput | 10.5 GB/s |
| IOPS | 156 |
| Avg latency | 51.16 ms |
| P50 latency | 42 ms |
| P99 latency | 74 ms |
| P99.9 latency | 326 ms |
| P99.99 latency | 17113 ms |
| Total written | 586 GiB |

**Note:** ~88 Gbps network utilization — approaching 100 GbE saturation between
hsc-14 and hsc-21. P99.99 inflates dramatically due to head-of-line blocking
at network saturation. iodepth=8 throughput gain (+40%) comes at the cost of
significantly worse tail latency vs iodepth=1.

---

## READ Baseline

**Note:** Read tests run immediately after write tests on the same container —
data recently written is likely still in hsc-21's 125 GB DRAM memory tier,
explaining why reads are faster than writes at iodepth=1.

### 64 MB, iodepth=1

```
fio --bs=64M --iodepth=1 --rw=read --size=1000G --runtime=60s
```

| Metric | Value |
|--------|-------|
| Throughput | 10.5 GB/s (9.82 GiB/s) |
| IOPS | 157 |
| Avg latency | 6.37 ms |
| P50 latency | 6.19 ms |
| P99 latency | 6.65 ms |
| P99.9 latency | 7.57 ms |
| P99.99 latency | 18.22 ms |

**Note:** Exceptionally tight latency distribution (P50–P99 spread of only 0.46 ms).
Read throughput (10.5 GB/s) exceeds write throughput (7.5 GB/s) at iodepth=1 —
data was served from hsc-21's DRAM memory tier (warm cache from prior write run).
Network utilization ~84 Gbps.

---

### 64 MB, iodepth=8

```
fio --bs=64M --iodepth=8 --rw=read --size=1000G --runtime=60s
```

| Metric | Value |
|--------|-------|
| Throughput | 11.2 GB/s (10.4 GiB/s) |
| IOPS | 166 |
| Avg latency | 47.85 ms |
| P50 latency | 42 ms |
| P99 latency | 45 ms |
| P99.9 latency | 309 ms |
| P99.99 latency | 735 ms |

**Note:** Network-saturated (~90 Gbps). P99.99 (735 ms) is much better than
the write equivalent (17113 ms) — reads have no write-ordering constraints,
so head-of-line blocking is less severe. Marginal throughput gain (+6%) over
iodepth=1 at the cost of 7× higher avg latency.

---

## Summary

| Op | Block Size | iodepth | Throughput | Avg Lat | P99 Lat | P99.99 Lat |
|----|-----------|---------|-----------|---------|---------|------------|
| WRITE | 4 KB | 1 | 23.4 MB/s | 174 µs | 215 µs | — |
| WRITE | 64 MB | 1 | 7507 MB/s | 8.94 ms | 12 ms | 124 ms |
| WRITE | 64 MB | 8 | 10500 MB/s | 51.16 ms | 74 ms | 17113 ms |
| READ | 64 MB | 1 | 10500 MB/s | 6.37 ms | 6.65 ms | 18.22 ms |
| READ | 64 MB | 8 | 11200 MB/s | 47.85 ms | 45 ms | 735 ms |

## Bottleneck Analysis

- **WRITE 4 KB**: DAOS client CPU overhead + IOPS-bound metadata operations
- **WRITE 64 MB, iodepth=1**: NVMe SSD throughput ceiling (~60 Gbps of 100 GbE)
- **WRITE 64 MB, iodepth=8**: 100 GbE saturation (~88 Gbps); P99.99 collapses to 17 s
- **READ 64 MB, iodepth=1**: DRAM memory tier on hsc-21 (warm cache); very tight latency
- **READ 64 MB, iodepth=8**: Network-saturated (~90 Gbps); much better tail than write

**Network ceiling**: 100 GbE = 12.5 GB/s hard limit for hsc-14 ↔ hsc-21.
**NVMe write ceiling**: ~7.5 GB/s at iodepth=1 (cold NVMe).
**DRAM tier read ceiling**: ~10.5 GB/s at iodepth=1 (warm cache).

## Context for NIXL+Ceph Benchmarks

DAOS storage can sustain 7.5 GB/s write and 10.5 GB/s read (warm) at iodepth=1.
The current NIXL RDMA GET peak of 2.4 GB/s is therefore **not** limited by DAOS —
it is limited by Ceph RGW's single RDMA QP serialized by `rdma_mutex_`. Removing
that bottleneck (multi-connection RGW) could theoretically approach the DAOS NVMe
ceiling of ~7.5 GB/s for cold reads.

For small objects (≤1 MB), the bottleneck is Ceph RGW S3 control plane overhead
(~3.4 ms per request), not DAOS (which handles 4 KB in ~174 µs directly).
