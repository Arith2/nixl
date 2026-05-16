# NIXL Gather Daemon (Option Y push-RDMA)

Server-side KV cache aggregation for NIXL. Runs colocated with the DAOS
server on hsc-21. Receives aggregated-read requests from NIXL (forwarded
through Ceph), reads chunks from DAOS asynchronously into an MR-pinned
arena, then pushes per-layer slices into the client's pre-registered target
buffer via `RDMA_WRITE_WITH_IMM`.

See [RUNBOOK_DEV_NIXL_AGGREGATE.md](../../RUNBOOK_DEV_NIXL_AGGREGATE.md),
the "Revised MVP — Option Y push-RDMA" section, for architectural
background and wire schemas.

## Files

| File | Role |
|---|---|
| `agg_proto.h` | Wire schemas + JSON marshalers; shared with the NIXL OBJ adapter |
| `rdma_common.h` | Header-only ibverbs helpers (QP state transitions, GID parsing, post_write_imm, post_dummy_recv) |
| `main.cpp` | Daemon: HTTP server + DAOS bringup + arena + worker pool + RDMA endpoint |
| `test_client.cpp` | Standalone dry-run client (no NIXL, no Ceph) |
| `third_party/httplib.h`, `json.hpp` | Vendored header-only deps |

## Build

On hsc-21 (has DAOS install + libibverbs):

```bash
cd $NIXL_ROOT/tools/gather_daemon
make               # builds both gather_daemon and test_client
```

The client-only binary builds with just libibverbs and can be built on
any node:

```bash
make test_client   # no DAOS deps
```

## Dry-run smoke test

One logical load = one chunk = one layer. Tests both planes:
- Control plane: `POST /agg/_open` from test_client, through direct HTTP
  (Ceph skipped for the initial dry run).
- Data plane: daemon issues `RDMA_WRITE_WITH_IMM(imm=0)` into the client's
  target buffer; client's CQ delivers a `IBV_WC_RECV_RDMA_WITH_IMM` event.

### 1. Prepopulate a key in DAOS (on hsc-21)

```bash
# Prereqs: daos_server running; Pool1 / lmcache already created.
# Write a 64 KiB key via any existing tooling, e.g. nixlbench write mode,
# or a small dd into a dfs path. Key name: kv_65536B_0000000.
```

### 2. Start the daemon on hsc-21

```bash
cd $NIXL_ROOT/tools/gather_daemon
./gather_daemon --port 8080 --workers 8 \
    --source-pool Pool1 --source-cont lmcache
# Expected bootup lines:
#   DAOS ready — pool=Pool1
#   mounted cont='lmcache'
#   arena: 268435456 B (16 × 16777216 B) at 0x...
#   RDMA: dev=mlx5_0 port=1 gid_idx=1 gid=fe80::...
#   RDMA: arena MR registered  base=0x... size=268435456 lkey=0x...
#   worker pool: 8 threads up
#   listening on 0.0.0.0:8080
```

### 3. Run test_client on the serving node (or any node on the same IB/RoCE fabric)

```bash
cd $NIXL_ROOT/tools/gather_daemon
./test_client --daemon http://hsc-21:8080 \
              --cont lmcache --key kv_65536B_0000000 --len 65536 \
              --hexdump
```

Expected output:
```
RDMA: dev=mlx5_0 port=1 gid_idx=1 gid=...
target: 69632 B at 0x... rkey=0x...
client QP: qpn=... psn=...
POST /agg/_open  body=...
daemon resp: {"agg_handle":"h-...","expected_writes":1,"session_id":"s-...","server_qp":{...},"ttl_ms":30000}
client QP → RTS (peer qpn=... psn=...)
  ← layer imm=0 byte_len=65536
all 1 layer(s) received.
poison-byte count: <some small number, not 65536>
```

A non-zero-but-far-less-than-`len` poison-byte count confirms the RDMA
write landed. (Full zeros in the input key would give zero-count; the
test is meant for a non-uniform key.)

## HTTP endpoints

| Endpoint | Role |
|---|---|
| `GET /health` | Liveness check. Returns `{"status":"ok"}`. |
| `POST /agg/_open` | Open an aggregation. Body per `OpenReq` (see `agg_proto.h`). Returns `OpenResp` with `agg_handle`, `expected_writes`, `session_id`, and — on first call — `server_qp` for the handshake. |
| `DELETE /agg/<agg_handle>` | Release the aggregation's arena slot. Daemon also reaps on TTL (not yet wired). |

## Tuning flags

| Flag | Default | Meaning |
|---|---|---|
| `--port N` | 8080 | HTTP control port |
| `--workers N` | 8 | DAOS worker threads (each owns a `daos_event_queue`) |
| `--source-pool NAME` | Pool1 | DAOS pool for chunks |
| `--source-cont NAME` | lmcache | Default container (more mounted on-demand from JSON) |
| `--arena-size N` | 268435456 (256 MiB) | Total MR-registered arena |
| `--slot-size N` | 16777216 (16 MiB) | Per-open slot size; arena holds `arena_size/slot_size` slots |
| `--rdma-dev NAME` | (auto) | IB device name (e.g. `mlx5_0`) |
| `--rdma-port N` | (auto) | IB port number |
| `--rdma-gid-idx N` | 1 | GID index (RoCE v2 = 1 typically) |

## Known limitations (this iteration)

- No TTL sweeper yet (orphan slots live until daemon restart).
- Session QPs are not explicitly destroyed; hold for daemon lifetime.
- `DELETE /agg/<handle>` only releases registry state if the worker
  hasn't picked it up yet — otherwise the worker cleans its slot.
- No integration with Ceph forwarder yet — test_client bypasses Ceph.
- No integration with NIXL OBJ plugin yet — test_client is the only client.
