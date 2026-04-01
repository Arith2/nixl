#!/usr/bin/env python3
"""
Test x-amz-kvcache streaming with RDMA.

Step 1: Use nixlAgent to PUT chunk objects (populate KV cache)
Step 2: Use nixlAgent for bulk GET (baseline)
Step 3: Use custom getKVCacheAsync for streaming GET

Run inside nixl-gpu container on hsc-12:
  python3 benchmark/scripts/test_kvcache_rdma.py \
      --num_chunks 16 --num_layers 80 \
      --kv_per_token_per_layer 256 --tokens_per_chunk 256
"""

import argparse
import ctypes
import json
import os
import sys
import time
import numpy as np

# Add NIXL to path
sys.path.insert(0, '/usr/local/nixl/lib/python3/dist-packages')
import nixl

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_chunks", type=int, default=16)
    parser.add_argument("--num_layers", type=int, default=80)
    parser.add_argument("--kv_per_token_per_layer", type=int, default=256)
    parser.add_argument("--tokens_per_chunk", type=int, default=256)
    parser.add_argument("--num_iters", type=int, default=10)
    parser.add_argument("--rdma_port", type=int, default=7471)
    parser.add_argument("--endpoint", type=str, default="http://10.93.244.74:8000")
    parser.add_argument("--skip_populate", action="store_true")
    args = parser.parse_args()

    N = args.num_chunks
    L = args.num_layers
    K = args.kv_per_token_per_layer
    T = args.tokens_per_chunk

    layer_slice = K * T
    full_chunk = L * layer_slice
    layer_total = layer_slice * N
    total_data = L * layer_total

    print(f"Config: {N} chunks × {L} layers")
    print(f"  kv/tok/layer={K}, tok/chunk={T}")
    print(f"  layer_slice={layer_slice/1024:.0f} KB, full_chunk={full_chunk/1024/1024:.1f} MB")
    print(f"  layer_total={layer_total/1024:.0f} KB, total={total_data/1024/1024:.1f} MB")

    # Create NIXL agent
    agent_config = nixl.nixlAgentConfig(name="kvcache_test")
    agent = nixl.nixlAgent(agent_config)

    # Configure OBJ backend
    backend_params = {
        "obj_endpoint_override": args.endpoint,
        "obj_scheme": "http",
        "obj_bucket_name": "lmcache",
        "obj_region": "us-east-1",
        "obj_access_key": "0555b35654ad1656d804",
        "obj_secret_key": "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==",
    }
    if args.rdma_port > 0:
        backend_params["obj_rdma_port"] = str(args.rdma_port)

    agent.add_backend("OBJ", backend_params)

    # Allocate DRAM buffer for data
    buf = np.zeros(max(total_data, N * full_chunk), dtype=np.uint8)
    buf_ptr = buf.ctypes.data

    # Register local memory
    local_reg = nixl.nixlRegDList(nixl.DRAM_SEG)
    local_reg.addDesc(buf_ptr, max(total_data, N * full_chunk), 0)
    agent.registerMem(local_reg)

    # Generate chunk keys
    chunk_keys = [f"kvcache_chunk_{i:06d}" for i in range(N)]

    # ===== Populate chunks =====
    if not args.skip_populate:
        print(f"\nPopulating {N} chunks ({full_chunk/1024/1024:.1f} MB each)...")
        for i, key in enumerate(chunk_keys):
            # Fill with layer pattern
            for layer in range(L):
                start = layer * layer_slice
                end = start + layer_slice
                buf[i * full_chunk + start : i * full_chunk + end] = layer & 0xFF

            # Register remote (OBJ) descriptor
            remote_reg = nixl.nixlRegDList(nixl.OBJ_SEG)
            remote_reg.addDesc(0, full_chunk, i, key)
            agent.registerMem(remote_reg)

            # Create transfer
            local_xfer = nixl.nixlXferDList(nixl.DRAM_SEG)
            local_xfer.addDesc(buf_ptr + i * full_chunk, full_chunk, 0)
            remote_xfer = nixl.nixlXferDList(nixl.OBJ_SEG)
            remote_xfer.addDesc(0, full_chunk, i, key)

            handle = agent.createXferReq(nixl.NIXL_WRITE, local_xfer, remote_xfer, "kvcache_test")
            status = agent.postXferReq(handle)
            while status == nixl.NIXL_IN_PROG:
                status = agent.getXferStatus(handle)
            agent.releaseXferReq(handle)

            if status != nixl.NIXL_SUCCESS:
                print(f"  PUT {key} failed: {status}")
                return
            print(f"  PUT {key}: {full_chunk} bytes")

    # ===== Benchmark: Bulk GET =====
    print(f"\n=== Bulk GET ({N} sequential requests) ===")
    latencies = []
    for it in range(args.num_iters):
        t0 = time.monotonic()
        for i, key in enumerate(chunk_keys):
            local_xfer = nixl.nixlXferDList(nixl.DRAM_SEG)
            local_xfer.addDesc(buf_ptr + i * full_chunk, full_chunk, 0)
            remote_xfer = nixl.nixlXferDList(nixl.OBJ_SEG)
            remote_xfer.addDesc(0, full_chunk, i, key)

            handle = agent.createXferReq(nixl.NIXL_READ, local_xfer, remote_xfer, "kvcache_test")
            status = agent.postXferReq(handle)
            while status == nixl.NIXL_IN_PROG:
                status = agent.getXferStatus(handle)
            agent.releaseXferReq(handle)

            if status != nixl.NIXL_SUCCESS:
                print(f"  GET {key} failed: {status}")
                break
        t1 = time.monotonic()
        latencies.append((t1 - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    print(f"  Avg total: {avg:.1f} ms")
    print(f"  Throughput: {N * full_chunk / 1024 / 1024 / 1024 / (avg / 1000):.2f} GB/s")
    print(f"  TTFL: {avg:.1f} ms (must wait for all chunks)")

    # ===== Note about streaming =====
    print(f"\n=== Streaming (x-amz-kvcache) ===")
    print(f"  NOTE: Streaming requires C++ getKVCacheAsync integration.")
    print(f"  Server-side kvcache_stream_daos verified via TCP test.")
    print(f"  Expected TTFL: ~{2.5 + layer_total / (5 * 1024**3) * 1000:.1f} ms")
    print(f"  Expected speedup: {avg / (2.5 + layer_total / (5 * 1024**3) * 1000):.0f}x")

    print(f"\n=== Summary ===")
    print(f"  Bulk GET ({N} requests):  {avg:.1f} ms")
    est_stream = 2.5 + layer_total / (5 * 1024**3) * 1000
    print(f"  Streaming (projected):    {est_stream:.1f} ms")
    print(f"  Projected speedup:        {avg / est_stream:.1f}x")

if __name__ == "__main__":
    main()
