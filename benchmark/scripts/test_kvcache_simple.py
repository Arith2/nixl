#!/usr/bin/env python3
"""
Simple test for x-amz-kvcache streaming descriptor.
Populates chunks via PUT, then tests bulk GET vs streaming GET.

Usage:
  # First start DAOS RDMA + Ceph RGW + etcd per RUNBOOK
  python3 test_kvcache_simple.py --num_chunks 16 --num_layers 80 \
      --kv_per_token_per_layer 256 --tokens_per_chunk 256

  # For Llama 8B (TP=1): --num_layers 32 --kv_per_token_per_layer 2048
  # For Llama 70B (TP=8): --num_layers 80 --kv_per_token_per_layer 256
"""

import argparse
import json
import time
import boto3
from botocore.config import Config as BotoConfig

ENDPOINT = "http://10.93.244.74:8000"
BUCKET = "lmcache"
ACCESS_KEY = "0555b35654ad1656d804"
SECRET_KEY = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="


def make_s3_client():
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name='us-east-1',
        config=BotoConfig(signature_version='s3v4'),
    )


def populate_chunks(s3, num_chunks, num_layers, kv_per_token_per_layer, tokens_per_chunk):
    """Write synthetic chunk objects to S3."""
    chunk_size = num_layers * kv_per_token_per_layer * tokens_per_chunk
    print(f"Populating {num_chunks} chunks, {chunk_size} bytes each ({chunk_size/1024/1024:.1f} MB)...")

    keys = []
    for i in range(num_chunks):
        key = f"kvcache_chunk_{i:06d}"
        keys.append(key)
        # Fill with deterministic pattern (layer_idx in each byte for verification)
        data = bytearray(chunk_size)
        layer_slice = kv_per_token_per_layer * tokens_per_chunk
        for layer in range(num_layers):
            start = layer * layer_slice
            end = start + layer_slice
            data[start:end] = bytes([layer & 0xFF]) * layer_slice

        s3.put_object(Bucket=BUCKET, Key=key, Body=bytes(data))
        print(f"  PUT {key}: {chunk_size} bytes")

    return keys


def benchmark_bulk_get(s3, keys, chunk_size, num_iters=10):
    """Benchmark: sequential GET of all chunks."""
    print(f"\n=== Bulk GET ({len(keys)} sequential requests) ===")

    latencies = []
    for it in range(num_iters):
        t0 = time.monotonic()
        for key in keys:
            resp = s3.get_object(Bucket=BUCKET, Key=key)
            resp['Body'].read()
        t1 = time.monotonic()
        latencies.append((t1 - t0) * 1000)

    avg = sum(latencies) / len(latencies)
    total_data = len(keys) * chunk_size
    print(f"  Avg total: {avg:.1f} ms")
    print(f"  Throughput: {total_data / 1024 / 1024 / 1024 / (avg / 1000):.3f} GB/s")
    print(f"  TTFL: {avg:.1f} ms (must wait for all chunks)")
    return avg


def benchmark_kvcache_stream(s3, keys, num_layers, kv_per_token_per_layer, tokens_per_chunk, num_iters=10):
    """Benchmark: single x-amz-kvcache request via custom header."""
    print(f"\n=== Streaming (1 x-amz-kvcache request) ===")
    print(f"  NOTE: This test uses TCP (no RDMA). Server processes x-amz-kvcache but returns data over HTTP body.")

    kvcache_json = json.dumps({
        "chunks": keys,
        "num_layers": num_layers,
        "kv_per_token_per_layer": kv_per_token_per_layer,
        "tokens_per_chunk": tokens_per_chunk,
    })

    layer_slice = kv_per_token_per_layer * tokens_per_chunk
    total_data = num_layers * layer_slice * len(keys)

    # Use botocore's event system to inject custom header into a normal GET request
    import botocore.session

    session = botocore.session.get_session()
    raw_client = session.create_client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name='us-east-1',
    )

    def add_kvcache_header(request, **kwargs):
        request.headers['x-amz-kvcache'] = kvcache_json

    raw_client.meta.events.register('before-send.s3.GetObject', add_kvcache_header)

    latencies = []
    for it in range(num_iters):
        t0 = time.monotonic()
        resp = raw_client.get_object(Bucket=BUCKET, Key=keys[0])
        body = resp['Body'].read()
        t1 = time.monotonic()
        latencies.append((t1 - t0) * 1000)

        if it == 0:
            print(f"  Response: {resp['ResponseMetadata']['HTTPStatusCode']}, body_len={len(body)}")

    avg = sum(latencies) / len(latencies)
    print(f"  Avg total: {avg:.1f} ms")
    if avg > 0:
        print(f"  Throughput: {total_data / 1024 / 1024 / 1024 / (avg / 1000):.3f} GB/s")
    print(f"  TTFL (est): ~{avg / max(num_layers,1):.1f} ms (first layer delivered)")
    return avg


def main():
    parser = argparse.ArgumentParser(description="Test x-amz-kvcache streaming")
    parser.add_argument("--num_chunks", type=int, default=16)
    parser.add_argument("--num_layers", type=int, default=80)
    parser.add_argument("--kv_per_token_per_layer", type=int, default=256)
    parser.add_argument("--tokens_per_chunk", type=int, default=256)
    parser.add_argument("--num_iters", type=int, default=10)
    parser.add_argument("--skip_populate", action="store_true")
    args = parser.parse_args()

    chunk_size = args.num_layers * args.kv_per_token_per_layer * args.tokens_per_chunk
    total = chunk_size * args.num_chunks
    layer_slice = args.kv_per_token_per_layer * args.tokens_per_chunk
    layer_total = layer_slice * args.num_chunks

    print(f"Config: {args.num_chunks} chunks × {args.num_layers} layers")
    print(f"  chunk_size={chunk_size/1024/1024:.1f} MB, layer_slice={layer_slice/1024:.0f} KB")
    print(f"  layer_total={layer_total/1024:.0f} KB, total={total/1024/1024:.1f} MB")
    print()

    s3 = make_s3_client()

    # Populate
    if not args.skip_populate:
        keys = populate_chunks(s3, args.num_chunks, args.num_layers,
                              args.kv_per_token_per_layer, args.tokens_per_chunk)
        if keys is None:
            return
    else:
        keys = [f"kvcache_chunk_{i:06d}" for i in range(args.num_chunks)]

    # Benchmark
    bulk_ms = benchmark_bulk_get(s3, keys, chunk_size, args.num_iters)
    stream_ms = benchmark_kvcache_stream(s3, keys, args.num_layers,
                                         args.kv_per_token_per_layer,
                                         args.tokens_per_chunk,
                                         args.num_iters)

    if bulk_ms and stream_ms:
        print(f"\n=== Summary ===")
        print(f"  Bulk GET:   {bulk_ms:.1f} ms")
        print(f"  Streaming:  {stream_ms:.1f} ms")
        print(f"  Speedup:    {bulk_ms / stream_ms:.1f}x")


if __name__ == "__main__":
    main()
