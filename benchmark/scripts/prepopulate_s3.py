#!/usr/bin/env python3
"""
Pre-populate S3 objects for cold-read NIXL benchmarks.

Writes block sizes in order (small -> large) so that the large writes
flush all smaller-block data out of the DAOS SCM/DRAM tier before
benchmarks start.  With 12K objects per block size (default), the
warmup (1024) + benchmark (8192) iterations only access the first 9216
objects, all of which will be cold on NVMe.

Object naming:
    prepop_{SIZE}_{INDEX:06d}    e.g.  prepop_64MB_000042
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

# Ordered small -> large  (do not reorder)
BLOCK_SIZES = [
    (4   * 1024,       "4KB"),
    (8   * 1024,       "8KB"),
    (16  * 1024,       "16KB"),
    (32  * 1024,       "32KB"),
    (64  * 1024,       "64KB"),
    (128 * 1024,       "128KB"),
    (256 * 1024,       "256KB"),
    (512 * 1024,       "512KB"),
    (1   * 1024**2,    "1MB"),
    (2   * 1024**2,    "2MB"),
    (4   * 1024**2,    "4MB"),
    (8   * 1024**2,    "8MB"),
    (16  * 1024**2,    "16MB"),
    (32  * 1024**2,    "32MB"),
    (64  * 1024**2,    "64MB"),
]


def make_client(endpoint: str) -> boto3.client:
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        config=cfg,
        region_name="us-east-1",
    )


def upload_one(client, bucket: str, key: str, data: bytes) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=data)


def populate_block_size(
    endpoint: str,
    bucket: str,
    size_bytes: int,
    size_name: str,
    num_objects: int,
    workers: int,
) -> int:
    """Upload num_objects of size_bytes.  Returns number of errors."""
    # Each worker gets its own client to avoid connection contention.
    clients = [make_client(endpoint) for _ in range(workers)]

    print(f"\n[{size_name}] generating {size_bytes:,} B payload ...", flush=True)
    data = os.urandom(size_bytes)

    print(
        f"[{size_name}] uploading {num_objects:,} objects "
        f"({workers} workers) ...",
        flush=True,
    )
    t0 = time.monotonic()
    done = 0
    errors = 0

    def _upload(idx: int) -> None:
        key = f"prepop_{size_name}_{idx:06d}"
        upload_one(clients[idx % workers], bucket, key, data)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_upload, i): i for i in range(num_objects)}
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as exc:
                errors += 1
                print(f"  WARN upload error: {exc}", flush=True)
            else:
                done += 1
                if done % 1000 == 0:
                    elapsed = time.monotonic() - t0
                    bw = done * size_bytes / elapsed / 1e9
                    print(f"  {done}/{num_objects}  {bw:.2f} GB/s", flush=True)

    elapsed = time.monotonic() - t0
    total_gb = num_objects * size_bytes / 1e9
    bw = total_gb / elapsed if elapsed > 0 else 0
    print(
        f"[{size_name}] done: {done} ok / {errors} err  "
        f"{total_gb:.1f} GB  {elapsed:.1f}s  {bw:.2f} GB/s",
        flush=True,
    )
    return errors


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Pre-populate S3 objects for cold-read NIXL benchmark"
    )
    ap.add_argument(
        "--endpoint", default="http://127.0.0.1:8000",
        help="S3 endpoint URL (default: http://127.0.0.1:8000)",
    )
    ap.add_argument(
        "--bucket", default="lmcache",
        help="S3 bucket name (default: lmcache)",
    )
    ap.add_argument(
        "--num", type=int, default=12000,
        help="objects per block size (default: 12000); "
             "warmup(1024)+bench(8192)=9216 must be <= num - ceil(125GB/block_size)",
    )
    ap.add_argument(
        "--workers", type=int, default=32,
        help="parallel upload threads (default: 32); "
             "reduce to 4-8 for 32MB/64MB if network saturates",
    )
    ap.add_argument(
        "--sizes", nargs="+", default=None,
        metavar="SIZE",
        help="limit to specific sizes, e.g. --sizes 4KB 64MB "
             "(preserves small->large order)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="print what would be uploaded without actually uploading",
    )
    args = ap.parse_args()

    selected = BLOCK_SIZES
    if args.sizes:
        valid = {n for _, n in BLOCK_SIZES}
        unknown = set(args.sizes) - valid
        if unknown:
            sys.exit(f"Unknown size(s): {unknown}. Valid: {sorted(valid)}")
        name_set = set(args.sizes)
        selected = [(b, n) for b, n in BLOCK_SIZES if n in name_set]

    total_bytes = sum(b * args.num for b, _ in selected)
    print(f"endpoint : {args.endpoint}")
    print(f"bucket   : {args.bucket}")
    print(f"objects  : {args.num:,} per block size × {len(selected)} sizes")
    print(f"total    : {total_bytes / 1e12:.3f} TB")
    print(f"workers  : {args.workers}")
    print(f"order    : small -> large  (evicts smaller data from DAOS SCM)")
    print(f"key fmt  : prepop_{{SIZE}}_{{INDEX:06d}}")

    if args.dry_run:
        print("\n[dry-run] sizes to upload:")
        for size_bytes, size_name in selected:
            print(f"  {size_name:6s}  {args.num:6,} objects  "
                  f"{size_bytes * args.num / 1e9:.1f} GB")
        return

    total_errors = 0
    t_start = time.monotonic()
    for size_bytes, size_name in selected:
        total_errors += populate_block_size(
            args.endpoint, args.bucket,
            size_bytes, size_name,
            args.num, args.workers,
        )

    elapsed = time.monotonic() - t_start
    print(f"\nAll done in {elapsed:.0f}s. Total errors: {total_errors}")
    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
