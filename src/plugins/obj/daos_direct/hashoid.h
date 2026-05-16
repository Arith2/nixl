/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * hashoid.h — shared OID derivation for NIXL client-side hashoid mode
 * and DAOS agg_sidecar's emulate_hashoid_{write,read}.
 *
 * Both sides must agree on these functions byte-for-byte so that a
 * (T, IOD, BS, iter) tuple maps to the same DAOS object ID from either
 * side — enabling cross-mode read/write compatibility for validation
 * and apples-to-apples tiering measurements.
 *
 * This is an in-tree copy mirrored from the NIXL plugin tree at
 * src/plugins/obj/daos_direct/hashoid.h. The two copies must stay
 * byte-identical so that NIXL client and DAOS agg_sidecar agree on
 * OID derivation; if you modify one, update the other.
 *
 * Pure C arithmetic — no DAOS / NIXL / C++ headers required.
 */

#ifndef NIXL_HASHOID_H
#define NIXL_HASHOID_H

#include <stdint.h>
#include <stddef.h>

#define HASHOID_FIXED_SALT 0xBADC0FFEE0DDF00DULL

/* Deterministic OID-from-iter: splitmix64 of (iter * golden_ratio ^ SALT).
 * Same iter → same OID, regardless of thread id or dispatch order. */
static inline uint64_t hashoid_oid_lo(int iter)
{
    uint64_t z = ((uint64_t)iter * 0x9E3779B97F4A7C15ULL) ^ HASHOID_FIXED_SALT;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* Encode (T, IOD, BS) into the lower 32 bits of oid.hi so that each
 * (concurrency, block-size) tuple writes to a disjoint OID space within
 * a single container. daos_obj_set_oid_by_class (engine-side) and
 * daos_obj_generate_oid (client-side public API) both preserve these
 * 32 bits — they OR class/type/meta into the upper 32 only (see
 * daos_obj_set_oid in include/daos/object.h). */
static inline uint64_t hashoid_oid_hi_user(int T, int IOD, size_t BS)
{
    uint32_t bs_log = 0;
    size_t tmp = BS;
    while (tmp >>= 1) bs_log++;   /* floor(log2(BS)) */
    return ((uint64_t)(T      & 0xFF) << 24) |
           ((uint64_t)(IOD    & 0xFF) << 16) |
           ((uint64_t)(bs_log & 0xFF) <<  8) |
           0x01ULL;               /* version tag */
}

/* ──────────────────────────────────────────────────────────────────
 * Layered encoding for s3rdma_agg: sequential OIDs WITHIN a layer
 * (so server-side aggregation can compute chunk_oid_i.lo = agg_oid.lo + i),
 * randomized ACROSS layers (so VOS placement still balances across the
 * 8 target xstreams).
 *
 *   bits   N..C    : layer hash (splitmix64 on layer_idx, low bits cleared)
 *   bits   C..0    : chunk_idx within the layer (0 .. 2^HASHOID_CHUNK_BITS-1)
 *
 * Sample with HASHOID_CHUNK_BITS=16:
 *   hashoid_oid_lo_layered(L, 0) = some_random_lo & ~0xFFFF      (layer base)
 *   hashoid_oid_lo_layered(L, i) = base + i                       (sequential)
 *
 * For aggregation reads:
 *   client passes the FIRST chunk's OID (= layer base) as agg_oid
 *   server's stage A computes chunk_oid_i.lo = agg_oid.lo + i for i=0..N-1
 *
 * This is the encoding shared by:
 *   - tools/agg_smoke/agg_kvbench.c   (raw libdaos test client)
 *   - DAOS agg_sidecar bench thread   (engine-hashoid prepop / scrub)
 *   - NIXL OBJ s3_split_plane plugin  (s3rdma_direct / batch / agg paths)
 * ────────────────────────────────────────────────────────────────── */
#define HASHOID_CHUNK_BITS  16              /* up to 65536 chunks per layer */
#define HASHOID_CHUNK_MASK  ((1ULL << HASHOID_CHUNK_BITS) - 1ULL)

static inline uint64_t hashoid_oid_lo_layered(int layer_idx, int chunk_idx)
{
    uint64_t z = ((uint64_t)layer_idx * 0x9E3779B97F4A7C15ULL) ^ HASHOID_FIXED_SALT;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= (z >> 31);
    return (z & ~HASHOID_CHUNK_MASK) | ((uint64_t)chunk_idx & HASHOID_CHUNK_MASK);
}

/* Convenience: layer base = hashoid_oid_lo_layered(layer_idx, 0). */
static inline uint64_t hashoid_layer_base(int layer_idx)
{
    return hashoid_oid_lo_layered(layer_idx, 0);
}

#endif /* NIXL_HASHOID_H */
