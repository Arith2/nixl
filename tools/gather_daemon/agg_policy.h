// agg_policy.h — default agg_size selection for KV-cache workloads.
//
// Header-only. Shared by the NIXL OBJ adapter (Phase 3) and by test tools.
// The daemon does NOT read this file — policy lives in the client, the
// daemon just groups per the `agg_params` it receives.
//
// Policy: target 4 MiB ≤ RDMA event size ≤ 16 MiB.
//   - 4 MiB amortizes per-WQE overhead on 100 GbE.
//   - 16 MiB caps head-of-line blocking on the decode critical path.
// If per-layer size is in-band: 1 aggregation per layer (sweet spot).
// If per-layer size > 16 MiB: split each layer into ≤ 16 MiB groups.
// If per-layer size < 4 MiB: keep 1 per layer anyway (don't merge layers —
// that would delay layer-0 readiness and hurt TTFL).

#pragma once

#include <cstdint>

namespace aggpolicy {

struct KvcacheDesc {
    uint32_t num_layers;
    uint32_t kv_per_token_per_layer;   // bytes (e.g. 4096 for Llama 3.1 8B @ fp16)
    uint32_t chunk_tokens;             // storage granularity (e.g. 16 or 64)
    uint32_t isl;                      // context length in tokens
};

constexpr uint32_t AGG_SWEET_MAX = 16u * 1024u * 1024u;   // 16 MiB
constexpr uint32_t AGG_SWEET_MIN =  4u * 1024u * 1024u;   // 4 MiB

// Returns the number of chunks per aggregation group, per the 4-16 MiB
// sweet-spot band. Never returns 0; falls back to chunks_per_layer if the
// band math would otherwise underflow.
inline uint32_t pick_agg_size(const KvcacheDesc &kv) {
    const uint32_t per_chunk_bytes    = kv.chunk_tokens * kv.kv_per_token_per_layer;
    const uint32_t chunks_per_layer   = kv.isl / kv.chunk_tokens;
    const uint32_t per_layer_bytes    = chunks_per_layer * per_chunk_bytes;

    if (per_layer_bytes <= AGG_SWEET_MAX) {
        // In-band: one aggregation per layer — clean layer boundary for TTFL.
        return chunks_per_layer;
    }
    // Out of band: split layer into equal groups of up to AGG_SWEET_MAX each.
    // Round-down so the group size stays ≤ 16 MiB.
    const uint32_t chunks_per_group = AGG_SWEET_MAX / per_chunk_bytes;
    if (chunks_per_group == 0) {
        // Per-chunk already > 16 MiB (extreme chunk_tokens). Degenerate to 1.
        return 1;
    }
    return chunks_per_group;
}

// Returns the number of aggregation groups that will result per logical load.
inline uint32_t num_groups(const KvcacheDesc &kv, uint32_t agg_size) {
    const uint32_t chunks_per_layer = kv.isl / kv.chunk_tokens;
    const uint32_t total_chunks     = chunks_per_layer * kv.num_layers;
    if (agg_size == 0) return 0;
    return (total_chunks + agg_size - 1) / agg_size;
}

// Returns bytes per aggregation group (i.e. per RDMA WRITE_WITH_IMM event).
inline uint64_t group_bytes(const KvcacheDesc &kv, uint32_t agg_size) {
    const uint32_t per_chunk_bytes = kv.chunk_tokens * kv.kv_per_token_per_layer;
    return (uint64_t)agg_size * (uint64_t)per_chunk_bytes;
}

}  // namespace aggpolicy
