// agg_proto.h — wire schemas for the NIXL ↔ gather_daemon protocol.
//
// Shared by the daemon (server) and the NIXL OBJ adapter (client). Keep this
// header self-contained: it depends only on nlohmann::json and <string>/<vector>.
//
// All structures here map directly to JSON bodies that travel through the
// Ceph RGW control plane (option-C marker) between the serving node and the
// gather_daemon co-located with DAOS.

#pragma once

#include "third_party/json.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace aggproto {

using json = nlohmann::json;

// ── RDMA endpoint descriptor ───────────────────────────────────────────────
// Exchanged both directions during the lazy session handshake that piggybacks
// on the first /agg/_open call. Represents one side of a Reliable-Connected
// QP pair.
struct QpInfo {
    uint32_t    qpn  = 0;
    uint32_t    psn  = 0;
    uint32_t    lid  = 0;        // 0 if RoCE
    uint32_t    mtu  = 4096;     // IBV_MTU_* translated to bytes
    uint8_t     port_num = 1;
    std::string gid;             // "fe80::1234:..." form (IPv6-literal of GID)
};

inline void to_json(json &j, const QpInfo &q) {
    j = json{{"qpn", q.qpn},   {"psn", q.psn},       {"lid", q.lid},
             {"mtu", q.mtu},   {"port_num", q.port_num}, {"gid", q.gid}};
}
inline void from_json(const json &j, QpInfo &q) {
    j.at("qpn").get_to(q.qpn);
    j.at("psn").get_to(q.psn);
    if (j.contains("lid"))      j.at("lid").get_to(q.lid);
    if (j.contains("mtu"))      j.at("mtu").get_to(q.mtu);
    if (j.contains("port_num")) j.at("port_num").get_to(q.port_num);
    j.at("gid").get_to(q.gid);
}

// ── Chunk descriptor — one DAOS object to read ─────────────────────────────
// Offsets are intra-object (normally 0; nonzero only for partial reads).
struct ChunkRef {
    std::string cont;    // DAOS container name (e.g. "lmcache")
    std::string key;     // DFS path under the container (e.g. "kv_65536B_0000000")
    uint64_t    off = 0;
    uint64_t    len = 0;
};

inline void to_json(json &j, const ChunkRef &c) {
    j = json{{"cont", c.cont}, {"key", c.key}, {"off", c.off}, {"len", c.len}};
}
inline void from_json(const json &j, ChunkRef &c) {
    j.at("cont").get_to(c.cont);
    j.at("key").get_to(c.key);
    j.at("off").get_to(c.off);
    j.at("len").get_to(c.len);
}

// ── Layer map — groups chunks into push units ──────────────────────────────
// Each entry describes one RDMA_WRITE_WITH_IMM the daemon will issue into the
// client's target buffer once that layer's chunks have all completed on DAOS.
// `imm_data` equals `idx` on the wire (see daemon worker).
struct LayerRange {
    uint32_t idx = 0;   // carried as imm_data
    uint64_t off = 0;   // offset within target buffer (and arena slice)
    uint64_t len = 0;   // byte length of this layer's slice
};

inline void to_json(json &j, const LayerRange &l) {
    j = json{{"idx", l.idx}, {"off", l.off}, {"len", l.len}};
}
inline void from_json(const json &j, LayerRange &l) {
    j.at("idx").get_to(l.idx);
    j.at("off").get_to(l.off);
    j.at("len").get_to(l.len);
}

// ── Target buffer — where the daemon writes via RDMA ───────────────────────
struct TargetBuf {
    uint64_t addr = 0;
    uint32_t rkey = 0;
    uint64_t len  = 0;
};

inline void to_json(json &j, const TargetBuf &t) {
    j = json{{"addr", t.addr}, {"rkey", t.rkey}, {"len", t.len}};
}
inline void from_json(const json &j, TargetBuf &t) {
    j.at("addr").get_to(t.addr);
    j.at("rkey").get_to(t.rkey);
    j.at("len").get_to(t.len);
}

// ── Request / response envelopes ───────────────────────────────────────────

// POST /agg/_open request body
//
// agg_mode selects the workload-specific grouping rule that the daemon applies
// to the flat `chunks[]` list. Supported values:
//   "custom"      — explicit grouping via layer_map[] (default; back-compat)
//   "kv_layer"    — KV cache, group into num_layers equal chunks_per_layer slices
//   "vector_flat" — vector retrieval, contiguous groups of chunks_per_group
//   "vector_shard"— per-chunk shard_id field; group by shard (future)
//   "range_window"— per-chunk entity_id field; group by entity (future)
//
// agg_params carries mode-specific knobs. Schema is opaque JSON here; each
// mode implementation parses what it needs.
struct OpenReq {
    std::string              type = "agg_open";
    std::string              session_id;        // empty on first call → daemon allocates
    std::string              req_id;
    std::vector<ChunkRef>    chunks;
    TargetBuf                target;
    // Grouping — exactly one of {layer_map, (agg_mode + agg_params)} is used.
    std::vector<LayerRange>  layer_map;         // populated when agg_mode == "custom"
    std::string              agg_mode = "custom";
    json                     agg_params = json::object();
    // Present only on session-initiating call; daemon then allocates a QP and
    // replies with its own QpInfo in the response.
    QpInfo                   client_qp;
    bool                     include_client_qp = false;
};

inline void to_json(json &j, const OpenReq &r) {
    j = json{{"type", r.type},
             {"session_id", r.session_id},
             {"req_id", r.req_id},
             {"chunks", r.chunks},
             {"target", r.target},
             {"agg_mode", r.agg_mode},
             {"agg_params", r.agg_params}};
    if (r.agg_mode == "custom") j["layer_map"] = r.layer_map;
    if (r.include_client_qp) j["client_qp"] = r.client_qp;
}
inline void from_json(const json &j, OpenReq &r) {
    if (j.contains("type")) j.at("type").get_to(r.type);
    if (j.contains("session_id")) j.at("session_id").get_to(r.session_id);
    j.at("req_id").get_to(r.req_id);
    j.at("chunks").get_to(r.chunks);
    j.at("target").get_to(r.target);
    if (j.contains("agg_mode"))   j.at("agg_mode").get_to(r.agg_mode);
    if (j.contains("agg_params")) r.agg_params = j.at("agg_params");
    if (j.contains("layer_map"))  j.at("layer_map").get_to(r.layer_map);
    if (j.contains("client_qp")) {
        j.at("client_qp").get_to(r.client_qp);
        r.include_client_qp = true;
    }
}

// POST /agg/_open response body
//
// synth_names[] is the ordered list of opaque identifiers assigned to each
// aggregation group; its length equals expected_writes, and its order matches
// the imm_data values (0 .. expected_writes-1) the daemon uses in
// RDMA_WRITE_WITH_IMM completions. For back-compat, agg_handle is kept as a
// single reference the client can use in DELETE /agg/<agg_handle>.
struct OpenResp {
    std::string              agg_handle;
    uint32_t                 expected_writes = 0;
    uint32_t                 ttl_ms = 30000;
    std::string              session_id;
    std::vector<std::string> synth_names;   // new: one per aggregation group
    QpInfo                   server_qp;
    bool                     include_server_qp = false;
};

inline void to_json(json &j, const OpenResp &r) {
    j = json{{"agg_handle", r.agg_handle},
             {"expected_writes", r.expected_writes},
             {"ttl_ms", r.ttl_ms},
             {"session_id", r.session_id},
             {"synth_names", r.synth_names}};
    if (r.include_server_qp) j["server_qp"] = r.server_qp;
}
inline void from_json(const json &j, OpenResp &r) {
    j.at("agg_handle").get_to(r.agg_handle);
    j.at("expected_writes").get_to(r.expected_writes);
    if (j.contains("ttl_ms")) j.at("ttl_ms").get_to(r.ttl_ms);
    j.at("session_id").get_to(r.session_id);
    if (j.contains("synth_names")) j.at("synth_names").get_to(r.synth_names);
    if (j.contains("server_qp")) {
        j.at("server_qp").get_to(r.server_qp);
        r.include_server_qp = true;
    }
}

// DELETE /agg/<agg_handle> — empty body both directions on success.

}  // namespace aggproto
