// rdma_common.h — header-only helpers for QP setup and WRITE_WITH_IMM posting.
//
// Shared by the daemon (main.cpp) and the standalone test client (test_client.cpp)
// so both speak the exact same ibverbs dialect. Inline-only; no .cpp linkage.

#pragma once

#include "agg_proto.h"

#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include <cstdio>
#include <cstring>
#include <string>

namespace rdmac {

// ── GID ↔ string helpers (matches Linux RoCE userspace convention) ─────────
inline std::string gid_to_str(const ibv_gid &g) {
    char s[64];
    inet_ntop(AF_INET6, g.raw, s, sizeof(s));
    return std::string(s);
}
inline bool str_to_gid(const std::string &s, ibv_gid &out) {
    return inet_pton(AF_INET6, s.c_str(), out.raw) == 1;
}

inline ibv_mtu mtu_from_bytes(uint32_t b) {
    switch (b) {
        case 256:  return IBV_MTU_256;
        case 512:  return IBV_MTU_512;
        case 1024: return IBV_MTU_1024;
        case 2048: return IBV_MTU_2048;
        case 4096:
        default:   return IBV_MTU_4096;
    }
}

// ── Device + port discovery ────────────────────────────────────────────────
struct PortInfo {
    ibv_context *ctx    = nullptr;
    uint8_t      port   = 1;
    uint8_t      gid_idx = 1;     // RoCE v2 default on our cluster
    ibv_port_attr pattr{};
    ibv_gid      gid{};
    std::string  dev_name;
};

// Find first active IB/RoCE port; caller may override dev_name / port / gid_idx
// via cli flags. Returns true and fills `out` on success.
inline bool open_first_port(PortInfo &out,
                            const std::string &want_dev = "",
                            int want_port = -1,
                            int want_gid_idx = -1) {
    int n = 0;
    ibv_device **list = ibv_get_device_list(&n);
    if (!list || n == 0) { std::fprintf(stderr, "no IB devices\n"); return false; }

    for (int i = 0; i < n; ++i) {
        const char *name = ibv_get_device_name(list[i]);
        if (!want_dev.empty() && want_dev != name) continue;

        ibv_context *c = ibv_open_device(list[i]);
        if (!c) continue;

        ibv_device_attr da{};
        if (ibv_query_device(c, &da)) { ibv_close_device(c); continue; }

        for (uint8_t p = 1; p <= da.phys_port_cnt; ++p) {
            if (want_port > 0 && (int)p != want_port) continue;
            ibv_port_attr pa{};
            if (ibv_query_port(c, p, &pa)) continue;
            if (pa.state != IBV_PORT_ACTIVE) continue;

            int gi = (want_gid_idx >= 0) ? want_gid_idx : 1;
            ibv_gid g{};
            if (ibv_query_gid(c, p, gi, &g)) continue;

            out.ctx      = c;
            out.port     = p;
            out.gid_idx  = (uint8_t)gi;
            out.pattr    = pa;
            out.gid      = g;
            out.dev_name = name;
            ibv_free_device_list(list);
            return true;
        }
        ibv_close_device(c);
    }
    ibv_free_device_list(list);
    std::fprintf(stderr, "no active port matched (dev='%s' port=%d)\n",
                 want_dev.c_str(), want_port);
    return false;
}

// ── QP state transitions (RC) ──────────────────────────────────────────────
inline bool modify_qp_init(ibv_qp *qp, uint8_t port_num) {
    ibv_qp_attr a{};
    a.qp_state        = IBV_QPS_INIT;
    a.port_num        = port_num;
    a.pkey_index      = 0;
    a.qp_access_flags = IBV_ACCESS_LOCAL_WRITE
                      | IBV_ACCESS_REMOTE_WRITE
                      | IBV_ACCESS_REMOTE_READ;
    int rc = ibv_modify_qp(qp, &a,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (rc) std::fprintf(stderr, "modify_qp_init rc=%d (%s)\n", rc, strerror(rc));
    return rc == 0;
}

inline bool modify_qp_rtr(ibv_qp *qp, uint8_t port_num, uint8_t gid_idx,
                          const aggproto::QpInfo &remote) {
    ibv_qp_attr a{};
    a.qp_state           = IBV_QPS_RTR;
    a.path_mtu           = mtu_from_bytes(remote.mtu);
    a.dest_qp_num        = remote.qpn;
    a.rq_psn             = remote.psn;
    a.max_dest_rd_atomic = 1;
    a.min_rnr_timer      = 12;
    a.ah_attr.is_global  = 1;
    a.ah_attr.port_num   = port_num;
    a.ah_attr.dlid       = (uint16_t)remote.lid;
    a.ah_attr.sl         = 0;
    a.ah_attr.src_path_bits = 0;
    a.ah_attr.grh.sgid_index = gid_idx;
    a.ah_attr.grh.hop_limit  = 1;
    str_to_gid(remote.gid, a.ah_attr.grh.dgid);
    int rc = ibv_modify_qp(qp, &a,
        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN
        | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (rc) std::fprintf(stderr, "modify_qp_rtr rc=%d (%s)\n", rc, strerror(rc));
    return rc == 0;
}

inline bool modify_qp_rts(ibv_qp *qp, uint32_t local_psn) {
    ibv_qp_attr a{};
    a.qp_state      = IBV_QPS_RTS;
    a.timeout       = 14;
    a.retry_cnt     = 7;
    a.rnr_retry     = 7;
    a.sq_psn        = local_psn;
    a.max_rd_atomic = 1;
    int rc = ibv_modify_qp(qp, &a,
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT
        | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
    if (rc) std::fprintf(stderr, "modify_qp_rts rc=%d (%s)\n", rc, strerror(rc));
    return rc == 0;
}

// ── Post primitives ────────────────────────────────────────────────────────

// Post an empty receive WR to consume one inbound IBV_WR_RDMA_WRITE_WITH_IMM
// (WRITE_WITH_IMM requires a posted recv even though it carries no payload
// targeted at any local buffer — the data lands via rkey, the WR delivers
// the completion + imm_data).
inline bool post_dummy_recv(ibv_qp *qp, uint64_t wr_id) {
    ibv_recv_wr wr{};
    wr.wr_id  = wr_id;
    wr.sg_list = nullptr;
    wr.num_sge = 0;
    ibv_recv_wr *bad = nullptr;
    int rc = ibv_post_recv(qp, &wr, &bad);
    if (rc) std::fprintf(stderr, "post_dummy_recv rc=%d\n", rc);
    return rc == 0;
}

inline bool post_write_imm(ibv_qp *qp, uint32_t lkey, uint64_t wr_id,
                           const void *src, uint32_t len,
                           uint64_t remote_addr, uint32_t remote_rkey,
                           uint32_t imm_data) {
    ibv_sge sge{};
    sge.addr   = (uintptr_t)src;
    sge.length = len;
    sge.lkey   = lkey;
    ibv_send_wr wr{};
    wr.wr_id   = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode  = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(imm_data);
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey        = remote_rkey;
    ibv_send_wr *bad = nullptr;
    int rc = ibv_post_send(qp, &wr, &bad);
    if (rc) std::fprintf(stderr, "post_write_imm rc=%d (%s)\n", rc, strerror(rc));
    return rc == 0;
}

}  // namespace rdmac
