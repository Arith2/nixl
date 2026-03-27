/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBJ_PLUGIN_S3_RDMA_CTX_H
#define OBJ_PLUGIN_S3_RDMA_CTX_H

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

// Token embedded in x-amz-rdma-token HTTP header (hex-encoded, 24 bytes = 48 hex chars).
// Must match NixlRdmaToken in rgw_rdma.h.
struct __attribute__((packed)) NixlRdmaToken {
    uint64_t addr;    // virtual address of NIXL buffer
    uint32_t rkey;    // remote memory key
    uint32_t length;  // data length in bytes
    uint64_t offset;  // S3 object byte offset
};

// Manages the IB device context, RC QP connection to Ceph RGW, and per-buffer MR registration.
// One instance per DefaultObjEngineImpl. Initialized once, connection reused for all transfers.
class RdmaContext {
public:
    // server_addr: hostname/IP of Ceph RGW host (same as S3 endpoint host)
    // rdma_port: port the RGW RDMA CM server listens on (default 7471)
    RdmaContext(const std::string& server_addr, int rdma_port);
    ~RdmaContext();

    bool isEnabled() const { return connected_; }

    // Register addr/len for remote RDMA access. Returns false on failure.
    bool registerMR(void* addr, size_t len);
    void deregisterMR(void* addr);

    // Build 48-char hex token string for x-amz-rdma-token header.
    // Returns empty string if addr is not registered.
    std::string buildToken(void* addr, size_t len, uint64_t offset);

private:
    bool connect(const std::string& server_addr, int rdma_port);
    void resetConnection();

    struct rdma_event_channel* ec_  = nullptr;
    struct rdma_cm_id*         id_  = nullptr;
    struct ibv_pd*             pd_  = nullptr;
    struct ibv_cq*             cq_  = nullptr;
    bool                       connected_ = false;

    std::mutex mr_mutex_;
    std::unordered_map<uintptr_t, struct ibv_mr*> mr_map_;
};

#endif // OBJ_PLUGIN_S3_RDMA_CTX_H