/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rdma_ctx.h"
#include "common/nixl_log.h"

#include <chrono>
#include <netdb.h>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <thread>

static std::string
hexEncode(const void* data, size_t len) {
    const auto* p = reinterpret_cast<const uint8_t*>(data);
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; i++)
        ss << std::setw(2) << static_cast<int>(p[i]);
    return ss.str();
}

void
RdmaContext::resetConnection() {
    if (cq_) { ibv_destroy_cq(cq_); cq_ = nullptr; }
    if (id_) {
        if (id_->qp) rdma_destroy_qp(id_);
        rdma_destroy_id(id_);
        id_ = nullptr;
    }
    if (ec_) { rdma_destroy_event_channel(ec_); ec_ = nullptr; }
    pd_ = nullptr;
    connected_ = false;
}

RdmaContext::RdmaContext(const std::string& server_addr, int rdma_port) {
    constexpr int max_retries = 5;
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        if (attempt > 0) {
            resetConnection();
            NIXL_INFO << "RdmaContext: retry " << attempt << "/" << (max_retries - 1)
                      << " connecting to " << server_addr << ":" << rdma_port;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (connect(server_addr, rdma_port))
            return;
    }
    NIXL_WARN << "RdmaContext: could not connect to " << server_addr << ":" << rdma_port
              << " — RDMA disabled, using TCP fallback";
}

RdmaContext::~RdmaContext() {
    {
        std::lock_guard<std::mutex> lock(mr_mutex_);
        for (auto& [addr, mr] : mr_map_)
            ibv_dereg_mr(mr);
        mr_map_.clear();
    }
    if (cq_) ibv_destroy_cq(cq_);
    if (id_) {
        rdma_disconnect(id_);
        rdma_destroy_qp(id_);
        rdma_destroy_id(id_);
    }
    if (ec_) rdma_destroy_event_channel(ec_);
}

bool
RdmaContext::connect(const std::string& server_addr, int rdma_port) {
    ec_ = rdma_create_event_channel();
    if (!ec_) {
        NIXL_ERROR << "RdmaContext: rdma_create_event_channel failed";
        return false;
    }

    if (rdma_create_id(ec_, &id_, nullptr, RDMA_PS_TCP) != 0) {
        NIXL_ERROR << "RdmaContext: rdma_create_id failed: " << strerror(errno);
        return false;
    }

    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(rdma_port);
    if (getaddrinfo(server_addr.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        NIXL_ERROR << "RdmaContext: getaddrinfo failed for " << server_addr;
        return false;
    }

    int ret = rdma_resolve_addr(id_, nullptr, res->ai_addr, 2000 /*ms*/);
    freeaddrinfo(res);
    if (ret != 0) {
        NIXL_ERROR << "RdmaContext: rdma_resolve_addr failed: " << strerror(errno);
        return false;
    }

    struct rdma_cm_event* ev = nullptr;
    if (rdma_get_cm_event(ec_, &ev) != 0 || ev->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        NIXL_ERROR << "RdmaContext: expected ADDR_RESOLVED";
        if (ev) rdma_ack_cm_event(ev);
        return false;
    }
    rdma_ack_cm_event(ev);

    if (rdma_resolve_route(id_, 2000) != 0) {
        NIXL_ERROR << "RdmaContext: rdma_resolve_route failed: " << strerror(errno);
        return false;
    }
    if (rdma_get_cm_event(ec_, &ev) != 0 || ev->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        NIXL_ERROR << "RdmaContext: expected ROUTE_RESOLVED";
        if (ev) rdma_ack_cm_event(ev);
        return false;
    }
    rdma_ack_cm_event(ev);

    pd_ = ibv_alloc_pd(id_->verbs);
    if (!pd_) { NIXL_ERROR << "RdmaContext: ibv_alloc_pd failed"; return false; }

    cq_ = ibv_create_cq(id_->verbs, 64, nullptr, nullptr, 0);
    if (!cq_) { NIXL_ERROR << "RdmaContext: ibv_create_cq failed"; return false; }

    struct ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq          = cq_;
    qp_attr.recv_cq          = cq_;
    qp_attr.qp_type          = IBV_QPT_RC;
    qp_attr.cap.max_send_wr  = 64;
    qp_attr.cap.max_recv_wr  = 1;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    if (rdma_create_qp(id_, pd_, &qp_attr) != 0) {
        NIXL_ERROR << "RdmaContext: rdma_create_qp failed: " << strerror(errno);
        return false;
    }

    struct rdma_conn_param conn_param{};
    conn_param.responder_resources = 16;
    conn_param.initiator_depth     = 16;
    conn_param.retry_count         = 7;
    conn_param.rnr_retry_count     = 7;
    if (rdma_connect(id_, &conn_param) != 0) {
        NIXL_ERROR << "RdmaContext: rdma_connect failed: " << strerror(errno);
        return false;
    }
    if (rdma_get_cm_event(ec_, &ev) != 0 || ev->event != RDMA_CM_EVENT_ESTABLISHED) {
        NIXL_ERROR << "RdmaContext: expected ESTABLISHED, got "
                   << (ev ? rdma_event_str(ev->event) : "null");
        if (ev) rdma_ack_cm_event(ev);
        return false;
    }
    rdma_ack_cm_event(ev);

    NIXL_INFO << "RdmaContext: connected to " << server_addr << ":" << rdma_port;
    connected_ = true;
    return true;
}

bool
RdmaContext::registerMR(void* addr, size_t len) {
    if (!connected_) return false;

    struct ibv_mr* mr = ibv_reg_mr(
        pd_, addr, len,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        NIXL_ERROR << "RdmaContext: ibv_reg_mr failed for addr=" << addr
                   << " len=" << len << ": " << strerror(errno);
        return false;
    }

    std::lock_guard<std::mutex> lock(mr_mutex_);
    mr_map_[reinterpret_cast<uintptr_t>(addr)] = mr;
    NIXL_INFO << "RdmaContext: registered MR addr=" << addr << " len=" << len
              << " rkey=0x" << std::hex << mr->rkey;
    return true;
}

void
RdmaContext::deregisterMR(void* addr) {
    std::lock_guard<std::mutex> lock(mr_mutex_);
    auto it = mr_map_.find(reinterpret_cast<uintptr_t>(addr));
    if (it != mr_map_.end()) {
        ibv_dereg_mr(it->second);
        mr_map_.erase(it);
    }
}

std::string
RdmaContext::buildToken(void* addr, size_t len, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mr_mutex_);
    auto it = mr_map_.find(reinterpret_cast<uintptr_t>(addr));
    if (it == mr_map_.end()) return "";

    NixlRdmaToken tok{};
    tok.addr   = reinterpret_cast<uint64_t>(addr);
    tok.rkey   = it->second->rkey;
    tok.length = static_cast<uint32_t>(len);
    tok.offset = offset;
    return hexEncode(&tok, sizeof(tok));
}