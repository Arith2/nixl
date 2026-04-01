/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "engine_impl.h"
#include "engine_utils.h"
#include "s3/client.h"
#include "common/nixl_log.h"
#include <absl/strings/str_format.h>
#include <algorithm>
#include <chrono>
#include <future>
#include <thread>
#include <optional>
#include <vector>
#include "rdma_ctx.h"

// Extract hostname from endpoint URL, e.g. "http://10.93.244.74:8000" -> "10.93.244.74"
static std::string
extractHost(const std::string& endpoint) {
    size_t start = endpoint.find("://");
    if (start == std::string::npos) return endpoint;
    start += 3;
    size_t end = endpoint.find_first_of(":/", start);
    if (end == std::string::npos) return endpoint.substr(start);
    return endpoint.substr(start, end - start);
}

namespace {

bool
isValidPrepXferParams(const nixl_xfer_op_t &operation,
                      const nixl_meta_dlist_t &local,
                      const nixl_meta_dlist_t &remote,
                      const std::string &remote_agent,
                      const std::string &local_agent) {
    if (operation != NIXL_WRITE && operation != NIXL_READ) {
        NIXL_ERROR << absl::StrFormat("Error: Invalid operation type: %d", operation);
        return false;
    }

    if (remote_agent != local_agent)
        NIXL_WARN << absl::StrFormat(
            "Warning: Remote agent doesn't match the requesting agent (%s). Got %s",
            local_agent,
            remote_agent);

    if (local.getType() != DRAM_SEG && local.getType() != VRAM_SEG) {
        NIXL_ERROR << absl::StrFormat("Error: Local memory type must be DRAM_SEG or VRAM_SEG, got %d",
                                      local.getType());
        return false;
    }

    if (remote.getType() != OBJ_SEG) {
        NIXL_ERROR << absl::StrFormat("Error: Remote memory type must be OBJ_SEG, got %d",
                                      remote.getType());
        return false;
    }

    return true;
}

class nixlObjBackendReqH : public nixlBackendReqH {
public:
    nixlObjBackendReqH() = default;
    ~nixlObjBackendReqH() = default;

    std::vector<std::future<nixl_status_t>> statusFutures_;

    nixl_status_t
    getOverallStatus() {
        // Iterate front-to-back to detect failures in earlier futures even if
        // later futures are not yet ready. This ensures we return errors as
        // soon as they occur rather than waiting for all futures to complete.
        auto it = statusFutures_.begin();
        while (it != statusFutures_.end()) {
            if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                auto current_status = it->get();
                if (current_status != NIXL_SUCCESS) {
                    statusFutures_.clear();
                    return current_status;
                }
                it = statusFutures_.erase(it);
            } else {
                return NIXL_IN_PROG;
            }
        }
        return NIXL_SUCCESS;
    }
};

class nixlObjMetadata : public nixlBackendMD {
public:
    nixlObjMetadata(nixl_mem_t nixl_mem, uint64_t dev_id, std::string obj_key)
        : nixlBackendMD(true),
          nixlMem(nixl_mem),
          devId(dev_id),
          objKey(obj_key) {}

    ~nixlObjMetadata() = default;

    nixl_mem_t nixlMem;
    uint64_t devId;
    std::string objKey;
};

} // namespace

DefaultObjEngineImpl::DefaultObjEngineImpl(const nixlBackendInitParams *init_params)
    : executor_(std::make_shared<asioThreadPoolExecutor>(getNumThreads(init_params->customParams))),
      crtMinLimit_(getCrtMinLimit(init_params->customParams)) {
    s3Client_ = std::make_shared<awsS3Client>(init_params->customParams, executor_);
    NIXL_INFO << "Object storage backend initialized with S3 Standard client only";

    // Initialize RDMA context if rdma_port param is provided
    if (init_params->customParams) {
        auto ep_it   = init_params->customParams->find("endpoint_override");
        auto port_it = init_params->customParams->find("rdma_port");
        if (ep_it != init_params->customParams->end() &&
            port_it != init_params->customParams->end()) {
            int rdma_port = std::stoi(port_it->second);
            std::string host = extractHost(ep_it->second);
            rdma_ctx_ = std::make_unique<RdmaContext>(host, rdma_port);
            if (!rdma_ctx_->isEnabled()) {
                rdma_ctx_.reset();  // disable if connection failed
            } else {
                // RGW RDMA server allocates and pins a 256 MiB pre-buffer after
                // accepting the connection (MAP_POPULATE + ibv_reg_mr).  This takes
                // ~200–500 ms.  Sleep here so the first PUT does not arrive before
                // ready_ is set on the server side, avoiding HTTP 500 "server not ready".
                NIXL_INFO << "RdmaContext: connected; waiting 2s for RGW RDMA server to be ready";
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }
    }

    // Parse kvcache mode parameters
    if (init_params->customParams) {
        auto mode_it = init_params->customParams->find("kvcache_mode");
        if (mode_it != init_params->customParams->end() && mode_it->second == "1") {
            kvcache_mode_ = true;
            auto layers_it = init_params->customParams->find("kvcache_num_layers");
            auto kv_it = init_params->customParams->find("kvcache_kv_per_token");
            if (layers_it != init_params->customParams->end())
                kvcache_num_layers_ = std::stoi(layers_it->second);
            if (kv_it != init_params->customParams->end())
                kvcache_kv_per_token_ = std::stoi(kv_it->second);
            auto agg_it = init_params->customParams->find("kvcache_layer_aggregate");
            if (agg_it != init_params->customParams->end())
                kvcache_layer_aggregate_ = std::stoi(agg_it->second);
            NIXL_INFO << "KVCache mode enabled: layers=" << kvcache_num_layers_
                      << " kv_per_token=" << kvcache_kv_per_token_;
        }
    }

    // Ensure at least one client was created
    if (!s3Client_) {
        throw std::runtime_error("Failed to create any S3 client");
    }
}

DefaultObjEngineImpl::DefaultObjEngineImpl(const nixlBackendInitParams *init_params,
                                           std::shared_ptr<iS3Client> s3_client,
                                           std::shared_ptr<iS3Client> s3_client_crt)
    : executor_(std::make_shared<asioThreadPoolExecutor>(std::thread::hardware_concurrency())),
      s3Client_(s3_client),
      crtMinLimit_(getCrtMinLimit(init_params->customParams)) {
    // DefaultObjEngineImpl only uses the standard S3 client, not the CRT client.
    // The s3_client_crt parameter is accepted for API consistency with derived
    // engine implementations (e.g., S3CrtObjEngineImpl) but is intentionally unused here.
    (void)s3_client_crt;
    if (s3Client_) s3Client_->setExecutor(executor_);
    NIXL_INFO << "Object storage backend initialized with injected S3 clients";
}

DefaultObjEngineImpl::~DefaultObjEngineImpl() {
    executor_->WaitUntilStopped();
}

nixl_status_t
DefaultObjEngineImpl::registerMem(const nixlBlobDesc &mem,
                                  const nixl_mem_t &nixl_mem,
                                  nixlBackendMD *&out) {
    nixl_mem_list_t supported_mems = {OBJ_SEG, DRAM_SEG, VRAM_SEG};
    if (std::find(supported_mems.begin(), supported_mems.end(), nixl_mem) == supported_mems.end())
        return NIXL_ERR_NOT_SUPPORTED;

    if (nixl_mem == OBJ_SEG) {
        std::unique_ptr<nixlObjMetadata> obj_md = std::make_unique<nixlObjMetadata>(
            nixl_mem, mem.devId, mem.metaInfo.empty() ? std::to_string(mem.devId) : mem.metaInfo);
        devIdToObjKey_[mem.devId] = obj_md->objKey;
        out = obj_md.release();
    } else {
        // DRAM_SEG or VRAM_SEG: register with RDMA NIC.
        // For VRAM_SEG, ibv_reg_mr works transparently via nvidia_peermem kernel module.
        auto dram_md = std::make_unique<nixlDramBEMD>(mem.addr, mem.len);
        if (rdma_ctx_ && rdma_ctx_->isEnabled()) {
            rdma_ctx_->registerMR(reinterpret_cast<void*>(mem.addr), mem.len);
            // In kvcache mode, store the registered buffer info so postXfer
            // can build a token covering the full registered region.
            if (kvcache_mode_) {
                kvcache_buf_addr_ = mem.addr;
                kvcache_buf_len_ = mem.len;
            }
        }
        out = dram_md.release();
    }

    return NIXL_SUCCESS;
}

nixl_status_t
DefaultObjEngineImpl::deregisterMem(nixlBackendMD *meta) {
    if (!meta) return NIXL_SUCCESS;

    if (auto* obj_md = dynamic_cast<nixlObjMetadata*>(meta)) {
        devIdToObjKey_.erase(obj_md->devId);
        delete obj_md;
    } else if (auto* dram_md = dynamic_cast<nixlDramBEMD*>(meta)) {
        if (rdma_ctx_)
            rdma_ctx_->deregisterMR(reinterpret_cast<void*>(dram_md->addr));
        delete dram_md;
    }

    return NIXL_SUCCESS;
}

nixl_status_t
DefaultObjEngineImpl::queryMem(const nixl_reg_dlist_t &descs,
                               std::vector<nixl_query_resp_t> &resp) const {
    resp.reserve(descs.descCount());

    iS3Client *client = getClient();
    if (!client) {
        NIXL_ERROR << "Failed to query memory: no client available";
        return NIXL_ERR_BACKEND;
    }

    try {
        for (auto &desc : descs)
            resp.emplace_back(client->checkObjectExists(desc.metaInfo) ?
                                  nixl_query_resp_t{nixl_b_params_t{}} :
                                  std::nullopt);
    }
    catch (const std::runtime_error &e) {
        NIXL_ERROR << "Failed to query memory: " << e.what();
        return NIXL_ERR_BACKEND;
    }

    return NIXL_SUCCESS;
}

nixl_status_t
DefaultObjEngineImpl::prepXfer(const nixl_xfer_op_t &operation,
                               const nixl_meta_dlist_t &local,
                               const nixl_meta_dlist_t &remote,
                               const std::string &remote_agent,
                               const std::string &local_agent,
                               nixlBackendReqH *&handle,
                               const nixl_opt_b_args_t *opt_args) const {
    if (!isValidPrepXferParams(operation, local, remote, remote_agent, local_agent))
        return NIXL_ERR_INVALID_PARAM;

    auto req_h = std::make_unique<nixlObjBackendReqH>();
    handle = req_h.release();
    return NIXL_SUCCESS;
}

nixl_status_t
DefaultObjEngineImpl::postXfer(const nixl_xfer_op_t &operation,
                               const nixl_meta_dlist_t &local,
                               const nixl_meta_dlist_t &remote,
                               const std::string &remote_agent,
                               nixlBackendReqH *&handle,
                               const nixl_opt_b_args_t *opt_args) const {
    nixlObjBackendReqH *req_h = static_cast<nixlObjBackendReqH *>(handle);

    // KV cache streaming mode: collect ALL registered chunk keys and make a single
    // getKVCacheAsync call. The caller sends a normal 1-local + 1-remote request;
    // we expand it to include all registered objects.
    if (kvcache_mode_ && operation == NIXL_READ && local.descCount() > 0) {
        size_t chunk_size = local[0].len;
        size_t tokens_per_chunk = chunk_size / (kvcache_num_layers_ * kvcache_kv_per_token_);

        // Collect ALL registered object keys (all prepop chunks)
        std::vector<std::string> chunk_keys;
        for (const auto& [devId, key] : devIdToObjKey_) {
            chunk_keys.push_back(key);
        }
        std::sort(chunk_keys.begin(), chunk_keys.end());  // deterministic order

        if (chunk_keys.empty()) {
            NIXL_ERROR << "KVCache: no chunk keys registered";
            return NIXL_ERR_INVALID_PARAM;
        }

        // Use registered buffer for RDMA token. Cap to actual MR size.
        uintptr_t data_ptr = kvcache_buf_addr_ ? kvcache_buf_addr_ : local[0].addr;
        size_t needed = chunk_keys.size() * chunk_size;
        size_t data_len = kvcache_buf_len_ ? std::min(needed, kvcache_buf_len_) : needed;

        iS3Client *client = getClient();
        if (!client) return NIXL_ERR_BACKEND;

        auto status_promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->statusFutures_.push_back(status_promise->get_future());

        auto status_callback = [status_promise](bool success) {
            status_promise->set_value(success ? NIXL_SUCCESS : NIXL_ERR_BACKEND);
        };

        std::optional<std::string> rdma_token;
        if (rdma_ctx_ && rdma_ctx_->isEnabled()) {
            auto token = rdma_ctx_->buildToken(
                reinterpret_cast<void*>(data_ptr), data_len, 0);
            if (!token.empty())
                rdma_token = token;
        }

        int layer_agg = kvcache_layer_aggregate_ > 0 ? kvcache_layer_aggregate_ : kvcache_num_layers_;

        NIXL_INFO << "KVCache: streaming " << chunk_keys.size() << " chunks"
                  << " layers=" << kvcache_num_layers_ << " agg=" << layer_agg
                  << " data_len=" << data_len;

        client->getKVCacheAsync(
            chunk_keys, kvcache_num_layers_, kvcache_kv_per_token_,
            tokens_per_chunk, layer_agg,
            data_ptr, data_len, status_callback, rdma_token);

        return NIXL_IN_PROG;
    }

    // Normal (non-kvcache) path
    for (int i = 0; i < local.descCount(); ++i) {
        const auto &local_desc = local[i];
        const auto &remote_desc = remote[i];

        auto obj_key_search = devIdToObjKey_.find(remote_desc.devId);
        if (obj_key_search == devIdToObjKey_.end()) {
            NIXL_ERROR << "The object segment key " << remote_desc.devId
                       << " is not registered with the backend";
            return NIXL_ERR_INVALID_PARAM;
        }

        auto status_promise = std::make_shared<std::promise<nixl_status_t>>();
        req_h->statusFutures_.push_back(status_promise->get_future());

        uintptr_t data_ptr = local_desc.addr;
        size_t data_len = local_desc.len;
        size_t offset = remote_desc.addr;

        iS3Client *client = getClientForSize(data_len);
        if (!client) {
            NIXL_ERROR << "Failed to post transfer: no client available";
            return NIXL_ERR_BACKEND;
        }

        auto status_callback = [status_promise](bool success) {
            status_promise->set_value(success ? NIXL_SUCCESS : NIXL_ERR_BACKEND);
        };

        std::optional<std::string> rdma_token;
        if (rdma_ctx_ && rdma_ctx_->isEnabled()) {
            auto token = rdma_ctx_->buildToken(
                reinterpret_cast<void*>(data_ptr), data_len, offset);
            if (!token.empty())
                rdma_token = token;
        }

        if (operation == NIXL_WRITE)
            client->putObjectAsync(
                obj_key_search->second, data_ptr, data_len, offset,
                status_callback, rdma_token);
        else
            client->getObjectAsync(
                obj_key_search->second, data_ptr, data_len, offset,
                status_callback, rdma_token);
    }

    return NIXL_IN_PROG;
}

nixl_status_t
DefaultObjEngineImpl::checkXfer(nixlBackendReqH *handle) const {
    nixlObjBackendReqH *req_h = static_cast<nixlObjBackendReqH *>(handle);
    return req_h->getOverallStatus();
}

nixl_status_t
DefaultObjEngineImpl::releaseReqH(nixlBackendReqH *handle) const {
    nixlObjBackendReqH *req_h = static_cast<nixlObjBackendReqH *>(handle);
    delete req_h;
    return NIXL_SUCCESS;
}

iS3Client *
DefaultObjEngineImpl::getClient() const {
    return s3Client_.get();
}

iS3Client *
DefaultObjEngineImpl::getClientForSize(size_t data_len) const {
    (void)data_len;
    return getClient();
}
