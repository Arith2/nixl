/*
 * SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "worker/nixl/nixl_worker.h"
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if HAVE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#endif
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include "utils/neuron.h"
#include "utils/utils.h"
#include <unistd.h>
#include <utility>
#include <mutex>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <utils/serdes/serdes.h>
#include <omp.h>

#define ROUND_UP(value, granularity) \
    ((((value) + (granularity) - 1) / (granularity)) * (granularity))

#define CHECK_NIXL_ERROR(result, message)                                                       \
    do {                                                                                        \
        if (0 != result) {                                                                      \
            std::cerr << "NIXL: " << message << " (Error code: " << result << ")" << std::endl; \
            exit(EXIT_FAILURE);                                                                 \
        }                                                                                       \
    } while (0)

#if HAVE_CUDA
#define HANDLE_VRAM_SEGMENT(_seg_type) _seg_type = VRAM_SEG;
#else
#define HANDLE_VRAM_SEGMENT(_seg_type)                                        \
    std::cerr << "VRAM segment type not supported without CUDA" << std::endl; \
    std::exit(EXIT_FAILURE);
#endif

#define GET_SEG_TYPE(is_initiator)                                                          \
    ({                                                                                      \
        std::string _seg_type_str = ((is_initiator) ? xferBenchConfig::initiator_seg_type : \
                                                      xferBenchConfig::target_seg_type);    \
        nixl_mem_t _seg_type;                                                               \
        if (0 == _seg_type_str.compare("DRAM")) {                                           \
            _seg_type = DRAM_SEG;                                                           \
        } else if (0 == _seg_type_str.compare("VRAM")) {                                    \
            HANDLE_VRAM_SEGMENT(_seg_type);                                                 \
        } else {                                                                            \
            std::cerr << "Invalid segment type: " << _seg_type_str << std::endl;            \
            exit(EXIT_FAILURE);                                                             \
        }                                                                                   \
        _seg_type;                                                                          \
    })

// Reuse parser from utils

// Generate GUSLI config file from device configurations
static std::string
generateGusliConfigFile(const std::vector<GusliDeviceConfig> &devices) {
    std::stringstream config;
    config << "# Config file\nversion=1\n";

    for (const auto &dev : devices) {
        // Format: "id type access_mode direct_io path security_flags"
        // Example: "11 F W D ./store0.bin sec=0x3"
        config << dev.device_id << " " << dev.device_type << " "
               << "W D " // Write mode, Direct I/O
               << dev.device_path << " " << dev.security_flags << "\n";
    }

    std::cout << "GUSLI Device Config: " << config.str() << std::endl;

    return config.str();
}

xferBenchNixlWorker::xferBenchNixlWorker(int *argc, char ***argv, std::vector<std::string> devices)
    : xferBenchWorker(argc, argv) {
    seg_type = GET_SEG_TYPE(isInitiator());

    int rank;
    std::string backend_name;
    nixl_b_params_t backend_params;
    bool enable_pt = xferBenchConfig::enable_pt;
    nixl_thread_sync_t sync_mode = xferBenchConfig::num_threads > 1 ?
        nixl_thread_sync_t::NIXL_THREAD_SYNC_RW :
        nixl_thread_sync_t::NIXL_THREAD_SYNC_DEFAULT;
    char hostname[256];
    nixl_mem_list_t mems;
    std::vector<nixl_backend_t> plugins;

    rank = rt->getRank();

    nixlAgentConfig dev_meta;
    dev_meta.useProgThread = enable_pt;
    dev_meta.syncMode = sync_mode;

    agent = new nixlAgent(name, dev_meta);

    agent->getAvailPlugins(plugins);

    if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_UCX) ||
        0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_LIBFABRIC) ||
        0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_GPUNETIO) ||
        0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_MOONCAKE) ||
        0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_UCCL) ||
        xferBenchConfig::isStorageBackend()) {
        backend_name = xferBenchConfig::backend;
    } else {
        std::cerr << "Unsupported NIXLBench backend: " << xferBenchConfig::backend << std::endl;
        exit(EXIT_FAILURE);
    }

    agent->getPluginParams(backend_name, mems, backend_params);

    if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_UCX)) {
        backend_params["num_threads"] = std::to_string(xferBenchConfig::progress_threads);

        // No need to set device_list if all is specified
        // fallback to backend preference
        if (devices[0] != "all" && devices.size() >= 1) {
            if (isInitiator()) {
                backend_params["device_list"] = devices[rank];
            } else {
                backend_params["device_list"] = devices[rank - xferBenchConfig::num_initiator_dev];
            }
        }

        if (gethostname(hostname, 256)) {
            std::cerr << "Failed to get hostname" << std::endl;
            exit(EXIT_FAILURE);
        }

        backend_params["num_workers"] = std::to_string(xferBenchConfig::num_threads + 1);

        std::cout << "Init nixl worker, dev "
                  << (("all" == devices[0]) ? "all" : backend_params["device_list"]) << " rank "
                  << rank << ", type " << name << ", hostname " << hostname << std::endl;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_LIBFABRIC)) {
        if (gethostname(hostname, 256)) {
            std::cerr << "Failed to get hostname" << std::endl;
            exit(EXIT_FAILURE);
        }

        // We need to make sure the Neuron runtime is initialized before initializing libfabric,
        // otherwise the FI_HMEM_NEURON backend will not be created. This issue has been fixed
        // upstream: https://github.com/ofiwg/libfabric/pull/11804
        int nc_count = neuronCoreCount();

        std::cout << "Init nixl worker, dev " << (("all" == devices[0]) ? "all" : devices[rank])
                  << " rank " << rank << ", type " << name << ", hostname " << hostname
                  << ", nc_count " << nc_count << std::endl;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_GDS)) {
        // Using default param values for GDS backend
        std::cout << "GDS backend" << std::endl;
        backend_params["batch_pool_size"] = std::to_string(xferBenchConfig::gds_batch_pool_size);
        backend_params["batch_limit"] = std::to_string(xferBenchConfig::gds_batch_limit);
        std::cout << "GDS batch pool size: " << xferBenchConfig::gds_batch_pool_size << std::endl;
        std::cout << "GDS batch limit: " << xferBenchConfig::gds_batch_limit << std::endl;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_GDS_MT)) {
        std::cout << "GDS_MT backend" << std::endl;
        backend_params["thread_count"] = std::to_string(xferBenchConfig::gds_mt_num_threads);
        std::cout << "GDS MT Num threads: " << xferBenchConfig::gds_mt_num_threads << std::endl;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_POSIX)) {
        // Set API type parameter for POSIX backend
        if (xferBenchConfig::posix_api_type == XFERBENCH_POSIX_API_AIO) {
            backend_params["use_aio"] = "true";
            backend_params["use_uring"] = "false";
            backend_params["use_posix_aio"] = "false";
        } else if (xferBenchConfig::posix_api_type == XFERBENCH_POSIX_API_URING) {
            backend_params["use_aio"] = "false";
            backend_params["use_uring"] = "true";
            backend_params["use_posix_aio"] = "false";
        } else if (xferBenchConfig::posix_api_type == XFERBENCH_POSIX_API_POSIXAIO) {
            backend_params["use_aio"] = "false";
            backend_params["use_uring"] = "false";
            backend_params["use_posix_aio"] = "true";
        }
        std::cout << "POSIX backend with API type: " << xferBenchConfig::posix_api_type
                  << std::endl;
        backend_params["ios_pool_size"] = std::to_string(xferBenchConfig::posix_ios_pool_size);
        backend_params["kernel_queue_size"] =
            std::to_string(xferBenchConfig::posix_kernel_queue_size);
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_GPUNETIO)) {
        std::cout << "GPUNETIO backend, network device " << devices[0] << " GPU device "
                  << xferBenchConfig::gpunetio_device_list << " OOB interface "
                  << xferBenchConfig::gpunetio_oob_list << std::endl;
        backend_params["network_devices"] = devices[0];
        backend_params["gpu_devices"] = xferBenchConfig::gpunetio_device_list;
        backend_params["oob_interface"] = xferBenchConfig::gpunetio_oob_list;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_MOONCAKE)) {
        std::cout << "Mooncake backend" << std::endl;
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_HF3FS)) {
        // Using default param values for HF3FS backend
        std::cout << "HF3FS backend iopool_size " << xferBenchConfig::hf3fs_iopool_size
                  << std::endl;
        backend_params["iopool_size"] = std::to_string(xferBenchConfig::hf3fs_iopool_size);
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_OBJ)) {
        // Using default param values for OBJ backend
        backend_params["access_key"] = xferBenchConfig::obj_access_key;
        backend_params["secret_key"] = xferBenchConfig::obj_secret_key;
        backend_params["session_token"] = xferBenchConfig::obj_session_token;
        backend_params["bucket"] = xferBenchConfig::obj_bucket_name;
        backend_params["scheme"] = xferBenchConfig::obj_scheme;
        backend_params["region"] = xferBenchConfig::obj_region;
        backend_params["use_virtual_addressing"] =
            xferBenchConfig::obj_use_virtual_addressing ? "true" : "false";
        backend_params["req_checksum"] = xferBenchConfig::obj_req_checksum;

        if (xferBenchConfig::obj_ca_bundle != "") {
            backend_params["ca_bundle"] = xferBenchConfig::obj_ca_bundle;
        }

        if (xferBenchConfig::obj_endpoint_override != "") {
            backend_params["endpoint_override"] = xferBenchConfig::obj_endpoint_override;
        }

        if (xferBenchConfig::obj_rdma_port != "") {
            backend_params["rdma_port"] = xferBenchConfig::obj_rdma_port;
        }

        if (xferBenchConfig::batch_mode) {
            backend_params["batch_mode"] = "1";
            backend_params["batch_size"] = std::to_string(xferBenchConfig::batch_size);
            backend_params["server_aggregate_size"] =
                std::to_string(xferBenchConfig::server_aggregate_size);
            if (xferBenchConfig::obj_prepop_start > 0)
                backend_params["batch_window_start"] = std::to_string(xferBenchConfig::obj_prepop_start);
            // -num_threads_batch sizes the engine's executor pool (fan-out
            // parallelism for the N libdfs reads inside one postXfer).
            // -num_threads keeps its original benchmark-worker semantic;
            // for batch_mode the script is expected to set it to 1 so each
            // iteration corresponds to exactly one HTTP control hop.
            backend_params["num_threads"] =
                std::to_string(xferBenchConfig::num_threads_batch);
            backend_params["num_threads_batch"] =
                std::to_string(xferBenchConfig::num_threads_batch);
            backend_params["num_threads_daos"] =
                std::to_string(xferBenchConfig::num_threads_batch);
            // -iodepth_batch makes s3rdma_batch mirror daos_direct's
            // NT × IOD shape inside the engine: num_threads_batch workers
            // with iodepth_batch async DAOS fetches per worker. Older scripts
            // can still use -batch_inflight_cap directly.
            if (xferBenchConfig::iodepth_batch > 0) {
                backend_params["iodepth_batch"] =
                    std::to_string(xferBenchConfig::iodepth_batch);
                backend_params["iodepth_daos"] =
                    std::to_string(xferBenchConfig::iodepth_batch);
                backend_params["batch_inflight_cap"] = std::to_string(
                    xferBenchConfig::num_threads_batch *
                    xferBenchConfig::iodepth_batch);
            } else {
                backend_params["batch_inflight_cap"] =
                    std::to_string(xferBenchConfig::batch_inflight_cap);
            }
        }

        if (xferBenchConfig::obj_daos_direct) {
            // DAOS direct engine — bypass Ceph on the data plane
            backend_params["daos_direct"] = "true";
            backend_params["daos_pool"] = xferBenchConfig::obj_daos_pool;
            backend_params["daos_cont"] = xferBenchConfig::obj_daos_cont;
            if (xferBenchConfig::obj_daos_direct_hashoid) {
                // Hashoid mode: skip dfs_open, synthesize OID client-side.
                // (T, IOD) feed oid.hi encoding in hashoid_oid_hi_user().
                backend_params["daos_hashoid"]  = "true";
                backend_params["hashoid_T"]     = std::to_string(xferBenchConfig::num_threads);
                backend_params["hashoid_IOD"]   = std::to_string(xferBenchConfig::iodepth);
            }
            std::cout << "OBJ backend with DAOS direct DFS engine enabled"
                      << " (pool=" << xferBenchConfig::obj_daos_pool
                      << " cont=" << xferBenchConfig::obj_daos_cont
                      << (xferBenchConfig::obj_daos_direct_hashoid ? " [HASHOID]" : "")
                      << ")" << std::endl;
        } else if (xferBenchConfig::obj_mode == "s3rdma_direct") {
            // Split-plane (cuObject-style): NIXL → Ceph RGW (control over HTTP
            // with x-amz-rdma-direct header) → on success → NIXL → DAOS
            // server via libdfs (data plane). Needs both the S3 params (for
            // the control hop) and the DAOS params (for the data plane).
            backend_params["mode"] = "s3rdma_direct";
            backend_params["daos_pool"] = xferBenchConfig::obj_daos_pool;
            backend_params["daos_cont"] = xferBenchConfig::obj_daos_cont;
            if (xferBenchConfig::obj_daos_direct_hashoid) {
                // Applies to both s3rdma_direct and s3rdma_batch (batch_mode
                // is parsed independently on the same engine).
                backend_params["daos_hashoid"]  = "true";
                backend_params["hashoid_T"]     = std::to_string(xferBenchConfig::num_threads);
                backend_params["hashoid_IOD"]   = std::to_string(xferBenchConfig::iodepth);
            }
            if (xferBenchConfig::obj_daos_agg) {
                // s3rdma_agg: server-side aggregation. With the new agg
                // postXfer flow ("1 HTTP per load + N per-layer fetches"),
                // -batch_size = OBJECTS_PER_LOAD and the chunks-per-layer
                // fan-out is controlled by -obj_agg_chunks_per_layer.
                // Falls back to batch_size if the new flag is unset (legacy).
                backend_params["daos_agg"] = "true";
                int acpl = xferBenchConfig::obj_agg_chunks_per_layer > 0
                              ? xferBenchConfig::obj_agg_chunks_per_layer
                              : xferBenchConfig::batch_size;
                backend_params["agg_chunks_per_layer"] = std::to_string(acpl);
                // chunks_per_layer = total chunks in a layer (= ISL/T).
                // Distinct from agg_chunks_per_layer (the agg fetch unit) —
                // when 4 MiB aggs are used inside 16 MiB layers, multiple
                // groups share the same layer_idx and need different recx
                // offsets. Falls back to agg_chunks_per_layer for the
                // legacy whole-layer case.
                int cpl = xferBenchConfig::obj_chunks_per_layer > 0
                              ? xferBenchConfig::obj_chunks_per_layer
                              : acpl;
                backend_params["chunks_per_layer"] = std::to_string(cpl);
            }
            if (xferBenchConfig::obj_daos_agg_patch) {
                // S3RDMA agg-patch: use the same outer S3RDMA-batch
                // dispatch, but call daos_obj_fetch_agg for each logical
                // object so DAOS stitches fine-grained chunks server-side.
                backend_params["daos_agg_patch"] = "true";
                if (xferBenchConfig::obj_daos_agg_patch_lwagg_manifest)
                    backend_params["daos_agg_patch_lwagg_manifest"] = "true";
                if (xferBenchConfig::obj_daos_agg_patch_rangeget)
                    backend_params["daos_agg_patch_rangeget"] = "true";
                int acpl = xferBenchConfig::obj_agg_chunks_per_layer > 0
                              ? xferBenchConfig::obj_agg_chunks_per_layer
                              : xferBenchConfig::batch_size;
                backend_params["agg_chunks_per_layer"] = std::to_string(acpl);
                int cpl = xferBenchConfig::obj_chunks_per_layer > 0
                              ? xferBenchConfig::obj_chunks_per_layer
                              : acpl;
                backend_params["chunks_per_layer"] = std::to_string(cpl);
            }
            if (xferBenchConfig::obj_daos_lwagg_server_mode) {
                backend_params["daos_lwagg_server_mode"] = "true";
                backend_params["lwagg_issue_mode"] =
                    xferBenchConfig::obj_lwagg_issue_mode;
                backend_params["lwagg_manifest_tsv"] =
                    xferBenchConfig::obj_lwagg_manifest_tsv;
                backend_params["lwagg_num_layers"] =
                    std::to_string(xferBenchConfig::obj_lwagg_num_layers);
                if (xferBenchConfig::obj_lwagg_layer_bytes > 0) {
                    backend_params["lwagg_layer_bytes"] =
                        std::to_string(xferBenchConfig::obj_lwagg_layer_bytes);
                }
                if (xferBenchConfig::obj_lwagg_object_bytes > 0) {
                    backend_params["lwagg_object_bytes"] =
                        std::to_string(xferBenchConfig::obj_lwagg_object_bytes);
                }
                if (xferBenchConfig::obj_lwagg_manifest_start > 0) {
                    backend_params["lwagg_manifest_start"] =
                        std::to_string(xferBenchConfig::obj_lwagg_manifest_start);
                }
            }
            std::cout << "OBJ backend with S3 split-plane (s3rdma_direct) enabled"
                      << " (pool=" << xferBenchConfig::obj_daos_pool
                      << " cont=" << xferBenchConfig::obj_daos_cont
                      << (xferBenchConfig::obj_daos_direct_hashoid ? " [HASHOID]" : "")
                      << (xferBenchConfig::batch_mode ? " [BATCH]" : "")
                      << (xferBenchConfig::obj_daos_agg ? " [AGG]" : "")
                      << (xferBenchConfig::obj_daos_agg_patch ? " [AGG_PATCH]" : "")
                      << (xferBenchConfig::obj_daos_lwagg_server_mode ? " [LWAGG]" : "")
                      << ")" << std::endl;
        } else if (xferBenchConfig::obj_crt_min_limit > 0) {
            // Warn if both CRT and accelerated options are set - CRT takes precedence
            if (xferBenchConfig::obj_accelerated_enable) {
                std::cerr << "Warning: Both obj_crt_min_limit and obj_accelerated_enable are set. "
                          << "CRT client will be used (takes precedence over accelerated)."
                          << std::endl;
            }
            backend_params["crtMinLimit"] = std::to_string(xferBenchConfig::obj_crt_min_limit);
            std::cout << "OBJ backend with S3 CRT client enabled for objects >= "
                      << xferBenchConfig::obj_crt_min_limit << " bytes" << std::endl;
        } else if (xferBenchConfig::obj_accelerated_enable) {
            backend_params["accelerated"] = "true";
            std::cout << "OBJ backend with S3 Accelerated client enabled";
            if (!xferBenchConfig::obj_accelerated_type.empty()) {
                backend_params["type"] = xferBenchConfig::obj_accelerated_type;
                std::cout << " (type: " << xferBenchConfig::obj_accelerated_type << ")";
            }
            std::cout << std::endl;
        } else {
            std::cout << "OBJ backend with standard S3 enabled" << std::endl;
        }
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_GUSLI)) {
        // GUSLI backend requires direct I/O - enable it automatically
        if (!xferBenchConfig::storage_enable_direct) {
            std::cout
                << "GUSLI backend: Automatically enabling storage_enable_direct for direct I/O"
                << std::endl;
            xferBenchConfig::storage_enable_direct = true;
        }

        // Parse and configure GUSLI devices from general device_list parameter
        int expected_num_devices =
            isInitiator() ? xferBenchConfig::num_initiator_dev : xferBenchConfig::num_target_dev;
        gusli_devices = parseGusliDeviceList(xferBenchConfig::device_list,
                                             xferBenchConfig::gusli_device_security,
                                             xferBenchConfig::gusli_device_byte_offsets,
                                             expected_num_devices);

        // Set GUSLI backend parameters
        backend_params["client_name"] = xferBenchConfig::gusli_client_name;
        backend_params["max_num_simultaneous_requests"] =
            std::to_string(xferBenchConfig::gusli_max_simultaneous_requests);

        // Generate config file if not explicitly provided
        if (xferBenchConfig::gusli_config_file.empty()) {
            backend_params["config_file"] = generateGusliConfigFile(gusli_devices);
        } else {
            backend_params["config_file"] = xferBenchConfig::gusli_config_file;
        }

        std::cout << "GUSLI backend initialized:" << std::endl;
        std::cout << "  Client name: " << xferBenchConfig::gusli_client_name << std::endl;
        std::cout << "  Max simultaneous requests: "
                  << xferBenchConfig::gusli_max_simultaneous_requests << std::endl;
        std::cout << "  Direct I/O: Enabled (required)" << std::endl;
        std::cout << "  Configured devices: " << gusli_devices.size() << std::endl;
        for (const auto &dev : gusli_devices) {
            std::cout << "    Device " << dev.device_id << " [" << dev.device_type
                      << "]: " << dev.device_path << " (" << dev.security_flags << ")"
                      << ", offset = " << dev.dev_offset << std::endl;
        }
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_UCCL)) {
        std::cout << "UCCL backend" << std::endl;
        backend_params["in_python"] = "0";
    } else if (0 == xferBenchConfig::backend.compare(XFERBENCH_BACKEND_AZURE_BLOB)) {
        // Using default param values for AZURE_BLOB backend
        backend_params["account_url"] = xferBenchConfig::azure_blob_account_url;
        backend_params["container_name"] = xferBenchConfig::azure_blob_container_name;
        backend_params["connection_string"] = xferBenchConfig::azure_blob_connection_string;
        std::cout << "AZURE_BLOB backend" << std::endl;
    } else {
        std::cerr << "Unsupported NIXLBench backend: " << xferBenchConfig::backend << std::endl;
        exit(EXIT_FAILURE);
    }

    CHECK_NIXL_ERROR(agent->createBackend(backend_name, backend_params, backend_engine),
                     "createBackend failed!");
}

xferBenchNixlWorker::~xferBenchNixlWorker() {
    delete rt;
    rt = nullptr;

    if (agent) {
        delete agent;
        agent = nullptr;
    }
}

// Convert vector of xferBenchIOV to nixl_reg_dlist_t
static void
iovListToNixlRegDlist(const std::vector<xferBenchIOV> &iov_list, nixl_reg_dlist_t &dlist) {
    nixlBlobDesc desc;
    for (const auto &iov : iov_list) {
        desc.addr = iov.addr;
        desc.len = iov.len;
        desc.devId = iov.devId;
        desc.metaInfo = iov.metaInfo;
        dlist.addDesc(desc);
    }
}

// Convert nixl_xfer_dlist_t to vector of xferBenchIOV
static std::vector<xferBenchIOV>
nixlXferDlistToIOVList(const nixl_xfer_dlist_t &dlist) {
    std::vector<xferBenchIOV> iov_list;
    for (const auto &desc : dlist) {
        iov_list.emplace_back(desc.addr, desc.len, desc.devId);
    }
    return iov_list;
}

// Convert vector of xferBenchIOV to nixl_xfer_dlist_t
static void
iovListToNixlXferDlist(const std::vector<xferBenchIOV> &iov_list, nixl_xfer_dlist_t &dlist) {
    nixlBasicDesc desc;
    for (const auto &iov : iov_list) {
        desc.addr = iov.addr;
        desc.len = iov.len;
        desc.devId = iov.devId;
        dlist.addDesc(desc);
    }
}

static bool
allocateXferMemory(size_t buffer_size, void **addr) {
    if (!addr) {
        std::cerr << "Invalid address" << std::endl;
        return false;
    }
    if (buffer_size == 0) {
        std::cerr << "Invalid buffer size" << std::endl;
        return false;
    }
    if (xferBenchConfig::page_size == 0) {
        std::cerr << "Error: Invalid page size returned by sysconf" << std::endl;
        return false;
    }

    int rc = posix_memalign(addr, xferBenchConfig::page_size, buffer_size);
    if (rc != 0 || !*addr) {
        std::cerr << "Failed to allocate " << buffer_size << " bytes of page-aligned DRAM memory"
                  << std::endl;
        return false;
    }
    memset(*addr, 0, buffer_size);
    return true;
}

std::optional<xferBenchIOV>
xferBenchNixlWorker::initBasicDescDram(size_t buffer_size, int mem_dev_id) {
    void *addr;

    if (!allocateXferMemory(buffer_size, &addr)) {
        std::cerr << "Failed to allocate " << buffer_size << " bytes of DRAM memory" << std::endl;
        return std::nullopt;
    }

    // TODO: Does device id need to be set for DRAM?
    return std::optional<xferBenchIOV>(std::in_place, (uintptr_t)addr, buffer_size, mem_dev_id);
}

#if HAVE_CUDA
static std::optional<xferBenchIOV>
getVramDescCuda(int devid, size_t buffer_size, uint8_t memset_value) {
    void *addr;
    CHECK_CUDA_ERROR(cudaMalloc(&addr, buffer_size), "Failed to allocate CUDA buffer");
    CHECK_CUDA_ERROR(cudaMemset(addr, memset_value, buffer_size), "Failed to set device memory");

    return std::optional<xferBenchIOV>(std::in_place, (uintptr_t)addr, buffer_size, devid);
}

static std::optional<xferBenchIOV>
getVramDescCudaVmm(int devid, size_t buffer_size, uint8_t memset_value) {
#if HAVE_CUDA_FABRIC
    CUdeviceptr addr = 0;
    CUmemAllocationProp prop = {};
    CUmemAccessDesc access = {};

    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    prop.allocFlags.gpuDirectRDMACapable = 1;
    prop.location.id = devid;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;

    // Get the allocation granularity
    size_t granularity = 0;
    CHECK_CUDA_DRIVER_ERROR(
        cuMemGetAllocationGranularity(&granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM),
        "Failed to get allocation granularity");
    std::cout << "Granularity: " << granularity << std::endl;

    size_t padded_size = ROUND_UP(buffer_size, granularity);
    CUmemGenericAllocationHandle handle;
    CHECK_CUDA_DRIVER_ERROR(cuMemCreate(&handle, padded_size, &prop, 0),
                            "Failed to create allocation");

    // Reserve the memory address
    CHECK_CUDA_DRIVER_ERROR(cuMemAddressReserve(&addr, padded_size, granularity, 0, 0),
                            "Failed to reserve address");

    // Map the memory
    CHECK_CUDA_DRIVER_ERROR(cuMemMap(addr, padded_size, 0, handle, 0), "Failed to map memory");

    std::cout << "Address: " << std::hex << std::showbase << addr << " Buffer size: " << std::dec
              << buffer_size << " Padded size: " << std::dec << padded_size << std::endl;

    // Set the memory access rights
    access.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    access.location.id = devid;
    access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    CHECK_CUDA_DRIVER_ERROR(cuMemSetAccess(addr, buffer_size, &access, 1), "Failed to set access");

    // Set memory content based on role
    CHECK_CUDA_DRIVER_ERROR(cuMemsetD8(addr, memset_value, buffer_size),
                            "Failed to set VMM device memory");

    return std::optional<xferBenchIOV>(
        std::in_place, (uintptr_t)addr, buffer_size, devid, padded_size, handle);

#else
    std::cerr << "CUDA_FABRIC is not supported" << std::endl;
    return std::nullopt;
#endif /* HAVE_CUDA_FABRIC */
}

static std::optional<xferBenchIOV>
getVramDescNeuron(int devid, size_t buffer_size, uint8_t memset_value) {
    void *addr;
    CHECK_NEURON_ERROR(neuronMalloc(&addr, buffer_size, devid), "Failed to allocate nrt tensor");
    CHECK_NEURON_ERROR(neuronMemset(addr, memset_value, buffer_size),
                       "Failed to set device memory");

    return std::optional<xferBenchIOV>(std::in_place, (uintptr_t)addr, buffer_size, devid);
}

static std::optional<xferBenchIOV>
getVramDesc(int devid, size_t buffer_size, bool isInit) {
    uint8_t memset_value =
        isInit ? XFERBENCH_INITIATOR_BUFFER_ELEMENT : XFERBENCH_TARGET_BUFFER_ELEMENT;

    // Assume no CUDA cores exist if Neuron cores are found.
    // There are no AWS instance types with both NVIDIA GPUs and Neuron accelerators.
    if (neuronCoreCount() > 0) {
        return getVramDescNeuron(devid, buffer_size, memset_value);
    }

    CHECK_CUDA_ERROR(cudaSetDevice(devid), "Failed to set device");
    if (xferBenchConfig::enable_vmm) {
        return getVramDescCudaVmm(devid, buffer_size, memset_value);
    } else {
        return getVramDescCuda(devid, buffer_size, memset_value);
    }
}

std::optional<xferBenchIOV>
xferBenchNixlWorker::initBasicDescVram(size_t buffer_size, int mem_dev_id) {
    if (IS_PAIRWISE_AND_SG()) {
        int devid = rt->getRank();

        if (isTarget()) {
            devid -= xferBenchConfig::num_initiator_dev;
        }

        if (devid != mem_dev_id) {
            return std::nullopt;
        }
    }

    return getVramDesc(mem_dev_id, buffer_size, isInitiator());
}
#endif /* HAVE_CUDA */

// Helper to open a single file with appropriate flags
static std::optional<xferFileState>
openFileWithFlags(const std::string &file_name, int flags) {
    uint64_t file_size = 0;
    if (XFERBENCH_OP_READ == xferBenchConfig::op_type) {
        struct stat st;
        if (::stat(file_name.c_str(), &st) == 0) {
            std::cout << "File " << file_name << " exists, size: " << st.st_size << std::endl;
            file_size = st.st_size;
        } else {
            std::cout << "File " << file_name << " does not exist, will be created." << std::endl;
        }
    }

    int fd = open(file_name.c_str(), flags, 0744);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << file_name << " with error: " << strerror(errno)
                  << std::endl;
        return std::nullopt;
    }

    return xferFileState{fd, file_size, 0};
}

// Create file descriptors from explicit filenames or auto-generate
static std::vector<xferFileState>
createFileFds(std::string name, int num_files, const std::vector<std::string> &filenames = {}) {
    std::vector<xferFileState> fds;
    int flags = O_RDWR | O_CREAT | O_LARGEFILE;

    if (!xferBenchConfig::isStorageBackend()) {
        std::cerr << "Unknown storage backend: " << xferBenchConfig::backend << std::endl;
        exit(EXIT_FAILURE);
    }

    if (xferBenchConfig::storage_enable_direct) {
        flags |= O_DIRECT;
    }

    // Use provided filenames if available
    if (!filenames.empty()) {
        if (filenames.size() != static_cast<size_t>(num_files)) {
            std::cerr << "Error: Number of filenames (" << filenames.size()
                      << ") doesn't match num_files (" << num_files << ")" << std::endl;
            exit(EXIT_FAILURE);
        }

        for (const auto &file_name : filenames) {
            std::cout << "Opening file: " << file_name << std::endl;
            auto fstate = openFileWithFlags(file_name, flags);
            if (!fstate) {
                // Cleanup already opened files
                for (auto &fd : fds) {
                    close(fd.fd);
                }
                return {};
            }
            fds.push_back(fstate.value());
        }
        return fds;
    }

    // Auto-generate filenames (backward compatibility)
    const std::string file_path = xferBenchConfig::filepath != "" ?
        xferBenchConfig::filepath :
        std::filesystem::current_path().string();
    std::string file_backend = xferBenchConfig::backend;
    std::transform(file_backend.begin(), file_backend.end(), file_backend.begin(), ::tolower);
    const std::string file_name_prefix = "/nixlbench_" + file_backend + "_test_file_";

    for (int i = 0; i < num_files; i++) {
        std::string file_name = file_path + file_name_prefix + name + "_" + std::to_string(i);
        std::cout << "Creating file: " << file_name << std::endl;

        auto fstate = openFileWithFlags(file_name, flags);
        if (!fstate) {
            // Cleanup already opened files
            for (int j = 0; j < i; j++) {
                close(fds[j].fd);
            }
            return {};
        }
        fds.push_back(fstate.value());
    }
    return fds;
}

std::optional<xferBenchIOV>
xferBenchNixlWorker::initBasicDescFile(size_t buffer_size, xferFileState &fstate, int mem_dev_id) {
    int fd = fstate.fd;
    uint64_t start_offset = fstate.offset;
    uint64_t end_offset = fstate.offset + buffer_size;
    auto ret = std::optional<xferBenchIOV>(std::in_place, fstate.offset, buffer_size, fd);

    fstate.offset = end_offset;

    // If in READ mode, only write if the region is not already present in the file
    if (XFERBENCH_OP_READ == xferBenchConfig::op_type && end_offset <= fstate.file_size) {
        return ret;
    }

    // Fill up with data
    void *buf;
    if (!allocateXferMemory(buffer_size, &buf)) {
        std::cerr << "Failed to allocate " << buffer_size << " bytes of memory" << std::endl;
        return std::nullopt;
    }

    // File is always initialized with XFERBENCH_TARGET_BUFFER_ELEMENT
    memset(buf, XFERBENCH_TARGET_BUFFER_ELEMENT, buffer_size);

    size_t offset = start_offset;
    char *write_ptr = static_cast<char *>(buf);
    while (buffer_size > 0) {
        ssize_t rc = pwrite(fd, write_ptr, buffer_size, offset);
        if (rc < 0) {
            std::cerr << "Failed to write to file: " << fd << " with error: " << strerror(errno)
                      << std::endl;
            return std::nullopt;
        }

        buffer_size -= rc;
        offset += rc;
        write_ptr += rc;
    }

    free(buf);

    if (end_offset > fstate.file_size) fstate.file_size = end_offset;

    return ret;
}

std::optional<xferBenchIOV>
xferBenchNixlWorker::initBasicDescObj(size_t buffer_size, int mem_dev_id, std::string name) {
    return std::optional<xferBenchIOV>(std::in_place, 0, buffer_size, mem_dev_id, name);
}

void
xferBenchNixlWorker::cleanupBasicDescDram(xferBenchIOV &iov) {
    free((void *)iov.addr);
}

#if HAVE_CUDA
void
xferBenchNixlWorker::cleanupBasicDescVram(xferBenchIOV &iov) {
    // Assume no CUDA cores exist if Neuron cores are found.
    // There are no AWS instance types with both NVIDIA GPUs and Neuron accelerators.
    if (neuronCoreCount() > 0) {
        CHECK_NEURON_ERROR(neuronFree((void *)iov.addr), "Failed to free nrt tensor");
        return;
    }

    CHECK_CUDA_ERROR(cudaSetDevice(iov.devId), "Failed to set device");
    if (xferBenchConfig::enable_vmm) {
        CHECK_CUDA_DRIVER_ERROR(cuMemUnmap(iov.addr, iov.len), "Failed to unmap memory");
        CHECK_CUDA_DRIVER_ERROR(cuMemRelease(iov.handle), "Failed to release memory");
        CHECK_CUDA_DRIVER_ERROR(cuMemAddressFree(iov.addr, iov.padded_size),
                                "Failed to free reserved address");
    } else {
        /*
         * CUDA streams allow for concurrent execution of kernels and memory operations. However,
         * memory management functions like cudaFree are implicitly synchronized with all streams to
         * guarantee safety. This means cudaFree will wait for all kernels (in any stream) that
         * might use the memory to finish before actually freeing it.
         * If the application hangs on cudaFree due to kernels running in other streams, switching
         * to cudaFreeAsync can allow the host to proceed without waiting for the entire device
         * synchronization.
         */
        CHECK_CUDA_ERROR(cudaFreeAsync((void *)iov.addr, 0), "Failed to deallocate CUDA buffer");
        CHECK_CUDA_ERROR(cudaStreamSynchronize(0), "Failed to synchronize stream 0");
    }
}
#endif /* HAVE_CUDA */

void
xferBenchNixlWorker::cleanupBasicDescFile(xferBenchIOV &iov) {
    close(iov.devId);
}

void
xferBenchNixlWorker::cleanupBasicDescObj(xferBenchIOV &iov) {
    if (xferBenchConfig::obj_prepop_num > 0) {
        // Prepop mode: keep all objects so they can be reused by subsequent GET benchmarks.
        return;
    }
    if (!xferBenchUtils::rmObj(iov.metaInfo)) {
        std::cerr << "Failed to remove object: " << iov.metaInfo << std::endl;
        exit(EXIT_FAILURE);
    }
}

std::optional<xferBenchIOV>
xferBenchNixlWorker::initBasicDescBlk(size_t buffer_size, int mem_dev_id, size_t dev_offset) {
    // The dev_offset represents the LBA (Logical Block Address) offset in the block device

    // Create IOV with LBA offset as address, buffer size, and device ID
    // The device ID corresponds to the block device UUID (e.g., 11 for local file, 14 for
    // /dev/zero)
    return std::optional<xferBenchIOV>(std::in_place, dev_offset, buffer_size, mem_dev_id);
}

void
xferBenchNixlWorker::cleanupBasicDescBlk(xferBenchIOV &iov) {
    // No cleanup needed for block device descriptors
    // The block device backend handles the device lifecycle
}

bool
xferBenchNixlWorker::ensureFileHasConsistencyData(const GusliDeviceConfig &device, size_t size) {
    int flags = O_RDWR | O_CREAT | O_LARGEFILE;
    if (xferBenchConfig::storage_enable_direct) flags |= O_DIRECT;

    int fd = open(device.device_path.c_str(), flags, 0744);
    if (fd < 0) {
        std::cerr << "Failed to open GUSLI file: " << device.device_path << ": " << strerror(errno)
                  << std::endl;
        return false;
    }

    // Sample one page at the offset GUSLI will read from
    void *check_buf;
    bool needs_write = true;
    if (allocateXferMemory(xferBenchConfig::page_size, &check_buf)) {
        ssize_t rd = pread(fd, check_buf, xferBenchConfig::page_size, device.dev_offset);
        if (rd == (ssize_t)xferBenchConfig::page_size) {
            needs_write = false;
            uint8_t *bytes = static_cast<uint8_t *>(check_buf);
            for (ssize_t i = 0; i < rd; i++) {
                if (bytes[i] != XFERBENCH_TARGET_BUFFER_ELEMENT) {
                    needs_write = true;
                    break;
                }
            }
        }
        free(check_buf);
    }

    if (needs_write) {
        std::cout << "Warning: GUSLI file '" << device.device_path << "' at offset "
                  << device.dev_offset << " does not contain expected pattern (0x" << std::hex
                  << (int)XFERBENCH_TARGET_BUFFER_ELEMENT << std::dec << "). Overwriting."
                  << std::endl;

        void *buf;
        if (!allocateXferMemory(size, &buf)) {
            close(fd);
            return false;
        }
        memset(buf, XFERBENCH_TARGET_BUFFER_ELEMENT, size);

        size_t remaining = size;
        size_t offset = device.dev_offset;
        char *write_ptr = static_cast<char *>(buf);
        while (remaining > 0) {
            ssize_t rc = pwrite(fd, write_ptr, remaining, offset);
            if (rc < 0) {
                std::cerr << "Failed to write to " << device.device_path << " at offset " << offset
                          << ": " << strerror(errno) << std::endl;
                free(buf);
                close(fd);
                return false;
            }
            remaining -= rc;
            offset += rc;
            write_ptr += rc;
        }
        free(buf);
    } else {
        std::cout << "GUSLI file '" << device.device_path << "' at offset " << device.dev_offset
                  << " already contains expected pattern (0x" << std::hex
                  << (int)XFERBENCH_TARGET_BUFFER_ELEMENT << std::dec
                  << "). Skipping initialization." << std::endl;
    }

    close(fd);
    return true;
}

std::vector<std::vector<xferBenchIOV>>
xferBenchNixlWorker::allocateMemory(int num_threads) {
    std::vector<std::vector<xferBenchIOV>> iov_lists;
    size_t i, buffer_size, num_devices = 0;
    nixl_opt_args_t opt_args;

    if (isInitiator()) {
        num_devices = xferBenchConfig::num_initiator_dev;
    } else if (isTarget()) {
        num_devices = xferBenchConfig::num_target_dev;
    }
    buffer_size = xferBenchConfig::total_buffer_size / (num_devices * num_threads);

    // For OBJ backend the DRAM staging buffer only needs to hold one transfer block.
    // The default total_buffer_size (8 GiB) would cause ibv_reg_mr to fail when
    // the RDMA path tries to pin that much memory.  Cap to max_block_size so that
    // RDMA MR registration succeeds and measurements reflect real RDMA behaviour.
    // In batch mode, need batch_size × object_size for the full batched load.
    if (xferBenchConfig::isObjStorageBackend() && xferBenchConfig::max_block_size > 0) {
        if (xferBenchConfig::batch_mode && xferBenchConfig::obj_prepop_num > 0) {
            // Batch mode: buffer must hold batch_size × object_size (not full prepop pool)
            int batch_n = xferBenchConfig::batch_size > 0
                        ? xferBenchConfig::batch_size
                        : xferBenchConfig::obj_prepop_num;
            size_t batch_total = (size_t)batch_n * xferBenchConfig::max_block_size;
            buffer_size = std::min(buffer_size, std::max(batch_total, xferBenchConfig::max_block_size));
        } else {
            buffer_size = std::min(buffer_size, xferBenchConfig::max_block_size);
        }
    }

    if (xferBenchConfig::storage_enable_direct) {
        if (xferBenchConfig::page_size == 0) {
            std::cerr << "Error: Invalid page size returned by sysconf" << std::endl;
            exit(EXIT_FAILURE);
        }
        buffer_size =
            ((buffer_size + xferBenchConfig::page_size - 1) / xferBenchConfig::page_size) *
            xferBenchConfig::page_size;
    }

    opt_args.backends.push_back(backend_engine);

    if (xferBenchConfig::isObjStorageBackend()) {
        const bool is_prepop = (xferBenchConfig::obj_prepop_num > 0);
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        uint64_t timestamp = tv.tv_sec * 1000000ULL + tv.tv_usec;

        prepop_keys_.clear();
        prepop_keys_.resize(num_threads);
        prepop_base_dev_ids_.clear();
        prepop_base_dev_ids_.resize(num_threads, 0);

        for (int list_idx = 0; list_idx < num_threads; list_idx++) {
            std::vector<xferBenchIOV> iov_list;
            for (i = 0; i < num_devices; i++) {
                std::optional<xferBenchIOV> basic_desc;
                std::string unique_name;

                if (is_prepop) {
                    // Base devId for this (thread, dev) pair; ensures no devId collisions.
                    int base_dev_id =
                        (list_idx * num_devices + i) * xferBenchConfig::obj_prepop_num;

                    if (!xferBenchConfig::obj_prepop_keys_file.empty()) {
                        // Read keys from file once, then scatter across threads.
                        static std::vector<std::string> all_file_keys;
                        static bool file_loaded = false;
                        if (!file_loaded) {
                            std::ifstream keys_file(xferBenchConfig::obj_prepop_keys_file);
                            if (!keys_file.is_open()) {
                                std::cerr << "Failed to open keys file: "
                                          << xferBenchConfig::obj_prepop_keys_file << std::endl;
                                return {};
                            }
                            std::string line;
                            while (std::getline(keys_file, line)) {
                                if (!line.empty())
                                    all_file_keys.push_back(line);
                            }
                            keys_file.close();
                            file_loaded = true;
                            std::cout << "Loaded " << all_file_keys.size()
                                      << " keys from " << xferBenchConfig::obj_prepop_keys_file
                                      << std::endl;
                        }
                        // Scatter keys to this thread: stride by num_threads.
                        int T = num_threads * num_devices;
                        int my_idx = list_idx * num_devices + i;
                        for (size_t k = my_idx; k < all_file_keys.size(); k += T) {
                            prepop_keys_[list_idx].push_back(all_file_keys[k]);
                        }
                        xferBenchConfig::obj_prepop_num = (int)prepop_keys_[list_idx].size();
                        base_dev_id = my_idx * xferBenchConfig::obj_prepop_num;
                        prepop_base_dev_ids_[list_idx] = base_dev_id;
                    } else if (xferBenchConfig::obj_prepop_keys_file.empty()) {
                        // Default: generate prepop_{size}B_{thread}_{dev}_{idx:06d} keys
                        std::string key_prefix =
                            "prepop_" + std::to_string(xferBenchConfig::start_block_size) + "B_" +
                            std::to_string(list_idx) + "_" + std::to_string(i) + "_";
                        for (int k = 0; k < xferBenchConfig::obj_prepop_num; k++) {
                            std::ostringstream oss;
                            oss << key_prefix << std::setfill('0') << std::setw(6) << k;
                            prepop_keys_[list_idx].push_back(oss.str());
                        }
                        prepop_base_dev_ids_[list_idx] = base_dev_id;
                    }
                    // Register all obj_prepop_num objects in a single batch.
                    // Each gets a unique devId so the transfer loop can rotate via devId.
                    std::vector<xferBenchIOV> all_prepop_iovs;
                    nixl_reg_dlist_t all_desc(OBJ_SEG);
                    for (int k = 0; k < xferBenchConfig::obj_prepop_num; k++) {
                        const std::string &key = prepop_keys_[list_idx][k];
                        nixlBlobDesc bd;
                        bd.addr    = 0;
                        bd.len     = buffer_size;
                        bd.devId   = base_dev_id + k;
                        bd.metaInfo = key;
                        all_desc.addDesc(bd);
                        all_prepop_iovs.emplace_back(0, buffer_size, base_dev_id + k, key);
                    }
                    CHECK_NIXL_ERROR(agent->registerMem(all_desc, &opt_args),
                                     "registerMem prepop failed");
                    prepop_all_iovs_.push_back(std::move(all_prepop_iovs));

                    // Push a dummy entry to iov_list so remote_iovs gets the base devId.
                    // This entry is NOT re-registered — it is part of prepop_all_iovs_.
                    unique_name = prepop_keys_[list_idx][0];
                    basic_desc = initBasicDescObj(buffer_size, base_dev_id, unique_name);
                    std::cout << "Prepop: registered " << xferBenchConfig::obj_prepop_num
                              << " objects (devIds " << base_dev_id << ".."
                              << (base_dev_id + xferBenchConfig::obj_prepop_num - 1)
                              << "), key[0]=" << unique_name << std::endl;
                } else {
                    unique_name = "nixlbench_obj" + std::to_string(list_idx) + "_" +
                        std::to_string(i) + "_" + std::to_string(timestamp);
                    if (xferBenchConfig::op_type == XFERBENCH_OP_READ) {
                        if (!xferBenchUtils::putObj(buffer_size, unique_name)) {
                            std::cerr << "Failed to put object: " << unique_name << std::endl;
                            continue;
                        }
                    }
                    std::cout << "Creating obj: " << unique_name << std::endl;
                }

                if (!is_prepop) {
                    basic_desc = initBasicDescObj(buffer_size, i, unique_name);
                }
                if (basic_desc) {
                    iov_list.push_back(basic_desc.value());
                }
            }
            if (is_prepop) {
                // All prepop objects were already registered in bulk above; do NOT call
                // registerMem again for iov_list (it contains dummy entries only).
                remote_iovs.push_back(iov_list);
            } else {
                nixl_reg_dlist_t desc_list(OBJ_SEG);
                iovListToNixlRegDlist(iov_list, desc_list);
                CHECK_NIXL_ERROR(agent->registerMem(desc_list, &opt_args), "registerMem failed");
                remote_iovs.push_back(iov_list);
            }
        }
    } else if (XFERBENCH_BACKEND_GUSLI == xferBenchConfig::backend) {
        // GUSLI backend uses block device descriptors
        if (gusli_devices.empty()) {
            std::cerr << "No GUSLI devices configured" << std::endl;
            exit(EXIT_FAILURE);
        }

        if (xferBenchConfig::op_type == XFERBENCH_OP_READ) {
            for (auto &device : gusli_devices) {
                if (device.device_type == 'F' &&
                    !ensureFileHasConsistencyData(device, buffer_size)) {
                    exit(EXIT_FAILURE);
                }
            }
        }

        for (int list_idx = 0; list_idx < num_threads; list_idx++) {
            std::vector<xferBenchIOV> iov_list;
            for (i = 0; i < num_devices; i++) {
                std::optional<xferBenchIOV> basic_desc;
                // Use device IDs from parsed configuration (num_devices == gusli_devices.size())
                basic_desc = initBasicDescBlk(
                    buffer_size, gusli_devices[i].device_id, gusli_devices[i].dev_offset);
                if (basic_desc) {
                    iov_list.push_back(basic_desc.value());
                }
            }
            nixl_reg_dlist_t desc_list(BLK_SEG);
            iovListToNixlRegDlist(iov_list, desc_list);
            CHECK_NIXL_ERROR(agent->registerMem(desc_list, &opt_args), "registerMem failed");
            remote_iovs.push_back(iov_list);
        }
    } else if (xferBenchConfig::isStorageBackend()) {
        int num_buffers = num_threads * num_devices;
        int num_files = xferBenchConfig::num_files;
        int remainder_buffers = num_buffers % num_files;

        if (num_files > num_buffers) {
            std::cerr << "Error: number of buffers (" << num_buffers
                      << ") needs to be bigger or equal to the number of files (" << num_files
                      << "). Try adjusting num_files." << std::endl;
            exit(EXIT_FAILURE);
        }

        if (remainder_buffers != 0) {
            std::cerr << "Error: number of buffers (" << num_buffers
                      << ") needs to be divisible by the number of files (" << num_files
                      << "). Try adjusting num_files." << std::endl;
            exit(EXIT_FAILURE);
        }

        std::vector<std::string> filenames;
        if (!xferBenchConfig::filenames.empty()) {
            std::string filename;
            std::stringstream ss(xferBenchConfig::filenames);
            while (std::getline(ss, filename, ',')) {
                filenames.push_back(filename);
            }
        }
        remote_fds = createFileFds(getName(), num_files, filenames);
        if (remote_fds.empty()) {
            std::cerr << "Failed to create " << xferBenchConfig::backend << " file" << std::endl;
            exit(EXIT_FAILURE);
        }

        int file_idx = 0;
        for (int list_idx = 0; list_idx < num_threads; list_idx++) {
            std::vector<xferBenchIOV> iov_list;
            for (i = 0; i < num_devices; i++) {
                std::optional<xferBenchIOV> basic_desc;
                basic_desc = initBasicDescFile(buffer_size, remote_fds[file_idx], i);
                if (basic_desc) {
                    iov_list.push_back(basic_desc.value());
                }
                file_idx += 1;
                if (file_idx >= num_files) file_idx = 0;
            }
            nixl_reg_dlist_t desc_list(FILE_SEG);
            iovListToNixlRegDlist(iov_list, desc_list);
            CHECK_NIXL_ERROR(agent->registerMem(desc_list, &opt_args), "registerMem failed");
            remote_iovs.push_back(iov_list);
        }
    }

    for (int list_idx = 0; list_idx < num_threads; list_idx++) {
        std::vector<xferBenchIOV> iov_list;
        for (i = 0; i < num_devices; i++) {
            std::optional<xferBenchIOV> basic_desc;

            switch (seg_type) {
            case DRAM_SEG: {
                // For GUSLI backend, use device ID from parsed configuration
                int mem_dev_id = (XFERBENCH_BACKEND_GUSLI == xferBenchConfig::backend &&
                                  !gusli_devices.empty()) ?
                    gusli_devices[i].device_id :
                    i;
                basic_desc = initBasicDescDram(buffer_size, mem_dev_id);
                break;
            }
#if HAVE_CUDA
            case VRAM_SEG:
                basic_desc = initBasicDescVram(buffer_size, i);
                break;
#endif
            default:
                std::cerr << "Unsupported mem type: " << seg_type << std::endl;
                exit(EXIT_FAILURE);
            }

            if (basic_desc) {
                if (!remote_iovs.empty()) {
                    basic_desc.value().metaInfo = remote_iovs[list_idx][i].metaInfo;
                }
                iov_list.push_back(basic_desc.value());
            }
        }

        nixl_reg_dlist_t desc_list(seg_type);
        iovListToNixlRegDlist(iov_list, desc_list);
        CHECK_NIXL_ERROR(agent->registerMem(desc_list, &opt_args), "registerMem failed");
        iov_lists.push_back(iov_list);

        /*
         * Workaround for a GUSLI registration bug which resets memory to 0, this initialization
         * is only needed when validating data. It was moved from the initBasicDescDram function to
         * here to avoid memsetting the memory again.
         */
        if (seg_type == DRAM_SEG && xferBenchConfig::check_consistency) {
            for (auto &iov : iov_list) {
                if (isInitiator()) {
                    memset((void *)iov.addr, XFERBENCH_INITIATOR_BUFFER_ELEMENT, buffer_size);
                } else if (isTarget()) {
                    memset((void *)iov.addr, XFERBENCH_TARGET_BUFFER_ELEMENT, buffer_size);
                }
            }
        }
    }

    return iov_lists;
}

void
xferBenchNixlWorker::deallocateMemory(std::vector<std::vector<xferBenchIOV>> &iov_lists) {
    nixl_opt_args_t opt_args;


    opt_args.backends.push_back(backend_engine);
    for (auto &iov_list : iov_lists) {
        nixl_reg_dlist_t desc_list(seg_type);
        iovListToNixlRegDlist(iov_list, desc_list);
        CHECK_NIXL_ERROR(agent->deregisterMem(desc_list, &opt_args), "deregisterMem failed");

        for (auto &iov : iov_list) {
            switch (seg_type) {
            case DRAM_SEG:
                cleanupBasicDescDram(iov);
                break;
#if HAVE_CUDA
            case VRAM_SEG:
                cleanupBasicDescVram(iov);
                break;
#endif
            default:
                std::cerr << "Unsupported mem type: " << seg_type << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }

    if (xferBenchConfig::isObjStorageBackend()) {
        if (!prepop_all_iovs_.empty()) {
            // Prepop mode: all 12K objects were registered in prepop_all_iovs_ (not remote_iovs).
            // Deregister them here; S3 objects are kept (cleanupBasicDescObj is a no-op).
            for (auto &iov_list : prepop_all_iovs_) {
                nixl_reg_dlist_t desc_list(OBJ_SEG);
                iovListToNixlRegDlist(iov_list, desc_list);
                agent->deregisterMem(desc_list, &opt_args);  // best-effort; ignore error
            }
            prepop_all_iovs_.clear();
        } else {
            for (auto &iov_list : remote_iovs) {
                for (auto &iov : iov_list) {
                    cleanupBasicDescObj(iov);
                }
                nixl_reg_dlist_t desc_list(OBJ_SEG);
                iovListToNixlRegDlist(iov_list, desc_list);
                CHECK_NIXL_ERROR(agent->deregisterMem(desc_list, &opt_args),
                                 "deregisterMem failed");
            }
        }
    } else if (xferBenchConfig::backend == XFERBENCH_BACKEND_GUSLI) {
        for (auto &iov_list : remote_iovs) {
            for (auto &iov : iov_list) {
                cleanupBasicDescBlk(iov);
            }
            nixl_reg_dlist_t desc_list(BLK_SEG);
            iovListToNixlRegDlist(iov_list, desc_list);
            CHECK_NIXL_ERROR(agent->deregisterMem(desc_list, &opt_args), "deregisterMem failed");
        }
    } else if (xferBenchConfig::isStorageBackend()) {
        for (auto &iov_list : remote_iovs) {
            for (auto &iov : iov_list) {
                cleanupBasicDescFile(iov);
            }
            nixl_reg_dlist_t desc_list(FILE_SEG);
            iovListToNixlRegDlist(iov_list, desc_list);
            CHECK_NIXL_ERROR(agent->deregisterMem(desc_list, &opt_args), "deregisterMem failed");
        }
    }
}

int
xferBenchNixlWorker::exchangeMetadata() {
    int meta_sz, ret = 0;

    // Skip metadata exchange for storage backends or when ETCD is not available
    if (xferBenchConfig::isStorageBackend()) {
        return 0;
    }

    if (isTarget()) {
        std::string local_metadata;
        const char *buffer;
        int destrank;

        agent->getLocalMD(local_metadata);

        buffer = local_metadata.data();
        meta_sz = local_metadata.size();

        if (IS_PAIRWISE_AND_SG()) {
            destrank = rt->getRank() - xferBenchConfig::num_target_dev;
            // XXX: Fix up the rank, depends on processes distributed on hosts
            // assumes placement is adjacent ranks to same node
        } else {
            destrank = 0;
        }
        rt->sendInt(&meta_sz, destrank);
        rt->sendChar((char *)buffer, meta_sz, destrank);
    } else if (isInitiator()) {
        std::string remote_agent;
        int srcrank;

        if (IS_PAIRWISE_AND_SG()) {
            srcrank = rt->getRank() + xferBenchConfig::num_initiator_dev;
            // XXX: Fix up the rank, depends on processes distributed on hosts
            // assumes placement is adjacent ranks to same node
        } else {
            srcrank = 1;
        }

        ret = rt->recvInt(&meta_sz, srcrank);
        if (ret < 0) {
            std::cerr << "NIXL: failed to receive metadata size" << std::endl;
            return ret;
        }

        std::string remote_metadata(meta_sz, '\0');
        ret = rt->recvChar(remote_metadata.data(), meta_sz, srcrank);
        if (ret < 0) {
            std::cerr << "NIXL: failed to receive metadata" << std::endl;
            return ret;
        }

        nixl_status_t status = agent->loadRemoteMD(remote_metadata, remote_agent);
        if (status != NIXL_SUCCESS) {
            std::cerr << "NIXL: loadRemoteMD failed: " << nixlEnumStrings::statusStr(status)
                      << std::endl;
            return -1;
        }
    }

    return ret;
}

std::vector<std::vector<xferBenchIOV>>
xferBenchNixlWorker::exchangeIOV(const std::vector<std::vector<xferBenchIOV>> &local_iovs,
                                 size_t block_size) {
    std::vector<std::vector<xferBenchIOV>> res;
    int desc_str_sz;

    if (xferBenchConfig::isStorageBackend()) {
        size_t fd_idx = 0;
        uint64_t file_offset = 0;
        for (auto &iov_list : local_iovs) {
            std::vector<xferBenchIOV> remote_iov_list;
            int devidx = 0;
            for (auto &iov : iov_list) {
                if (xferBenchConfig::isObjStorageBackend()) {
                    std::optional<xferBenchIOV> basic_desc;
                    basic_desc = initBasicDescObj(iov.len, iov.devId, iov.metaInfo);
                    if (basic_desc) {
                        remote_iov_list.push_back(basic_desc.value());
                    }
                } else if (XFERBENCH_BACKEND_GUSLI == xferBenchConfig::backend) {
                    xferBenchIOV iov_remote(iov);
                    iov_remote.addr = gusli_devices[devidx++].dev_offset + file_offset;
                    iov_remote.len = block_size;
                    iov_remote.devId = iov.devId;
                    remote_iov_list.push_back(iov_remote);
                } else {
                    xferBenchIOV iov_remote(iov);
                    iov_remote.addr = file_offset;
                    iov_remote.len = block_size;
                    iov_remote.devId = remote_fds[fd_idx].fd;
                    remote_iov_list.push_back(iov_remote);
                    fd_idx++;
                    if (fd_idx >= remote_fds.size()) {
                        file_offset += block_size;
                        fd_idx = 0;
                    }
                }
            }
            res.push_back(remote_iov_list);
            file_offset += block_size;
        }
    } else {
        for (const auto &local_iov : local_iovs) {
            nixlSerDes ser_des;
            nixl_xfer_dlist_t local_desc(seg_type);

            iovListToNixlXferDlist(local_iov, local_desc);

            if (isTarget()) {
                int destrank;
                if (IS_PAIRWISE_AND_SG()) {
                    destrank = rt->getRank() - xferBenchConfig::num_target_dev;
                    // XXX: Fix up the rank, depends on processes distributed on hosts
                    // assumes placement is adjacent ranks to same node
                } else {
                    destrank = 0;
                }

                local_desc.serialize(&ser_des);
                std::string desc_str = ser_des.exportStr();
                desc_str_sz = desc_str.size();
                rt->sendInt(&desc_str_sz, destrank);
                rt->sendChar(desc_str.data(), desc_str.size(), destrank);
            } else if (isInitiator()) {
                int srcrank;
                if (IS_PAIRWISE_AND_SG()) {
                    srcrank = rt->getRank() + xferBenchConfig::num_initiator_dev;
                    // XXX: Fix up the rank, depends on processes distributed on hosts
                    // assumes placement is adjacent ranks to same node
                } else {
                    srcrank = 1;
                }

                if (rt->recvInt(&desc_str_sz, srcrank) != 0) {
                    std::cerr << "NIXL: failed to receive metadata size" << std::endl;
                    std::exit(EXIT_FAILURE);
                }

                std::string desc_str;
                desc_str.resize(desc_str_sz, '\0');
                if (rt->recvChar(desc_str.data(), desc_str.size(), srcrank) != 0) {
                    std::cerr << "NIXL: failed to receive metadata" << std::endl;
                    std::exit(EXIT_FAILURE);
                }

                ser_des.importStr(desc_str);

                nixl_xfer_dlist_t remote_desc(&ser_des);
                res.emplace_back(nixlXferDlistToIOVList(remote_desc));
            }
        }
    }
    // Ensure all processes have completed the exchange with a barrier/sync
    synchronize();
    return res;
}

// Per-request wallclock tracing (enabled with NIXL_TRACE=1).
// Prints absolute system_clock timestamps at POST and DONE for each xfer so
// they can be correlated against RGW radosgw.8000.log timestamps.
static inline bool nixl_trace_enabled() {
    static const bool en = (std::getenv("NIXL_TRACE") != nullptr);
    return en;
}

static inline double nixl_now_sec() {
    auto t = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(t).count() / 1e6;
}

static inline void nixl_trace(const char *evt, int tid, int slot, int iter) {
    if (!nixl_trace_enabled()) return;
    fprintf(stderr, "[NIXL_TRACE t=%.6f tid=%d slot=%d iter=%d] %s\n",
            nixl_now_sec(), tid, slot, iter, evt);
    fflush(stderr);
}

// Per-iteration markers written into the OBJ plugin trace log
// (/tmp/nixl_obj_trace_us.log by default, NIXL_OBJ_TRACE_FILE override). Format
// matches the NIXL_OBJ_US_R macro in src/plugins/obj/s3/obj_us_trace.h so that
// post-processing can merge these markers with the per-chunk completion events
// emitted from the OBJ plugin.
static inline FILE* obj_trace_fp() {
    static FILE* f = nullptr;
    static std::once_flag once;
    std::call_once(once, []{
        const char* path = std::getenv("NIXL_OBJ_TRACE_FILE");
        if (!path) path = "/tmp/nixl_obj_trace_us.log";
        f = std::fopen(path, "a");
        if (f) std::setvbuf(f, nullptr, _IOLBF, 0);
    });
    return f;
}

static inline bool obj_trace_enabled() {
    static const bool en = (std::getenv("NIXL_OBJ_TRACE") != nullptr);
    return en;
}

// Whether to add a synchronizing #pragma omp barrier at the top of each
// iteration so that "iteration k" is well-defined globally across threads.
// Off by default — enabling adds a per-iter rendezvous cost (slow threads
// stall fast ones), which intentionally slightly reduces the BW number but
// makes per-iteration TTFL/TFL analysis clean.
static inline bool iter_barrier_enabled() {
    static const bool en = (std::getenv("NIXL_ITER_BARRIER") != nullptr);
    return en;
}

static inline void obj_trace_iter_marker(const char *evt, int iter_idx) {
    if (!obj_trace_enabled()) return;
    FILE* f = obj_trace_fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %d %s\n", ts, tid, iter_idx, evt);
}

// One-shot config marker emitted at the start of transfer(), capturing the
// run-wide knobs that determine how to interpret per-chunk events. Format
// keeps the same 4 leading columns (ts, tid, iter_idx=-1 placeholder, event)
// so existing parsers see "config" as just another event name. The K=V tail
// is appended after the event name.
static inline void obj_trace_emit_config(int num_threads, int num_iter, int warmup_iter,
                                         size_t start_batch_size, bool batch_mode,
                                         int batch_size, int num_threads_batch,
                                         const char *op_type) {
    if (!obj_trace_enabled()) return;
    FILE* f = obj_trace_fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f,
        "%.6f %ld -1 nixlbench_config op=%s num_threads=%d num_iter=%d warmup_iter=%d "
        "start_batch_size=%zu batch_mode=%d batch_size=%d num_threads_batch=%d\n",
        ts, tid, op_type, num_threads, num_iter, warmup_iter,
        start_batch_size, batch_mode ? 1 : 0, batch_size, num_threads_batch);
}

// Helper to execute a single transfer iteration
static inline nixl_status_t
execSingleTransfer(nixlAgent *agent,
                   nixlXferReqH *req,
                   xferBenchTimer &timer,
                   xferBenchStats &thread_stats,
                   int trace_iter = -1) {
    int trace_tid = nixl_trace_enabled() ? omp_get_thread_num() : 0;
    nixl_trace("POST", trace_tid, 0, trace_iter);
    nixl_status_t rc = agent->postXferReq(req);
    thread_stats.post_duration.add(timer.lap());
    while (NIXL_IN_PROG == rc) {
        rc = agent->getXferStatus(req);
    }
    nixl_trace("DONE", trace_tid, 0, trace_iter);
    return rc;
}

// Helper to prepare transfer descriptors based on backend type
static void
prepareTransferDescriptors(nixl_xfer_dlist_t &local_desc,
                           nixl_xfer_dlist_t &remote_desc,
                           const std::vector<xferBenchIOV> &local_iov,
                           const std::vector<xferBenchIOV> &remote_iov) {
    // Set remote descriptor type based on backend
    if (xferBenchConfig::isObjStorageBackend()) {
        remote_desc = nixl_xfer_dlist_t(OBJ_SEG);
    } else if (XFERBENCH_BACKEND_GUSLI == xferBenchConfig::backend) {
        remote_desc = nixl_xfer_dlist_t(BLK_SEG);
    } else if (xferBenchConfig::isStorageBackend()) {
        remote_desc = nixl_xfer_dlist_t(FILE_SEG);
    }

    iovListToNixlXferDlist(local_iov, local_desc);
    iovListToNixlXferDlist(remote_iov, remote_desc);
}

// Execute transfers with configurable request lifecycle behavior
// recreate_per_iteration: true for GUSLI (bug workaround), false for standard backends
static int
execTransferIterations(nixlAgent *agent,
                       const nixl_xfer_op_t op,
                       nixl_xfer_dlist_t &local_desc,
                       nixl_xfer_dlist_t &remote_desc,
                       const std::string &target,
                       nixl_opt_args_t &params,
                       const int num_iter,
                       xferBenchTimer &timer,
                       xferBenchStats &thread_stats,
                       const bool recreate_per_iteration,
                       const std::vector<std::string> *prepop_keys,
                       const int prepop_iter_offset) {
    nixlXferReqH *req = nullptr;
    nixlTime::us_t total_prepare_duration = 0;

    // Prepop key rotation requires recreating the xfer request per iteration so the OBJ
    // backend sees the updated devId (which maps to a different registered S3 key).
    const bool use_recreate = recreate_per_iteration || (prepop_keys && !prepop_keys->empty());

    // Capture base devId for prepop rotation.
    const uint64_t prepop_base_dev_id =
        (prepop_keys && !prepop_keys->empty()) ? remote_desc[0].devId : 0;

    // ----------------------------------------------------------------------
    // CPU-staged GPU buffer mode (-cuda_stage_after_op):
    //
    // When enabled and the initiator buffer is host DRAM, every iteration
    // also issues a synchronous cudaMemcpy between the host buffer and a
    // per-thread CUDA device buffer (H2D for READ, D2H for WRITE) inside
    // the timed region. The resulting end-to-end throughput is the
    // "naive serial CPU staging" baseline used by Figure 3 to compare
    // against the GPUDirect path (-initiator_seg_type VRAM, no extra
    // flag).
    //
    // We allocate one CUDA buffer per OMP thread, sized to the largest
    // local descriptor we expect to see (original local_desc[0].len at
    // entry; layerwise mode shrinks the per-iter len, never grows it).
    // The buffer is freed at function exit.
    // ----------------------------------------------------------------------
#if HAVE_CUDA
    const bool stage_enabled = xferBenchConfig::cuda_stage_after_op &&
                               xferBenchConfig::initiator_seg_type ==
                                   XFERBENCH_SEG_TYPE_DRAM;
#else
    const bool stage_enabled = false;
#endif
    void *stage_gpu_buf = nullptr;
    size_t stage_buf_size = 0;
#if HAVE_CUDA
    if (stage_enabled) {
        stage_buf_size = static_cast<size_t>(local_desc[0].len);
        cudaError_t crc = cudaMalloc(&stage_gpu_buf, stage_buf_size);
        if (crc != cudaSuccess) {
            std::cerr << "cuda_stage_after_op: cudaMalloc(" << stage_buf_size
                      << ") failed: " << cudaGetErrorString(crc) << std::endl;
            return -1;
        }
    }
    auto stage_h2d = [&](size_t len) {
        // After a READ completes: host buffer -> GPU device buffer.
        if (!stage_enabled) return;
        cudaError_t crc = cudaMemcpy(stage_gpu_buf,
                                     reinterpret_cast<const void *>(local_desc[0].addr),
                                     len, cudaMemcpyHostToDevice);
        if (crc != cudaSuccess)
            std::cerr << "stage_h2d cudaMemcpy failed: "
                      << cudaGetErrorString(crc) << std::endl;
    };
    auto stage_d2h = [&](size_t len) {
        // Before a WRITE: GPU device buffer -> host buffer (so the
        // network op then sends the staged bytes).
        if (!stage_enabled) return;
        cudaError_t crc = cudaMemcpy(reinterpret_cast<void *>(local_desc[0].addr),
                                     stage_gpu_buf, len, cudaMemcpyDeviceToHost);
        if (crc != cudaSuccess)
            std::cerr << "stage_d2h cudaMemcpy failed: "
                      << cudaGetErrorString(crc) << std::endl;
    };
    // Cleanup helper used at every return from this function below.
    auto stage_cleanup = [&]() {
        if (stage_gpu_buf) { cudaFree(stage_gpu_buf); stage_gpu_buf = nullptr; }
    };
#else
    auto stage_h2d = [](size_t) {};
    auto stage_d2h = [](size_t) {};
    auto stage_cleanup = []() {};
#endif

    // ----------------------------------------------------------------------
    // iodepth > 1: sliding-window async path (FIO-style)
    //
    // Maintain Q outstanding nixlXferReq handles. Fill the window by posting
    // Q requests, then loop: poll all slots round-robin for any DONE, on
    // completion record the per-request latency and immediately post the
    // next iteration into the freed slot. Sliding window invariant:
    // (posted - completed) == Q at steady state, dropping naturally during
    // the final drain phase.
    //
    // Notes:
    //   - Both recreate and non-recreate paths are supported. In recreate
    //     mode (used by OBJ prepop key rotation) we createXferReq() per slot
    //     per iteration so devId can rotate. In non-recreate mode we create
    //     Q distinct request handles upfront and reuse each one within its
    //     slot for the lifetime of the run.
    //   - Per-request latency = wall-clock from postXferReq to NIXL_SUCCESS,
    //     measured per slot. Throughput = total_bytes / total_wall_clock
    //     (formula unchanged from the iodepth=1 path; printStats handles it).
    //   - For OBJ backends, num_threads also sizes the AWS SDK asio pool, so
    //     for full pipelining you typically want num_threads >= iodepth.
    // ----------------------------------------------------------------------
    const int iodepth = std::max(1, xferBenchConfig::iodepth);
    if (iodepth > 1) {
        std::vector<nixlXferReqH *> slot_req(iodepth, nullptr);
        std::vector<xferBenchTimer> slot_timer(iodepth);
        std::vector<bool>           slot_armed(iodepth, false);
        nixlTime::us_t total_prepare_duration = 0;

        // Helper: build (or rebuild) a request for slot `s` to service iter `it`,
        // then postXferReq it and start the per-slot timer.
        auto post_slot = [&](int s, int it) -> int {
            // Apply prepop devId rotation for THIS iteration if needed.
            if (use_recreate && prepop_keys && !prepop_keys->empty()) {
                int num_chunks = (int)prepop_keys->size();
                int idx = (prepop_iter_offset + it) % num_chunks;
                remote_desc[0].devId = prepop_base_dev_id + idx;
            }

            // (Re)create the request for this slot if needed.
            if (use_recreate || slot_req[s] == nullptr) {
                if (use_recreate && slot_req[s] != nullptr) {
                    agent->releaseXferReq(slot_req[s]);
                    slot_req[s] = nullptr;
                }
                nixl_status_t create_rc = agent->createXferReq(
                    op, local_desc, remote_desc, target, slot_req[s], &params);
                if (NIXL_SUCCESS != create_rc) {
                    std::cerr << "createXferReq failed: "
                              << nixlEnumStrings::statusStr(create_rc) << std::endl;
                    return -1;
                }
            }
            total_prepare_duration += timer.lap();

            // Start per-slot timer immediately before postXferReq so the
            // measurement covers post + transfer + completion poll.
            slot_timer[s] = xferBenchTimer{};
            // CPU-staged WRITE: D2H copy before the network op so the
            // bytes the NIC sends come from the GPU buffer. Inside the
            // slot timer so the cost is included in iter latency.
            if (op == NIXL_WRITE) stage_d2h(local_desc[0].len);
            nixl_trace("POST", omp_get_thread_num(), s, it);
            nixl_status_t rc = agent->postXferReq(slot_req[s]);
            thread_stats.post_duration.add(timer.lap());
            if (rc != NIXL_SUCCESS && rc != NIXL_IN_PROG) {
                std::cout << "NIXL postXferReq failed: "
                          << nixlEnumStrings::statusStr(rc) << std::endl;
                return -1;
            }
            slot_armed[s] = true;
            return 0;
        };

        // Helper: drain a slot that has reached NIXL_SUCCESS, recording stats.
        auto reap_slot = [&](int s) -> int {
            nixl_trace("DONE", omp_get_thread_num(), s, -1);
            // CPU-staged READ: H2D copy after the data lands in host
            // memory but before we lap the timers, so the cost shows up
            // in transfer_duration / iter_duration.
            if (op == NIXL_READ) stage_h2d(local_desc[0].len);
            thread_stats.transfer_duration.add(timer.lap());
            thread_stats.iter_duration.add(slot_timer[s].lap());
            slot_armed[s] = false;
            if (use_recreate) {
                if (agent->releaseXferReq(slot_req[s]) != NIXL_SUCCESS) {
                    std::cout << "NIXL releaseXferReq failed" << std::endl;
                    return -1;
                }
                slot_req[s] = nullptr;
            }
            return 0;
        };

        int posted = 0;
        int completed = 0;

        // Phase 1: fill the initial window.
        while (posted < num_iter && posted < iodepth) {
            if (post_slot(posted, posted) < 0) return -1;
            posted++;
        }

        // Phase 2: slide — reap any DONE, post next iter into the freed slot.
        while (completed < num_iter) {
            int idx = -1;
            // Round-robin poll until at least one slot reports SUCCESS.
            // Mirrors FIO's daos_eq_poll(min=1) behavior.
            while (idx < 0) {
                for (int s = 0; s < iodepth; ++s) {
                    if (!slot_armed[s]) continue;
                    nixl_status_t rc = agent->getXferStatus(slot_req[s]);
                    if (rc == NIXL_SUCCESS) {
                        idx = s;
                        break;
                    }
                    if (rc != NIXL_IN_PROG) {
                        std::cout << "NIXL Xfer failed with status: "
                                  << nixlEnumStrings::statusStr(rc) << std::endl;
                        return -1;
                    }
                }
                // Tight spin — same as the iodepth=1 path's busy wait. If
                // CPU pressure becomes a problem add std::this_thread::yield()
                // here, but for max throughput keep spinning.
            }

            if (reap_slot(idx) < 0) return -1;
            completed++;

            if (posted < num_iter) {
                if (post_slot(idx, posted) < 0) return -1;
                posted++;
            }
        }

        // End of run: release any remaining (non-recreate) request handles.
        if (!use_recreate) {
            for (int s = 0; s < iodepth; ++s) {
                if (slot_req[s] != nullptr) {
                    if (agent->releaseXferReq(slot_req[s]) != NIXL_SUCCESS) {
                        std::cout << "NIXL releaseXferReq failed" << std::endl;
                        return -1;
                    }
                    slot_req[s] = nullptr;
                }
            }
        }

        thread_stats.prepare_duration.add(
            num_iter > 0 ? total_prepare_duration / num_iter : 0);
        stage_cleanup();
        return 0;
    }
    // ----------------------------------------------------------------------
    // iodepth == 1: original serial post-then-wait path, unchanged below.
    // ----------------------------------------------------------------------

    // Create request once if not recreating per iteration
    if (!use_recreate) {
        nixl_status_t create_rc =
            agent->createXferReq(op, local_desc, remote_desc, target, req, &params);
        if (NIXL_SUCCESS != create_rc) {
            std::cerr << "createXferReq failed: " << nixlEnumStrings::statusStr(create_rc)
                      << std::endl;
            stage_cleanup();
            return -1;
        }
        thread_stats.prepare_duration.add(timer.lap());
    }

    // Execute transfer iterations
    if (__builtin_expect(use_recreate, 0)) {
        // Recreate path: used by GUSLI and OBJ prepop (devId rotation).
        for (int i = 0; i < num_iter; ++i) {
            if (iter_barrier_enabled()) {
                #pragma omp barrier
                #pragma omp master
                obj_trace_iter_marker("nixlbench_iter_start", i);
            }
            // Rotate to a different pre-registered object by updating the remote devId.
            // The OBJ backend resolves devId → S3 key via its devIdToObjKey_ map.
            if (prepop_keys && !prepop_keys->empty()) {
                int num_chunks = (int)prepop_keys->size();
                int idx = (prepop_iter_offset + i) % num_chunks;
                remote_desc[0].devId = prepop_base_dev_id + idx;
            }

            xferBenchTimer iter_timer;
            // CPU-staged WRITE: D2H copy must happen BEFORE the network op
            // so the bytes the network sends come from the GPU buffer.
            if (op == NIXL_WRITE) stage_d2h(local_desc[0].len);
            nixl_status_t create_rc =
                agent->createXferReq(op, local_desc, remote_desc, target, req, &params);
            if (__builtin_expect(create_rc != NIXL_SUCCESS, 0)) {
                std::cerr << "createXferReq failed: " << nixlEnumStrings::statusStr(create_rc)
                          << std::endl;
                stage_cleanup();
                return -1;
            }
            total_prepare_duration += timer.lap();

            nixl_status_t rc = execSingleTransfer(agent, req, timer, thread_stats, i);

            if (__builtin_expect(rc != NIXL_SUCCESS, 0)) {
                std::cout << "NIXL Xfer failed with status: " << nixlEnumStrings::statusStr(rc)
                          << std::endl;
                agent->releaseXferReq(req);
                stage_cleanup();
                return -1;
            }
            // CPU-staged READ: H2D copy must happen AFTER the network op
            // delivers data into the host buffer, before we lap the timer.
            if (op == NIXL_READ) stage_h2d(local_desc[0].len);
            thread_stats.transfer_duration.add(timer.lap());

            if (__builtin_expect(agent->releaseXferReq(req) != NIXL_SUCCESS, 0)) {
                std::cout << "NIXL releaseXferReq failed" << std::endl;
                stage_cleanup();
                return -1;
            }
            thread_stats.iter_duration.add(iter_timer.lap());
            if (iter_barrier_enabled()) {
                #pragma omp barrier
                #pragma omp master
                obj_trace_iter_marker("nixlbench_iter_end", i);
            }
        }
        // Average prepare duration across iterations
        thread_stats.prepare_duration.add(total_prepare_duration / num_iter);
    } else {
        // Standard path: Single request for all iterations
        for (int i = 0; i < num_iter; ++i) {
            if (iter_barrier_enabled()) {
                #pragma omp barrier
                #pragma omp master
                obj_trace_iter_marker("nixlbench_iter_start", i);
            }
            xferBenchTimer iter_timer;
            if (op == NIXL_WRITE) stage_d2h(local_desc[0].len);
            nixl_status_t rc = execSingleTransfer(agent, req, timer, thread_stats, i);

            if (__builtin_expect(rc != NIXL_SUCCESS, 0)) {
                std::cout << "NIXL Xfer failed with status: " << nixlEnumStrings::statusStr(rc)
                          << std::endl;
                agent->releaseXferReq(req);
                stage_cleanup();
                return -1;
            }
            if (op == NIXL_READ) stage_h2d(local_desc[0].len);
            thread_stats.transfer_duration.add(timer.lap());
            thread_stats.iter_duration.add(iter_timer.lap());
            if (iter_barrier_enabled()) {
                #pragma omp barrier
                #pragma omp master
                obj_trace_iter_marker("nixlbench_iter_end", i);
            }
        }

        // Release request once after all iterations
        if (__builtin_expect(agent->releaseXferReq(req) != NIXL_SUCCESS, 0)) {
            std::cout << "NIXL releaseXferReq failed" << std::endl;
            stage_cleanup();
            return -1;
        }
    }

    stage_cleanup();
    return 0;
}

static int
execTransfer(nixlAgent *agent,
             const std::vector<std::vector<xferBenchIOV>> &local_iovs,
             const std::vector<std::vector<xferBenchIOV>> &remote_iovs,
             const nixl_xfer_op_t op,
             const int num_iter,
             const int num_threads,
             xferBenchStats &stats,
             const std::vector<std::vector<std::string>> &prepop_keys = {},
             const int prepop_iter_offset = 0,
             const std::vector<uint64_t> &prepop_base_dev_ids = {}) {
    int ret = 0;
    stats.clear();

    xferBenchTimer total_timer;
#pragma omp parallel num_threads(num_threads)
    {
        xferBenchStats thread_stats;
        thread_stats.reserve(num_iter);
        xferBenchTimer timer;
        const int tid = omp_get_thread_num();
        const auto &local_iov = local_iovs[tid];
        const auto &remote_iov = remote_iovs[tid];

        // Prepare transfer descriptors
        nixl_xfer_dlist_t local_desc(GET_SEG_TYPE(true));
        nixl_xfer_dlist_t remote_desc(GET_SEG_TYPE(false));
        prepareTransferDescriptors(local_desc, remote_desc, local_iov, remote_iov);

        // Setup transfer parameters
        nixl_opt_args_t params;
        std::string target = xferBenchConfig::isStorageBackend() ? "initiator" : "target";
        if (!xferBenchConfig::isStorageBackend()) {
            params.notif = "0xBEEF";
        }

        const std::vector<std::string> *thread_keys =
            (!prepop_keys.empty() && tid < (int)prepop_keys.size()) ?
                &prepop_keys[tid] : nullptr;

        // Override prepop_base_dev_id from the stored per-thread value
        // instead of relying on remote_desc[0].devId (which may be wrong).
        const uint64_t thread_base_dev_id =
            (!prepop_base_dev_ids.empty() && tid < (int)prepop_base_dev_ids.size())
                ? prepop_base_dev_ids[tid] : 0;
        if (thread_keys && !thread_keys->empty()) {
            remote_desc[0].devId = thread_base_dev_id;
        }

        const int result = execTransferIterations(agent,
                                                  op,
                                                  local_desc,
                                                  remote_desc,
                                                  target,
                                                  params,
                                                  num_iter,
                                                  timer,
                                                  thread_stats,
                                                  xferBenchConfig::recreate_xfer,
                                                  thread_keys,
                                                  prepop_iter_offset);

        if (__builtin_expect(result != 0, 0)) {
            ret = result;
        }

#pragma omp critical
        { stats.add(thread_stats); }
    }

    const nixlTime::us_t total_duration = total_timer.lap();
    stats.total_duration.add(total_duration);
    return ret;
}

std::variant<xferBenchStats, int>
xferBenchNixlWorker::transfer(size_t block_size,
                              const std::vector<std::vector<xferBenchIOV>> &local_iovs,
                              const std::vector<std::vector<xferBenchIOV>> &remote_iovs) {
    int num_iter = xferBenchConfig::num_iter / xferBenchConfig::num_threads;
    int skip = xferBenchConfig::warmup_iter / xferBenchConfig::num_threads;
    xferBenchStats stats;
    int ret = 0;
    nixl_xfer_op_t xfer_op = XFERBENCH_OP_READ == xferBenchConfig::op_type ? NIXL_READ : NIXL_WRITE;
    // int completion_flag = 1;

    // Reduce skip by 10x for large block sizes
    if (block_size > LARGE_BLOCK_SIZE) {
        skip /= xferBenchConfig::large_blk_iter_ftr;
        num_iter /= xferBenchConfig::large_blk_iter_ftr;
    }

    // One-shot config marker so post-processing knows num_threads + the rest
    // of the run-wide knobs without parsing the CLI separately.
    obj_trace_emit_config(
        xferBenchConfig::num_threads, xferBenchConfig::num_iter,
        xferBenchConfig::warmup_iter, xferBenchConfig::start_batch_size,
        xferBenchConfig::batch_mode, xferBenchConfig::batch_size,
        xferBenchConfig::num_threads_batch,
        (xfer_op == NIXL_READ) ? "READ" : "WRITE");

    int prepop_start = xferBenchConfig::obj_prepop_start;
    if (skip > 0) {
        ret = execTransfer(agent, local_iovs, remote_iovs, xfer_op, skip,
                           xferBenchConfig::num_threads, stats, prepop_keys_, prepop_start,
                           prepop_base_dev_ids_);
        if (ret < 0) {
            return std::variant<xferBenchStats, int>(ret);
        }
    }

    // Synchronize to ensure all processes have completed the warmup (iter and polling)
    synchronize();

    stats.clear();

    ret = execTransfer(agent, local_iovs, remote_iovs, xfer_op, num_iter,
                       xferBenchConfig::num_threads, stats, prepop_keys_, prepop_start + skip,
                       prepop_base_dev_ids_);
    if (ret < 0) {
        return std::variant<xferBenchStats, int>(ret);
    }

    synchronize();
    return std::variant<xferBenchStats, int>(stats);
}

void
xferBenchNixlWorker::poll(size_t block_size) {
    nixl_notifs_t notifs;
    nixl_status_t status;
    int skip = 0, num_iter = 0, total_iter = 0;

    skip = xferBenchConfig::warmup_iter;
    num_iter = xferBenchConfig::num_iter;
    // Reduce skip by 10x for large block sizes
    if (block_size > LARGE_BLOCK_SIZE) {
        skip /= xferBenchConfig::large_blk_iter_ftr;
        num_iter /= xferBenchConfig::large_blk_iter_ftr;
    }
    total_iter = skip + num_iter;

    /* Ensure warmup is done*/
    do {
        status = agent->getNotifs(notifs);
    } while (status == NIXL_SUCCESS && skip != int(notifs["initiator"].size()));
    synchronize();

    /* Polling for actual iterations*/
    do {
        status = agent->getNotifs(notifs);
    } while (status == NIXL_SUCCESS && total_iter != int(notifs["initiator"].size()));
    synchronize();
}

int
xferBenchNixlWorker::synchronizeStart() {
    // For storage backends without ETCD, no synchronization needed
    if (xferBenchConfig::isStorageBackend() && xferBenchConfig::etcd_endpoints.empty()) {
        std::cout << "Single instance storage backend - no synchronization needed" << std::endl;
        return 0;
    }

    if (IS_PAIRWISE_AND_SG()) {
        std::cout << "Waiting for all processes to start... (expecting " << rt->getSize()
                  << " total: " << xferBenchConfig::num_initiator_dev << " initiators and "
                  << xferBenchConfig::num_target_dev << " targets)" << std::endl;
    } else {
        std::cout << "Waiting for all processes to start... (expecting " << rt->getSize()
                  << " total" << std::endl;
    }
    if (rt) {
        int ret = rt->barrier("start_barrier");
        if (ret != 0) {
            std::cerr << "Failed to synchronize at start barrier" << std::endl;
            return -1;
        }
        std::cout << "All processes are ready to proceed" << std::endl;
        return 0;
    }
    return -1;
}
