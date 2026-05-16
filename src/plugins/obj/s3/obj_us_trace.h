// SPDX-License-Identifier: Apache-2.0
// Microsecond-resolution wallclock event log for the NIXL OBJ S3 plugin.
// Env-gated: set NIXL_OBJ_TRACE=1 and writes go to /tmp/nixl_obj_trace_us.log
// (or NIXL_OBJ_TRACE_FILE if set). Output format matches the RGW probes
// (rgw_us_trace.h) so they can be merged into one cross-node timeline.
#ifndef NIXL_OBJ_US_TRACE_H
#define NIXL_OBJ_US_TRACE_H

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace nixl_obj_us_trace_ns {

inline FILE* fp() {
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

inline bool enabled() {
    static const bool en = (std::getenv("NIXL_OBJ_TRACE") != nullptr);
    return en;
}

inline void emit(const char* event) {
    if (!enabled()) return;
    FILE* f = fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld 0 %s\n", ts, tid, event);
}

inline void emit_r(const char* event, uint64_t req_id) {
    if (!enabled()) return;
    FILE* f = fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %lu %s\n", ts, tid, (unsigned long)req_id, event);
}

}  // namespace nixl_obj_us_trace_ns

#define NIXL_OBJ_US(name)        ::nixl_obj_us_trace_ns::emit(name)
#define NIXL_OBJ_US_R(name, rid) ::nixl_obj_us_trace_ns::emit_r(name, rid)

#endif
