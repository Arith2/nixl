/*
 * prepop_flat_hashoid.c — fast libdaos-direct prepop for s3rdma_direct /
 *                        s3rdma_batch hashoid READ tests.
 *
 * Why this exists: nixlbench's s3rdma_direct WRITE goes through 1 HTTP +
 * 1 daos_obj_update per chunk, which serializes through RGW and is the
 * bottleneck of cell prepop (T=16 takes ~21 min for 850K chunks). This
 * tool bypasses NIXL and RGW entirely — it's libdaos calls in tight loop
 * with -j parallelism, writing to the SAME OIDs the s3_split_plane plugin's
 * hashoid READ path will look up:
 *
 *     oid.lo = hashoid_oid_lo(fnv1a_32(key))
 *     oid.hi = hashoid_oid_hi_user(T, IOD, BS)
 *     dkey  = "d"   akey = "a"   recx = [0, BS)
 *
 * Byte-identical to s3_split_plane/engine_impl.cpp's `do_hashoid_op`
 * (and to the s3rdma_batch hashoid READ path), so the data this tool
 * writes is exactly what the read benchmark fetches back.
 *
 * Usage:
 *   prepop_flat_hashoid <pool> <cont> <keyfile> <BS> <num_threads_for_hi>
 *                       <iodepth_for_hi> [<n_workers>]
 *
 *     pool, cont          — DAOS pool/cont labels (already created)
 *     keyfile             — path to /tmp/kvbench_*.txt (one key per line)
 *     BS                  — bytes per chunk (= LAYER_SLICE)
 *     num_threads_for_hi  — value of nixlbench's `-num_threads` during the
 *                           READ (1 in the figure-12 batch config). Goes
 *                           into oid.hi via hashoid_oid_hi_user(T,IOD,BS).
 *     iodepth_for_hi      — value of nixlbench's `-iodepth` during the
 *                           READ (1 in the figure-12 batch config).
 *     n_workers           — pthreads issuing async daos_obj_update; default 16.
 *
 * Output: progress every 64K chunks; final "WROTE N keys in T sec
 * (X.X GB/s effective)".
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <stdatomic.h>
#include <sys/stat.h>

#include <daos.h>
#include "../../src/plugins/obj/daos_direct/hashoid.h"

#define DKEY  "d"
#define AKEY  "a"

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* FNV-1a 32-bit, byte-identical to split_hashoid_fnv1a_32 in
 * src/plugins/obj/s3_split_plane/engine_impl.cpp. */
static inline uint32_t fnv1a_32(const char *s, size_t n) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; ++i) { h ^= (unsigned char)s[i]; h *= 16777619u; }
    return h;
}

struct shared_ctx {
    daos_handle_t  coh;
    char          *bufs;        /* pre-allocated, n_workers * BS */
    size_t         BS;
    int            T;
    int            IOD;
    char         **keys;        /* array of keys */
    size_t         n_keys;
    atomic_size_t  next_idx;
    atomic_size_t  done_count;
    int            n_workers;
};

struct worker_arg {
    struct shared_ctx *ctx;
    int                wid;
};

static void *worker_main(void *arg) {
    struct worker_arg *wa = (struct worker_arg*)arg;
    struct shared_ctx *ctx = wa->ctx;
    char *buf = ctx->bufs + (size_t)wa->wid * ctx->BS;
    size_t err_count = 0;

    while (1) {
        size_t i = atomic_fetch_add_explicit(&ctx->next_idx, 1,
                                              memory_order_relaxed);
        if (i >= ctx->n_keys) break;

        const char *key = ctx->keys[i];
        size_t klen = strlen(key);

        daos_obj_id_t oid;
        oid.lo = hashoid_oid_lo((int)fnv1a_32(key, klen));
        oid.hi = hashoid_oid_hi_user(ctx->T, ctx->IOD, ctx->BS);

        int rc = daos_obj_generate_oid(ctx->coh, &oid,
                                       DAOS_OT_MULTI_HASHED, OC_SX, 0, 0);
        if (rc != 0) {
            if (err_count++ < 5)
                fprintf(stderr, "[w%d] generate_oid key=%s rc=%d\n",
                        wa->wid, key, rc);
            continue;
        }

        daos_handle_t oh = DAOS_HDL_INVAL;
        rc = daos_obj_open(ctx->coh, oid, DAOS_OO_RW, &oh, NULL);
        if (rc != 0) {
            if (err_count++ < 5)
                fprintf(stderr, "[w%d] obj_open key=%s rc=%d\n",
                        wa->wid, key, rc);
            continue;
        }

        daos_key_t dkey;
        d_iov_set(&dkey, (void*)DKEY, sizeof(DKEY) - 1);
        daos_recx_t recx = { .rx_idx = 0, .rx_nr = ctx->BS };
        daos_iod_t iod = { 0 };
        d_iov_set(&iod.iod_name, (void*)AKEY, sizeof(AKEY) - 1);
        iod.iod_type  = DAOS_IOD_ARRAY;
        iod.iod_size  = 1;
        iod.iod_nr    = 1;
        iod.iod_recxs = &recx;

        d_iov_t iov;
        d_iov_set(&iov, buf, ctx->BS);
        d_sg_list_t sgl = { .sg_nr = 1, .sg_iovs = &iov };

        rc = daos_obj_update(oh, DAOS_TX_NONE, 0, &dkey, 1,
                             &iod, &sgl, NULL);
        daos_obj_close(oh, NULL);
        if (rc != 0) {
            if (err_count++ < 5)
                fprintf(stderr, "[w%d] obj_update key=%s rc=%d\n",
                        wa->wid, key, rc);
            continue;
        }

        size_t prev = atomic_fetch_add_explicit(&ctx->done_count, 1,
                                                 memory_order_relaxed);
        if ((prev + 1) % 65536 == 0) {
            fprintf(stderr, "  [prepop] %zu / %zu keys\n",
                    prev + 1, ctx->n_keys);
        }
    }

    if (err_count > 0)
        fprintf(stderr, "[w%d] %zu errors\n", wa->wid, err_count);
    return NULL;
}

#define CHK(call) do { int _r = (call); \
    if (_r != 0) { fprintf(stderr, #call " failed rc=%d\n", _r); return 1; } \
} while (0)

int main(int argc, char **argv) {
    if (argc < 7) {
        fprintf(stderr,
            "Usage: %s <pool> <cont> <keyfile> <BS> "
            "<T_for_hi> <IOD_for_hi> [<n_workers=16>]\n", argv[0]);
        return 2;
    }
    const char *pool_label = argv[1];
    const char *cont_label = argv[2];
    const char *keyfile    = argv[3];
    size_t      BS         = (size_t)strtoull(argv[4], NULL, 10);
    int         T          = atoi(argv[5]);
    int         IOD        = atoi(argv[6]);
    int         n_workers  = (argc >= 8) ? atoi(argv[7]) : 16;
    if (n_workers < 1) n_workers = 1;
    if (n_workers > 64) n_workers = 64;

    /* Load all keys into memory (they're small — at most ~1 MiB total). */
    FILE *fp = fopen(keyfile, "r");
    if (!fp) { perror(keyfile); return 1; }
    size_t cap = 1 << 14, n = 0;
    char **keys = malloc(cap * sizeof(*keys));
    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        size_t L = strlen(line);
        while (L > 0 && (line[L-1] == '\n' || line[L-1] == '\r')) line[--L] = 0;
        if (L == 0) continue;
        if (n == cap) { cap *= 2; keys = realloc(keys, cap * sizeof(*keys)); }
        keys[n] = strdup(line);
        n++;
    }
    fclose(fp);
    fprintf(stderr, "[prepop] loaded %zu keys from %s\n", n, keyfile);

    /* DAOS init + connect. */
    CHK(daos_init());
    daos_handle_t poh = DAOS_HDL_INVAL, coh = DAOS_HDL_INVAL;
    CHK(daos_pool_connect(pool_label, NULL, DAOS_PC_RW, &poh, NULL, NULL));
    CHK(daos_cont_open(poh, cont_label, DAOS_COO_RW, &coh, NULL, NULL));

    /* Pre-allocate per-worker buffers (filled with 0xAB pattern). */
    char *bufs = aligned_alloc(4096, (size_t)n_workers * BS);
    if (!bufs) { fprintf(stderr, "alloc %zu B failed\n",
                         (size_t)n_workers * BS); return 1; }
    memset(bufs, 0xAB, (size_t)n_workers * BS);

    struct shared_ctx ctx = {
        .coh        = coh,
        .bufs       = bufs,
        .BS         = BS,
        .T          = T,
        .IOD        = IOD,
        .keys       = keys,
        .n_keys     = n,
        .n_workers  = n_workers,
    };
    atomic_init(&ctx.next_idx, 0);
    atomic_init(&ctx.done_count, 0);

    pthread_t *tids = malloc(n_workers * sizeof(*tids));
    struct worker_arg *wargs = malloc(n_workers * sizeof(*wargs));

    fprintf(stderr, "[prepop] %s/%s  BS=%zu  hashoid_hi(T=%d,IOD=%d)  workers=%d\n",
            pool_label, cont_label, BS, T, IOD, n_workers);

    double t0 = now_sec();
    for (int w = 0; w < n_workers; ++w) {
        wargs[w].ctx = &ctx;
        wargs[w].wid = w;
        if (pthread_create(&tids[w], NULL, worker_main, &wargs[w]) != 0) {
            fprintf(stderr, "pthread_create w=%d failed\n", w);
            return 1;
        }
    }
    for (int w = 0; w < n_workers; ++w) pthread_join(tids[w], NULL);
    double dt = now_sec() - t0;

    size_t total_bytes = atomic_load(&ctx.done_count) * BS;
    double gbps = (double)total_bytes / dt / 1e9;
    fprintf(stderr,
        "[prepop] WROTE %zu keys (%.2f GiB) in %.2f sec  (%.2f GB/s effective)\n",
        atomic_load(&ctx.done_count),
        (double)total_bytes / (1024.0 * 1024.0 * 1024.0), dt, gbps);

    daos_cont_close(coh, NULL);
    daos_pool_disconnect(poh, NULL);
    daos_fini();

    free(bufs);
    for (size_t i = 0; i < n; ++i) free(keys[i]);
    free(keys); free(tids); free(wargs);
    return 0;
}
