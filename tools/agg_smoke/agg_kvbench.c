/*
 * agg_kvbench.c — s3rdma_agg benchmark modeled after s3rdma_batch:
 *
 *   Write 32 aggregates × 256 chunks × 64 KiB  (= 512 MiB unique)
 *     Represents 32 layers × 4K-token context × 16 tokens/chunk
 *     = 256 chunks per layer, each 64 KiB.
 *
 *   Scrub cache by writing 4096 × 64 MiB (= 256 GiB) to a disjoint
 *   scratch container via direct daos_obj_update (no RGW/DFS) — this
 *   mirrors "daos hashoid rdma" scrub (hashed per-chunk OIDs, RDMA
 *   bulk, direct DAOS path).
 *
 *   Issue N_LAYERS aggregate reads in s3rdma_batch style: one logical
 *   postXfer-equivalent, with PIPELINE_DEPTH aggregate RPCs in flight
 *   concurrently via daos_event_t + daos_eq_poll. Each aggregate is a
 *   single daos_obj_fetch on the layer's aggregate OID, with the
 *   nixl-agg recipe set in TLS before the fetch.
 *
 *   Report:
 *     - per-layer latency (min/p50/p99/max/mean)
 *     - total-load latency (all 32 layers done)
 *     - TTFL (time to first layer completed) and TFL (total first load)
 *     - aggregate throughput (total bytes / total wall)
 *     - byte integrity check on every aggregate
 *
 * Build: see Makefile / agg_cold_read.c for dep flags.
 * Usage:
 *   ./agg_kvbench <pool> <cont> [N_LAYERS=32] [N_CHUNKS=256]
 *                 [CHUNK_BS=65536] [PIPE_DEPTH=4]
 *                 [SCRUB_MIB=262144] [SCRUB_CHUNK_MIB=64]
 *                 [SCRUB_CONT=lmcache_scrub]
 *                 [IOD_T=8] [IOD_I=4]        # recipe agg_p2: T=iod_t, IOD=iod_i
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>

#include <daos.h>
#include <daos_obj.h>
#include <daos/daos_nixl_req_id.h>

/* Shared OID derivation — same header is symlinked into agg_sidecar so the
 * engine-hashoid bench thread, this client, and the NIXL plugin all map
 * (layer, chunk) → OID identically. Layered: random across layers, sequential
 * within a layer (so server-side aggregate handler can compute
 * chunk_oid_i.lo = agg_oid.lo + i). */
#include "../../src/plugins/obj/daos_direct/hashoid.h"

#define CHECK(rc) do { int _r = (rc); if (_r != 0) { \
    fprintf(stderr, "%s:%d " #rc " failed: rc=%d\n", __FILE__, __LINE__, _r); \
    exit(2); } } while (0)

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000ULL;
}

static int ulcmp(const void *a, const void *b) {
    unsigned long x = *(const unsigned long *)a;
    unsigned long y = *(const unsigned long *)b;
    return (x > y) - (x < y);
}

/* Pattern for chunk (layer, chunk_idx, byte_off): deterministic 1-byte
 * signature; we byte-verify offset 0 of every chunk only (fast + enough
 * to catch mis-placement bugs). */
static inline unsigned char
pat(int layer, int chunk) {
    return (unsigned char)(((layer & 0x7F) << 1) ^ (chunk & 0xFF));
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <pool> <cont> [N_LAYERS=32] [N_CHUNKS=256] "
                        "[CHUNK_BS=65536] [PIPE_DEPTH=4] [SCRUB_MIB=262144] "
                        "[SCRUB_CHUNK_MIB=64] [SCRUB_CONT=lmcache_scrub] "
                        "[IOD_T=8] [IOD_I=4]\n", argv[0]);
        return 1;
    }
    const char *pool = argv[1];
    const char *cont = argv[2];
    int N_LAYERS    = argc > 3 ? atoi(argv[3]) : 32;
    int N_CHUNKS    = argc > 4 ? atoi(argv[4]) : 256;
    int CHUNK_BS    = argc > 5 ? atoi(argv[5]) : 65536;
    int PIPE_DEPTH  = argc > 6 ? atoi(argv[6]) : 4;
    long SCRUB_MIB  = argc > 7 ? atol(argv[7]) : (long)(256 * 1024);
    long SCRUB_CBS  = argc > 8 ? atol(argv[8]) : 64;
    const char *SCRUB_CONT = argc > 9 ? argv[9] : "lmcache_scrub";
    int IOD_T       = argc > 10 ? atoi(argv[10]) : 8;
    int IOD_I       = argc > 11 ? atoi(argv[11]) : 4;
    /* SKIP_WRITE=1 assumes aggregates are already written at deterministic
     * base_lo. SKIP_SCRUB=1 trusts the BIO cache is already cold. Both
     * default via env vars so the 2-client simultaneous test doesn't
     * re-write or re-scrub. SKIP_VERIFY=1 omits the per-layer
     * byte-integrity spot-check from the timed READ window (byte-verify
     * reads ~256 sparse 64 KiB offsets across 16 MiB, adding ~0.2 ms per
     * aggregate to TFL wall). Paper-grade measurements want this out of
     * the critical path. */
    int SKIP_WRITE  = getenv("SKIP_WRITE")  ? atoi(getenv("SKIP_WRITE"))  : 0;
    int SKIP_SCRUB  = getenv("SKIP_SCRUB")  ? atoi(getenv("SKIP_SCRUB"))  : 0;
    int SKIP_VERIFY = getenv("SKIP_VERIFY") ? atoi(getenv("SKIP_VERIFY")) : 0;
    int SKIP_READ   = getenv("SKIP_READ")   ? atoi(getenv("SKIP_READ"))   : 0;

    size_t agg_bytes   = (size_t)N_CHUNKS * CHUNK_BS;
    size_t scrub_total = (size_t)SCRUB_MIB * 1024 * 1024;
    size_t scrub_chunk = (size_t)SCRUB_CBS * 1024 * 1024;
    long   scrub_iter  = (long)(scrub_total / scrub_chunk);

    printf("agg_kvbench\n");
    printf("  pool/cont       : %s / %s\n", pool, cont);
    printf("  write           : %d layers x %d chunks x %d B  = %.1f MiB\n",
           N_LAYERS, N_CHUNKS, CHUNK_BS,
           (double)N_LAYERS * agg_bytes / (1024.0 * 1024.0));
    printf("  aggregate       : %zu B (%.1f MiB) per layer\n",
           agg_bytes, (double)agg_bytes / (1024.0 * 1024.0));
    printf("  scrub           : %ld MiB in %ld MiB chunks (%ld writes)  via %s\n",
           SCRUB_MIB, SCRUB_CBS, scrub_iter, SCRUB_CONT);
    printf("  pipeline depth  : %d aggregate RPCs in flight\n", PIPE_DEPTH);
    printf("  recipe IOD      : T=%d IOD=%d (placed in agg_p2)\n", IOD_T, IOD_I);
    printf("\n");

    CHECK(daos_init());

    daos_handle_t poh, coh, coh_scrub;
    CHECK(daos_pool_connect(pool, NULL, DAOS_PC_RW, &poh, NULL, NULL));
    CHECK(daos_cont_open(poh, cont, DAOS_COO_RW, &coh, NULL, NULL));

    /* (Re)create scrub container — fresh each run so destroy at end
     * reclaims all NVMe space. We also need it open for the WARMUP_READ
     * phase, which reads back DEAD-prefix OIDs from the scrub container
     * to prime the RPC pipeline without touching the prepop. */
    int warmup_n_check  = getenv("WARMUP_READ_N")  ? atoi(getenv("WARMUP_READ_N"))  : 0;
    int needs_scrub_cont = !SKIP_SCRUB || warmup_n_check > 0;
    if (needs_scrub_cont) {
        int rc_tmp = daos_cont_create_with_label(poh, SCRUB_CONT, NULL, NULL, NULL);
        if (rc_tmp != 0 && rc_tmp != -DER_EXIST) {
            fprintf(stderr, "scrub-cont create rc=%d\n", rc_tmp); exit(2);
        }
        CHECK(daos_cont_open(poh, SCRUB_CONT, DAOS_COO_RW, &coh_scrub, NULL, NULL));
    } else {
        coh_scrub.cookie = 0;
    }

    /* ---------- WRITE phase ---------- */
    uint64_t *base_lo = calloc(N_LAYERS, sizeof(uint64_t));
    uint64_t  chunk_hi = 0;    /* populated from first chunk */
    /* dkey/akey "d"/"a" — same convention as NIXL split-plane plugin's
     * hashoid path (see kSplitHashoidDkey/Akey). agg_sidecar's bench
     * thread uses uint64(1)/char('0') (dc_array compute_dkey
     * convention) and is intentionally NOT used as a prepop source
     * here; if engine-hashoid prepop is later needed, that path's
     * dkey/akey will need to be aligned to "d"/"a" too. */
    daos_key_t dk; d_iov_set(&dk, (void *)"d", 1);

    /* Layered hashoid: random across layers, sequential within. The
     * server-side aggregate handler relies on chunk_oid_i.lo = base + i
     * so the per-chunk write loop below uses base_lo[l] + c directly,
     * which equals hashoid_oid_lo_layered(l, c) for c < 2^HASHOID_CHUNK_BITS. */
    for (int l = 0; l < N_LAYERS; l++)
        base_lo[l] = hashoid_layer_base(l);

    /* Always generate first chunk OID so we know chunk_hi (= user-bits
     * | class-bits OR'd in by daos_obj_generate_oid) — even if we skip
     * the write. User bits encode (T, IOD, BS) so disjoint recipes don't
     * collide on the same lo. NIXL plugin's agg mode sets the SAME hi
     * pre-generate_oid → resulting hi matches byte-for-byte. */
    {
        daos_obj_id_t oid = { .lo = base_lo[0],
                              .hi = hashoid_oid_hi_user(IOD_T, IOD_I, CHUNK_BS) };
        CHECK(daos_obj_generate_oid(coh, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0));
        chunk_hi = oid.hi;
    }

    if (!SKIP_WRITE) {
        printf("[WRITE] %d x %d x %d B...\n", N_LAYERS, N_CHUNKS, CHUNK_BS);
        uint64_t tw0 = now_us();
        char *chunk_buf = malloc(CHUNK_BS);
        if (!chunk_buf) { fprintf(stderr, "oom\n"); exit(2); }

        for (int l = 0; l < N_LAYERS; l++) {
            for (int c = 0; c < N_CHUNKS; c++) {
                daos_obj_id_t oid = { .lo = base_lo[l] + c,
                                      .hi = hashoid_oid_hi_user(IOD_T, IOD_I, CHUNK_BS) };
                CHECK(daos_obj_generate_oid(coh, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0));
                daos_handle_t oh;
                CHECK(daos_obj_open(coh, oid, DAOS_OO_RW, &oh, NULL));

                memset(chunk_buf, (int)pat(l, c), CHUNK_BS);
                d_iov_t iov; d_iov_set(&iov, chunk_buf, CHUNK_BS);
                d_sg_list_t sgl = { .sg_nr = 1, .sg_nr_out = 1, .sg_iovs = &iov };
                daos_recx_t rx = { .rx_idx = 0, .rx_nr = CHUNK_BS };
                daos_iod_t iod = { 0 };
                d_iov_set(&iod.iod_name, (void *)"a", 1);
                iod.iod_type = DAOS_IOD_ARRAY; iod.iod_size = 1;
                iod.iod_nr = 1; iod.iod_recxs = &rx;

                CHECK(daos_obj_update(oh, DAOS_TX_NONE, 0, &dk, 1, &iod, &sgl, NULL));
                CHECK(daos_obj_close(oh, NULL));
            }
            if ((l + 1) % 8 == 0)
                printf("  [WRITE] %d/%d layers\n", l + 1, N_LAYERS);
        }
        uint64_t tw1 = now_us();
        printf("[WRITE] done: %.1f MiB in %lu us (%.2f GB/s)\n\n",
               (double)N_LAYERS * agg_bytes / (1024.0 * 1024.0),
               tw1 - tw0,
               (double)N_LAYERS * agg_bytes / (double)(tw1 - tw0) / 1e3);

        free(chunk_buf);
    } else {
        printf("[WRITE] SKIPPED (SKIP_WRITE=1)\n\n");
    }

    /* ---------- SCRUB phase ---------- */
    if (SKIP_SCRUB || scrub_iter == 0) {
        printf("[SCRUB] SKIPPED (SKIP_SCRUB=%d scrub_iter=%ld)\n\n",
               SKIP_SCRUB, scrub_iter);
        goto skip_scrub;
    }
    printf("[SCRUB] %ld x %ld MiB = %ld MiB to container %s...\n",
           scrub_iter, SCRUB_CBS, SCRUB_MIB, SCRUB_CONT);
    char *scrub_buf = aligned_alloc(4096, scrub_chunk);
    if (!scrub_buf) { fprintf(stderr, "scrub oom\n"); exit(2); }
    memset(scrub_buf, 0xA5, scrub_chunk);

    uint64_t ts0 = now_us();
    for (long i = 0; i < scrub_iter; i++) {
        daos_obj_id_t oid = { .lo = 0xDEAD0000ULL + i, .hi = 0 };
        CHECK(daos_obj_generate_oid(coh_scrub, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0));
        daos_handle_t oh;
        CHECK(daos_obj_open(coh_scrub, oid, DAOS_OO_RW, &oh, NULL));

        d_iov_t iov; d_iov_set(&iov, scrub_buf, scrub_chunk);
        d_sg_list_t sgl = { .sg_nr = 1, .sg_nr_out = 1, .sg_iovs = &iov };
        daos_recx_t rx = { .rx_idx = 0, .rx_nr = scrub_chunk };
        daos_iod_t iod = { 0 };
        d_iov_set(&iod.iod_name, (void *)"a", 1);
        iod.iod_type = DAOS_IOD_ARRAY; iod.iod_size = 1;
        iod.iod_nr = 1; iod.iod_recxs = &rx;

        CHECK(daos_obj_update(oh, DAOS_TX_NONE, 0, &dk, 1, &iod, &sgl, NULL));
        CHECK(daos_obj_close(oh, NULL));

        if ((i + 1) % 64 == 0) {
            uint64_t tsx = now_us();
            printf("  [SCRUB] %ld/%ld (%.2f GB/s)\n",
                   i + 1, scrub_iter,
                   (double)(i + 1) * scrub_chunk / (double)(tsx - ts0) / 1e3);
        }
    }
    uint64_t ts1 = now_us();
    printf("[SCRUB] done: %ld MiB in %lu us (%.2f GB/s)\n\n",
           SCRUB_MIB, ts1 - ts0,
           (double)scrub_total / (double)(ts1 - ts0) / 1e3);
    free(scrub_buf);
skip_scrub:;

    /* ---------- WARMUP_READ phase ----------
     *
     * Reads N OIDs from the scrub container (just-written DEAD-prefix
     * OIDs) at the same block size as the scrub. Purpose: prime the
     * client→server RPC path (Mercury QP, completion thread, ULT pool)
     * BEFORE the timed agg read, WITHOUT touching the prepop OIDs in
     * the lmcache container. The lmcache prepop stays cold (the scrub
     * already evicted it from BIO cache) so the subsequent timed agg
     * read still measures a true cold KV load.
     *
     * Activated by env vars WARMUP_READ_N (count) and WARMUP_READ_BS
     * (bytes/op). Both must be > 0 and a scrub container must be open.
     * Default off so existing runs are unaffected. */
    int    warmup_n  = getenv("WARMUP_READ_N")  ? atoi(getenv("WARMUP_READ_N"))  : 0;
    long   warmup_bs = getenv("WARMUP_READ_BS") ? atol(getenv("WARMUP_READ_BS")) : 0;
    if (warmup_n > 0 && warmup_bs > 0 && coh_scrub.cookie != 0) {
        printf("[WARMUP] read %d x %ld B (%ld MiB) from %s...\n",
               warmup_n, warmup_bs, warmup_bs / 1024 / 1024, SCRUB_CONT);
        char *warm_buf = aligned_alloc(4096, warmup_bs);
        if (!warm_buf) { fprintf(stderr, "warm oom\n"); exit(2); }
        uint64_t tw0 = now_us();
        for (int i = 0; i < warmup_n; i++) {
            daos_obj_id_t oid = { .lo = 0xDEAD0000ULL + i, .hi = 0 };
            CHECK(daos_obj_generate_oid(coh_scrub, &oid, DAOS_OT_MULTI_HASHED, OC_SX, 0, 0));
            daos_handle_t oh;
            CHECK(daos_obj_open(coh_scrub, oid, DAOS_OO_RO, &oh, NULL));

            d_iov_t iov; d_iov_set(&iov, warm_buf, warmup_bs);
            d_sg_list_t sgl = { .sg_nr = 1, .sg_nr_out = 1, .sg_iovs = &iov };
            daos_recx_t rx = { .rx_idx = 0, .rx_nr = warmup_bs };
            daos_iod_t iod = { 0 };
            d_iov_set(&iod.iod_name, (void *)"a", 1);
            iod.iod_type = DAOS_IOD_ARRAY; iod.iod_size = 1;
            iod.iod_nr = 1; iod.iod_recxs = &rx;

            CHECK(daos_obj_fetch(oh, DAOS_TX_NONE, 0, &dk, 1, &iod, &sgl, NULL, NULL));
            CHECK(daos_obj_close(oh, NULL));

            if ((i + 1) % 64 == 0) {
                uint64_t twx = now_us();
                printf("  [WARMUP] %d/%d (%.2f GB/s)\n", i + 1, warmup_n,
                       (double)(i + 1) * warmup_bs / (double)(twx - tw0) / 1e3);
            }
        }
        uint64_t tw1 = now_us();
        printf("[WARMUP] done: %d x %ld MiB in %lu us (%.2f GB/s)\n\n",
               warmup_n, warmup_bs / 1024 / 1024, tw1 - tw0,
               (double)warmup_n * warmup_bs / (double)(tw1 - tw0) / 1e3);
        free(warm_buf);
    }

    /* ---------- READ phase: pipelined aggregate fetches ----------
     *
     * Mimics s3rdma_batch: single "postXfer" call dispatches all
     * N_LAYERS aggregate reads; executor pool = PIPE_DEPTH async slots.
     * We use daos_event_t per slot + daos_eq_poll to wait for any-done. */

    /* Declared up here so the SKIP_READ goto past the read phase still
     * lands in the cleanup block with a valid free()-able pointer and
     * a defined return value. */
    unsigned long *lat_us = NULL;
    int            bad    = 0;

    if (SKIP_READ) {
        printf("[READ] SKIPPED (SKIP_READ=1)\n");
        goto skip_read_phase;
    }
    printf("[READ] %d aggregates, pipeline_depth=%d (skip_verify=%d)...\n",
           N_LAYERS, PIPE_DEPTH, SKIP_VERIFY);

    /* Pre-open all N_LAYERS aggregate OHs BEFORE the timed window so the
     * per-layer daos_obj_open RPC doesn't show up on the critical path.
     * OHs are small; holding 32 of them concurrently is fine. */
    daos_handle_t *oh_cache = calloc(N_LAYERS, sizeof(*oh_cache));
    if (!oh_cache) { fprintf(stderr, "oh_cache oom\n"); exit(2); }
    uint64_t to0 = now_us();
    for (int l = 0; l < N_LAYERS; l++) {
        daos_obj_id_t agg_oid = { .lo = base_lo[l], .hi = chunk_hi };
        CHECK(daos_obj_open(coh, agg_oid, DAOS_OO_RO, &oh_cache[l], NULL));
    }
    uint64_t to1 = now_us();
    printf("  [READ pre-open] %d OHs cached in %lu us (%.2f us/open)\n",
           N_LAYERS, to1 - to0, (double)(to1 - to0) / N_LAYERS);

    daos_handle_t eqh;
    CHECK(daos_eq_create(&eqh));

    /* Per-slot state */
    typedef struct slot {
        daos_event_t    ev;
        int             layer;      /* which layer this slot is handling; -1 if idle */
        daos_handle_t   oh;
        char           *buf;
        daos_iod_t      iod;
        daos_recx_t     recx;
        d_iov_t         iov;
        d_sg_list_t     sgl;
        uint64_t        t_submit;
    } slot_t;

    slot_t *slots = calloc(PIPE_DEPTH, sizeof(*slots));
    for (int s = 0; s < PIPE_DEPTH; s++) {
        slots[s].buf = malloc(agg_bytes);
        if (!slots[s].buf) { fprintf(stderr, "slot buf oom\n"); exit(2); }
        slots[s].layer = -1;
        CHECK(daos_event_init(&slots[s].ev, eqh, NULL));
    }

    lat_us = calloc(N_LAYERS, sizeof(*lat_us));
    int            first_done = -1;
    uint64_t       t_first_done = 0;

    int next_to_submit = 0;    /* next layer index to dispatch */
    int completed      = 0;    /* number of layers completed */

    /* Set recipe TLS once for the whole pipeline region. All fetches in
     * this region use the same (N_CHUNKS, CHUNK_BS, IOD_T, IOD_I) recipe. */
    daos_set_nixl_agg_recipe((uint32_t)N_CHUNKS, (uint32_t)CHUNK_BS,
                              (uint32_t)IOD_T, (uint32_t)IOD_I);

    uint64_t tr0 = now_us();

    /* Prime: fill all slots up to PIPE_DEPTH or N_LAYERS (whichever smaller) */
    while (next_to_submit < N_LAYERS &&
           next_to_submit < PIPE_DEPTH) {
        slot_t *s = &slots[next_to_submit];
        int l = next_to_submit;

        s->oh = oh_cache[l];   /* reuse pre-opened handle */

        memset(s->buf, 0xEE, agg_bytes);
        d_iov_set(&s->iov, s->buf, agg_bytes);
        s->sgl.sg_nr = 1; s->sgl.sg_nr_out = 1; s->sgl.sg_iovs = &s->iov;
        s->recx.rx_idx = 0; s->recx.rx_nr = agg_bytes;
        memset(&s->iod, 0, sizeof(s->iod));
        d_iov_set(&s->iod.iod_name, (void *)"a", 1);
        s->iod.iod_type = DAOS_IOD_ARRAY; s->iod.iod_size = 1;
        s->iod.iod_nr = 1; s->iod.iod_recxs = &s->recx;
        s->layer = l;
        s->t_submit = now_us();

        CHECK(daos_obj_fetch(s->oh, DAOS_TX_NONE, 0, &dk, 1,
                             &s->iod, &s->sgl, NULL, &s->ev));
        next_to_submit++;
    }

    /* Drain + refill loop */
    while (completed < N_LAYERS) {
        daos_event_t *done_evs[PIPE_DEPTH];
        int n_done = daos_eq_poll(eqh, 0, DAOS_EQ_WAIT,
                                  PIPE_DEPTH, done_evs);
        if (n_done < 0) {
            fprintf(stderr, "daos_eq_poll rc=%d\n", n_done);
            exit(2);
        }
        for (int i = 0; i < n_done; i++) {
            /* Map event back to slot by pointer arithmetic */
            slot_t *s = NULL;
            for (int k = 0; k < PIPE_DEPTH; k++) {
                if (&slots[k].ev == done_evs[i]) { s = &slots[k]; break; }
            }
            if (!s) { fprintf(stderr, "event->slot mismatch\n"); exit(2); }

            if (done_evs[i]->ev_error != 0) {
                fprintf(stderr, "agg layer=%d ev_error=%d\n",
                        s->layer, done_evs[i]->ev_error);
                exit(2);
            }
            uint64_t t_done = now_us();
            lat_us[s->layer] = (unsigned long)(t_done - s->t_submit);
            if (first_done < 0) { first_done = s->layer; t_first_done = t_done; }

            /* Byte spot-check: first byte of each chunk. Skipped when
             * SKIP_VERIFY=1 so the sparse read-loop doesn't show up on
             * the timed critical path. */
            if (!SKIP_VERIFY) {
                int this_bad = 0;
                for (int c = 0; c < N_CHUNKS; c++) {
                    unsigned char want = pat(s->layer, c);
                    unsigned char got  = (unsigned char)s->buf[(size_t)c * CHUNK_BS];
                    if (got != want) {
                        if (this_bad < 2)
                            fprintf(stderr, "  L%d C%d got=0x%02x want=0x%02x\n",
                                    s->layer, c, got, want);
                        this_bad++;
                    }
                }
                if (this_bad) bad++;
            }

            /* OH is pooled (oh_cache); do NOT close here. */
            s->layer = -1;
            completed++;

            /* Re-queue event for reuse */
            CHECK(daos_event_fini(&done_evs[i][0]));
            CHECK(daos_event_init(&done_evs[i][0], eqh, NULL));

            /* Dispatch next layer into this slot if any remain */
            if (next_to_submit < N_LAYERS) {
                int l = next_to_submit++;
                s->oh = oh_cache[l];  /* reuse pre-opened handle */
                memset(s->buf, 0xEE, agg_bytes);
                d_iov_set(&s->iov, s->buf, agg_bytes);
                s->sgl.sg_nr = 1; s->sgl.sg_nr_out = 1; s->sgl.sg_iovs = &s->iov;
                s->recx.rx_idx = 0; s->recx.rx_nr = agg_bytes;
                memset(&s->iod, 0, sizeof(s->iod));
                d_iov_set(&s->iod.iod_name, (void *)"a", 1);
                s->iod.iod_type = DAOS_IOD_ARRAY; s->iod.iod_size = 1;
                s->iod.iod_nr = 1; s->iod.iod_recxs = &s->recx;
                s->layer = l;
                s->t_submit = now_us();
                CHECK(daos_obj_fetch(s->oh, DAOS_TX_NONE, 0, &dk, 1,
                                     &s->iod, &s->sgl, NULL, &s->ev));
            }
        }
    }
    uint64_t tr1 = now_us();

    daos_set_nixl_agg_recipe(0, 0, 0, 0);

    /* Cleanup slots */
    for (int s = 0; s < PIPE_DEPTH; s++) {
        daos_event_fini(&slots[s].ev);
        free(slots[s].buf);
    }
    free(slots);
    daos_eq_destroy(eqh, 0);

    /* Close all pre-opened OHs (outside timed window) */
    for (int l = 0; l < N_LAYERS; l++)
        daos_obj_close(oh_cache[l], NULL);
    free(oh_cache);

    /* ---------- Report ---------- */
    qsort(lat_us, N_LAYERS, sizeof(*lat_us), ulcmp);
    unsigned long mn = lat_us[0];
    unsigned long p50 = lat_us[N_LAYERS / 2];
    unsigned long p99 = lat_us[(N_LAYERS * 99) / 100];
    unsigned long mx  = lat_us[N_LAYERS - 1];
    double sum = 0;
    for (int i = 0; i < N_LAYERS; i++) sum += (double)lat_us[i];
    double mean_us = sum / (double)N_LAYERS;

    printf("\n=== s3rdma_agg PIPELINED READ RESULTS ===\n");
    printf("N_layers:           %d\n", N_LAYERS);
    printf("agg bytes/layer:    %zu (%.2f MiB)\n",
           agg_bytes, (double)agg_bytes / (1024.0 * 1024.0));
    printf("pipeline depth:     %d\n", PIPE_DEPTH);
    printf("\n");
    printf("Wall time:          %.2f ms (all %d layers)\n",
           (double)(tr1 - tr0) / 1e3, N_LAYERS);
    printf("Throughput:         %.2f GB/s  (sum bytes / wall)\n",
           (double)N_LAYERS * agg_bytes / (double)(tr1 - tr0) / 1e3);
    printf("TTFL:               %.2f ms  (first layer done)\n",
           (double)(t_first_done - tr0) / 1e3);
    printf("TFL (total-load):   %.2f ms  (all 32 layers done)\n",
           (double)(tr1 - tr0) / 1e3);
    printf("\n");
    printf("Per-layer lat us:   min=%lu  p50=%lu  p99=%lu  max=%lu  mean=%.1f\n",
           mn, p50, p99, mx, mean_us);
    printf("Per-layer GB/s:     min=%.2f  p50=%.2f  mean=%.2f  max=%.2f\n",
           (double)agg_bytes / (double)mx / 1e3,
           (double)agg_bytes / (double)p50 / 1e3,
           (double)agg_bytes / mean_us / 1e3,
           (double)agg_bytes / (double)mn / 1e3);
    printf("\n");
    printf("Byte-mismatched aggregates: %d / %d\n", bad, N_LAYERS);

skip_read_phase:
    /* Tear down */
    daos_cont_close(coh, NULL);
    if (coh_scrub.cookie != 0) {
        daos_cont_close(coh_scrub, NULL);
        daos_cont_destroy(poh, SCRUB_CONT, 1, NULL);
    }
    daos_pool_disconnect(poh, NULL);
    daos_fini();

    free(base_lo);
    if (lat_us) free(lat_us);
    return bad ? 3 : 0;
}
