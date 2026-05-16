/*
 * agg_sidecar: local-fast-path prototype.
 * Reads /tmp/agg_oid_map.bin (produced by tools/gather_daemon/dump_oids) and
 * issues vos_obj_fetch on a target xstream to bypass the Mercury RPC envelope.
 */

#define D_LOGFAC DD_FAC(server)

#include <daos_srv/daos_engine.h>
#include <daos_srv/vos.h>
#include <daos_srv/container.h>
#include <daos_srv/pool.h>
#include <daos_srv/dtx_srv.h>
#include <daos/rpc.h>
#include <daos/object.h>
#include <daos/placement.h>
#include <daos/dtx.h>
#include <gurt/common.h>
#include <pthread.h>
#include <sched.h>          /* sched_yield */
#include <sys/eventfd.h>    /* eventfd */
#include <poll.h>           /* poll */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <errno.h>
#include <sys/mman.h>                    /* mmap / MAP_HUGETLB */
#include <sys/stat.h>                    /* S_IFREG, etc. */

/* dfs on-disk layout constants — mirrors client/dfs/dfs_internal.h */
#define INODE_AKEY_NAME    "DFS_INODE"
#define MODE_IDX           0
#define OID_IDX            (sizeof(mode_t))
#define MTIME_IDX          (OID_IDX + sizeof(daos_obj_id_t))
#define CTIME_IDX          (MTIME_IDX + sizeof(uint64_t))
#define CSIZE_IDX          (CTIME_IDX + sizeof(uint64_t))
#define OCLASS_IDX         (CSIZE_IDX + sizeof(daos_size_t))
#define MTIME_NSEC_IDX     (OCLASS_IDX + sizeof(daos_oclass_id_t))
#define CTIME_NSEC_IDX     (MTIME_NSEC_IDX + sizeof(uint64_t))
#define UID_IDX            (CTIME_NSEC_IDX + sizeof(uint64_t))
#define GID_IDX            (UID_IDX + sizeof(uid_t))
#define SIZE_IDX           (GID_IDX + sizeof(gid_t))
#define HLC_IDX            (SIZE_IDX + sizeof(daos_size_t))
#define END_IDX            (HLC_IDX + sizeof(uint64_t))

/* File containing the dfs root OID + default oclass + chunk size.
 * Produced once by tools/gather_daemon/get_dfs_roots (client tool).
 * Format: [daos_obj_id_t root_oid][daos_oclass_id_t file_oclass][daos_size_t chunk_size] */
#define AGG_ROOTS_FILE     "/tmp/agg_dfs_roots.bin"

/* Persistent file containing OIDs written during emulate_write phase.
 * Format: [u64 count] then count × [daos_obj_id_t oid][u32 shard][u32 lv] */
#define AGG_EMUWRITE_OIDS  "/tmp/agg_emu_oids.bin"

/* Pre-created parent-directory OIDs produced by tools/gather_daemon/make_dirs.
 * Format: [u32 count] then count × { daos_obj_id_t oid; char name[32] }.
 * When present, emulate_write/read dispatch iter N to dirs[N % count] so
 * dentry writes fan across multiple targets instead of funneling through the
 * container root. If absent, fall back to the container root OID. */
#define AGG_DIRS_FILE      "/tmp/agg_dfs_dirs.bin"
#define AGG_MAX_DIRS       64

/* Runtime-configurable UUIDs — set via env vars AGG_POOL_UUID / AGG_CONT_UUID
 * in daos_server_debug.yml. Defaults baked in so the module still runs if the
 * env is missing, but in practice we always override at phase start. */
static char g_pool_uuid[37] = "0b43b245-1c26-4db4-8fc8-76448f02d003";
static char g_cont_uuid[37] = "d494d19f-826f-41fc-9f91-23dab15b37a5";
#define AGG_POOL_UUID g_pool_uuid
#define AGG_CONT_UUID g_cont_uuid
#define AGG_OID_FILE  "/tmp/agg_oid_map.bin"
#define AGG_TEST_OID_FILE "/tmp/agg_test_oid.bin"
#define AGG_TEST_OID_FILE_BRAVO "/tmp/agg_test_oid_bravo.bin"
#define AGG_BENCH_OID_FILE "/tmp/agg_bench_oids.bin"
#define AGG_READ_SZ   65536          /* 64 KiB per key */

/* Gather buffer for READ phase: dynamic-dispatch workers scatter each
 * fetched 64 KiB into slot[iter % AGG_GATHER_SLOTS] inside this blob.
 * Allocated once at module setup (prefers 2 MiB hugepages, falls back to
 * 4 KiB pages), freed at cleanup, reused across all read-phase runs. */
#define AGG_GATHER_BUF_SZ  (256ull << 20)                      /* 256 MiB */
#define AGG_GATHER_SLOTS   (AGG_GATHER_BUF_SZ / AGG_READ_SZ)   /* 4096    */

static void *g_gather_buf;
static int   g_gather_buf_is_hugepage;

/* Shared dispatch counter for dynamic (work-stealing) pthread dispatch.
 * Every worker fetch_adds to claim the next iter; no pre-sliced ranges. */
struct dispatch_state {
	atomic_int next_iter;
	int        total_iters;
};

/* Parent-dir placement cache. Populated at phase start by reading
 * /tmp/agg_dfs_dirs.bin and running pl_obj_place per dir, so per-iter workers
 * can pick dirs[iter % num_dirs] without touching pl_map on the hot path.
 *
 * For multi-group oclasses (OC_SX, OC_S2, etc.), the object's shards are
 * partitioned into num_grps groups of grp_size shards each. Each dkey hashes
 * to one group via d_hash_jump(d_hash_murmur64(dkey), num_grps); within the
 * group, shard 0 is the leader for writes. We cache the leader (tgt, shard)
 * of every group so per-iter workers can route without touching pl_map. */
#define AGG_MAX_DIR_GRPS 64
struct dir_grp {
	uint32_t tgt;
	uint32_t shard;
};
struct dir_info {
	daos_obj_id_t oid;
	uint32_t      num_grps;                 /* = ol_grp_nr */
	uint32_t      lv;
	struct dir_grp grps[AGG_MAX_DIR_GRPS];  /* leader per group */
	char          name[32];
};

static pthread_t g_bench_thread;
static volatile int g_stop;

struct oid_map {
	daos_obj_id_t	*oids;
	uint64_t	 count;
};

static struct oid_map g_map;

static int __attribute__((unused))
load_oid_map(const char *path, struct oid_map *out)
{
	FILE *f = fopen(path, "rb");
	if (!f) { D_ERROR("agg: fopen %s failed\n", path); return -1; }
	uint64_t cnt;
	if (fread(&cnt, sizeof(cnt), 1, f) != 1) { fclose(f); return -1; }
	daos_obj_id_t *arr = calloc(cnt, sizeof(daos_obj_id_t));
	for (uint64_t i = 0; i < cnt; ++i) {
		if (fread(&arr[i], sizeof(daos_obj_id_t), 1, f) != 1) { fclose(f); free(arr); return -1; }
		uint32_t nl;
		if (fread(&nl, sizeof(nl), 1, f) != 1) { fclose(f); free(arr); return -1; }
		char skip[64];
		if (nl > sizeof(skip)) nl = sizeof(skip);
		if (fread(skip, 1, nl, f) != nl) { fclose(f); free(arr); return -1; }
	}
	fclose(f);
	out->oids = arr;
	out->count = cnt;
	D_INFO("agg: loaded %lu oids\n", cnt);
	return 0;
}

/* Runs on target xstream. Reads one key via vos_obj_fetch. */
struct fetch_arg {
	struct ds_cont_child	*cont;
	daos_obj_id_t		 oid;
	uint32_t		 id_shard;
	uint32_t		 id_layout_ver;
	size_t			 write_size;       /* 0 → default AGG_READ_SZ */
	void			*dst;              /* caller-provided read dest; NULL → internal calloc */
	size_t			 dst_size;         /* 0 → default AGG_READ_SZ */
	void			*src_buf;          /* caller-provided, pre-filled write src; NULL → internal malloc+memset */
	size_t			 src_buf_size;     /* size of src_buf if provided */
	uint64_t		 elapsed_us;
	uint64_t		 got_size;
	unsigned char		 first_byte;
	int			 rc;
};

/* Write 64 KiB of a byte pattern into the array object at offset 0.
 * Mirrors fetch_one_ult's IOD shape — just calls vos_obj_update instead. */
static int
write_one_ult(void *arg)
{
	struct fetch_arg	*a = arg;
	daos_unit_oid_t		 uoid = {.id_pub = a->oid,
					 .id_shard = a->id_shard,
					 .id_layout_ver = a->id_layout_ver};
	uint64_t		 dkey_val = 1;
	char			 akey_val = '0';
	daos_key_t		 dkey;
	daos_iod_t		 iod = {0};
	size_t			 wsz = a->write_size > 0 ? a->write_size : AGG_READ_SZ;
	daos_recx_t		 recx = {.rx_idx = 0, .rx_nr = wsz};
	d_sg_list_t		 sgl = {0};
	d_iov_t			 iov;
	char			*buf;
	struct timespec		 t0, t1;
	int			 rc;

	/* Per-xstream container lookup (see comment in dfs_open_local_ult) */
	uuid_t pu_w, cu_w;
	uuid_parse(AGG_POOL_UUID, pu_w);
	uuid_parse(AGG_CONT_UUID, cu_w);
	struct ds_cont_child *cont_xs = NULL;
	rc = ds_cont_child_lookup(pu_w, cu_w, &cont_xs);
	if (rc || cont_xs == NULL) {
		a->rc = rc;
		return rc;
	}
	a->cont = cont_xs;

	int buf_owned = 0;
	if (a->src_buf && a->src_buf_size >= wsz) {
		buf = a->src_buf;
	} else {
		buf = malloc(wsz);
		if (!buf) { a->rc = -ENOMEM; if (cont_xs) ds_cont_child_put(cont_xs); return -1; }
		memset(buf, (int)a->first_byte, wsz);
		buf_owned = 1;
	}

	d_iov_set(&dkey, &dkey_val, sizeof(dkey_val));
	d_iov_set(&iod.iod_name, &akey_val, 1);
	iod.iod_type  = DAOS_IOD_ARRAY;
	iod.iod_size  = 1;
	iod.iod_nr    = 1;
	iod.iod_recxs = &recx;
	d_iov_set(&iov, buf, wsz);
	sgl.sg_nr = 1;
	sgl.sg_iovs = &iov;

	uint32_t pm_ver = a->cont->sc_pool->spc_map_version;
	daos_epoch_t epoch = d_hlc_get();

	/* Only log for the first few ops — otherwise 2048 × 5 lines overwhelms the log */
	static _Atomic int w_trace_count = 0;
	int trace = (atomic_fetch_add(&w_trace_count, 1) < 3);

	clock_gettime(CLOCK_MONOTONIC, &t0);

	if (trace) D_INFO("agg: W_TRACE step=pre_preamble oid.hi=%lx lo=%lx epoch=%lx pm_ver=%u\n",
			  a->oid.hi, a->oid.lo, (unsigned long)epoch, pm_ver);

	/* ---------- Full server-side preamble — reuses public APIs that
	 * obj_ioc_begin + ds_obj_rw_handler call. Only RPC wire is skipped. */
	int csum_rc = ds_cont_csummer_init(a->cont);
	if (trace) D_INFO("agg: W_TRACE step=ds_cont_csummer_init rc=%d\n", csum_rc);

	dss_rpc_cntr_enter(DSS_RC_OBJ);
	if (trace) D_INFO("agg: W_TRACE step=dss_rpc_cntr_enter\n");

	uint32_t nr_grps = 0;
	struct daos_oclass_attr *oca = daos_oclass_attr_find(a->oid, &nr_grps);
	if (trace) D_INFO("agg: W_TRACE step=daos_oclass_attr_find oca=%p nr_grps=%u\n",
			  oca, nr_grps);

	struct sched_req_attr    sra = {0};
	sched_req_attr_init(&sra, SCHED_REQ_UPDATE, &a->cont->sc_pool->spc_uuid);
	struct sched_request    *sreq = sched_req_get(&sra, ABT_THREAD_NULL);
	if (trace) D_INFO("agg: W_TRACE step=sched_req_get -> %p\n", sreq);

	struct dtx_id            dti;
	daos_dti_gen(&dti, false);
	struct dtx_epoch         dep = {.oe_value = epoch, .oe_first = epoch, .oe_flags = 0};
	struct dtx_leader_handle *dlh = NULL;
	/* RPC path uses flags=0x1001 = DTX_SOLO | DTX_EPOCH_OWNER (from log) */
	uint32_t                 dtx_flags = DTX_SOLO | DTX_EPOCH_OWNER;

	rc = dtx_leader_begin(a->cont->sc_hdl, &dti, &dep,
			      1 /*sub_modification_cnt*/, pm_ver, &uoid,
			      NULL, 0, NULL, 0, dtx_flags,
			      NULL /*mbs*/, NULL /*dce*/, &dlh);
	if (trace) D_INFO("agg: W_TRACE step=dtx_leader_begin rc=%d dlh=%p flags=%x\n",
			  rc, dlh, dtx_flags);

	if (rc == 0) {
		uint64_t dkey_hash_w = d_hash_murmur64((const unsigned char *)&dkey_val,
						       sizeof(dkey_val), 5731);
		int si_rc_w = dtx_sub_init(&dlh->dlh_handle, &uoid, dkey_hash_w);
		if (trace) D_INFO("agg: W_TRACE step=dtx_sub_init rc=%d\n", si_rc_w);

		rc = vos_obj_update_ex(a->cont->sc_hdl, uoid, epoch, pm_ver, 0,
				       &dkey, 1, &iod, NULL /*csums*/, &sgl,
				       &dlh->dlh_handle);
		if (trace) D_INFO("agg: W_TRACE step=vos_obj_update_ex rc=%d iod_size=%lu\n",
				  rc, iod.iod_size);

		int end_rc = dtx_leader_end(dlh, a->cont, rc);
		if (trace) D_INFO("agg: W_TRACE step=dtx_leader_end rc=%d (prev rc=%d)\n", end_rc, rc);
		if (rc == 0) rc = end_rc;
	}

	if (sreq) sched_req_put(sreq);
	dss_rpc_cntr_exit(DSS_RC_OBJ, !!rc);
	if (trace) D_INFO("agg: W_TRACE step=sched_req_put + dss_rpc_cntr_exit done\n");
	clock_gettime(CLOCK_MONOTONIC, &t1);
	a->elapsed_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL + (t1.tv_nsec - t0.tv_nsec) / 1000;
	a->rc = rc;
	a->got_size = iod.iod_size;

	if (buf_owned) free(buf);
	if (cont_xs) ds_cont_child_put(cont_xs);
	return rc;
}

/* ========================================================================
 * dfs_open_local + dfs_write_local — full local emulation of dfs_open + dfs_write.
 *
 * dfs_open_local mirrors client/dfs/common.c:insert_entry. It inserts a
 * dentry (name → new file OID + metadata) into the root directory via
 * vos_obj_update_ex — the same VOS call the RPC path's insert_entry
 * ultimately reaches, just without Mercury/CaRT in between.
 *
 * dfs_write_local is write_one_ult above, extracted for name parity.
 * ======================================================================== */

struct open_local_arg {
	struct ds_cont_child *cont;
	const char	     *name;          /* filename, e.g. "local_obj_0000" */
	daos_obj_id_t	      root_oid;      /* parent dir = container root */
	uint32_t	      root_id_shard;
	uint32_t	      root_id_layout_ver;
	daos_oclass_id_t      file_oclass;   /* oclass for the new file */
	daos_size_t	      chunk_size;    /* default chunk size from SB */
	/* outputs */
	daos_obj_id_t	      new_oid;       /* fresh OID we allocated */
	uint64_t	      elapsed_us;
	int		      rc;
};

static int
dfs_open_local_ult(void *arg)
{
	struct open_local_arg *a = arg;
	char             dentry_buf[END_IDX];
	daos_key_t       dkey;
	daos_iod_t       iod = {0};
	daos_recx_t      recx = { .rx_idx = 0, .rx_nr = END_IDX };
	d_sg_list_t      sgl = {0};
	d_iov_t          iov;
	daos_epoch_t     epoch;
	uint32_t         pm_ver;
	struct timespec  t0, t1, now;
	int              rc;

	/* Only trace the first 3 opens — at 15 log lines per call, 4096 opens
	 * would dump 60k lines into the engine log and swamp the measurement. */
	static _Atomic int o_trace_count = 0;
	int o_trace = (atomic_fetch_add(&o_trace_count, 1) < 3);

	/* ds_cont_child is per-xstream. Look it up HERE on the target xstream
	 * where vos ops will run, not on the bench thread's xstream. */
	uuid_t pu_local, cu_local;
	uuid_parse(AGG_POOL_UUID, pu_local);
	uuid_parse(AGG_CONT_UUID, cu_local);
	struct ds_cont_child *cont_xs = NULL;
	rc = ds_cont_child_lookup(pu_local, cu_local, &cont_xs);
	if (rc || cont_xs == NULL) {
		D_ERROR("agg: DFS_OPEN_LOCAL ds_cont_child_lookup rc=%d\n", rc);
		a->rc = rc;
		return rc;
	}
	a->cont = cont_xs;  /* overwrite with xstream-local handle */
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL lookup cont on this xstream sc_hdl=%p\n",
			     (void *)cont_xs->sc_hdl.cookie);

	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL begin name='%s' root=%lx/%lx file_oc=%u csize=%lu\n",
			     a->name, a->root_oid.hi, a->root_oid.lo, a->file_oclass, a->chunk_size);

	/* --- 1. Allocate a fresh OID for the new file ------------------ */
	/* Counter-based OID, like dfs's oid_gen (without the RPC for the
	 * namespace allocation — we just pick unique values).  Use non-
	 * reserved lo (>=2) so it doesn't clash with SB/root. */
	static _Atomic uint64_t next_lo = 100;
	a->new_oid.lo = atomic_fetch_add(&next_lo, 1);
	a->new_oid.hi = 0;
	rc = daos_obj_set_oid_by_class(&a->new_oid, DAOS_OT_ARRAY_BYTE,
				       a->file_oclass, 0);
	if (rc) {
		D_ERROR("agg: DFS_OPEN_LOCAL set_oid rc=%d\n", rc);
		a->rc = rc;
		return rc;
	}
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL allocated new_oid=%lx/%lx\n",
			     a->new_oid.hi, a->new_oid.lo);

	/* --- 2. Build the 88-byte dfs_entry payload -------------------- */
	memset(dentry_buf, 0, sizeof(dentry_buf));
	mode_t            mode    = S_IFREG | 0644;
	daos_oclass_id_t  oclass  = a->file_oclass;
	uid_t             uid     = 0;
	gid_t             gid     = 0;
	daos_size_t       value_len = 0;
	uint64_t          obj_hlc = 0;

	clock_gettime(CLOCK_REALTIME, &now);
	uint64_t mtime = now.tv_sec, ctime = now.tv_sec;
	uint64_t mtime_ns = now.tv_nsec, ctime_ns = now.tv_nsec;

	memcpy(dentry_buf + MODE_IDX,      &mode,      sizeof(mode));
	memcpy(dentry_buf + OID_IDX,       &a->new_oid, sizeof(a->new_oid));
	memcpy(dentry_buf + MTIME_IDX,     &mtime,     sizeof(mtime));
	memcpy(dentry_buf + CTIME_IDX,     &ctime,     sizeof(ctime));
	memcpy(dentry_buf + CSIZE_IDX,     &a->chunk_size, sizeof(a->chunk_size));
	memcpy(dentry_buf + OCLASS_IDX,    &oclass,    sizeof(oclass));
	memcpy(dentry_buf + MTIME_NSEC_IDX,&mtime_ns,  sizeof(mtime_ns));
	memcpy(dentry_buf + CTIME_NSEC_IDX,&ctime_ns,  sizeof(ctime_ns));
	memcpy(dentry_buf + UID_IDX,       &uid,       sizeof(uid));
	memcpy(dentry_buf + GID_IDX,       &gid,       sizeof(gid));
	memcpy(dentry_buf + SIZE_IDX,      &value_len, sizeof(value_len));
	memcpy(dentry_buf + HLC_IDX,       &obj_hlc,   sizeof(obj_hlc));

	/* --- 3. Build the VOS update descriptors ----------------------- */
	d_iov_set(&dkey, (void *)a->name, strlen(a->name));
	d_iov_set(&iod.iod_name, INODE_AKEY_NAME, sizeof(INODE_AKEY_NAME) - 1);
	iod.iod_nr    = 1;
	iod.iod_type  = DAOS_IOD_ARRAY;
	iod.iod_size  = 1;
	iod.iod_recxs = &recx;

	d_iov_set(&iov, dentry_buf, END_IDX);
	sgl.sg_nr   = 1;
	sgl.sg_iovs = &iov;

	/* --- 4. vos_obj_update_ex on the root directory OID ------------ */
	daos_unit_oid_t root_uoid = {
		.id_pub         = a->root_oid,
		.id_shard       = a->root_id_shard,
		.id_layout_ver  = a->root_id_layout_ver,
	};
	epoch  = d_hlc_get();
	pm_ver = a->cont->sc_pool->spc_map_version;

	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL vos_obj_update_ex root_uoid=%lx/%lx shard=%u lv=%u epoch=%lx pm_ver=%u dkey='%s' akey='%s' recx=[0..%lu] cell=1\n",
			     root_uoid.id_pub.hi, root_uoid.id_pub.lo,
			     root_uoid.id_shard, root_uoid.id_layout_ver,
			     (unsigned long)epoch, pm_ver, a->name, INODE_AKEY_NAME,
			     (unsigned long)END_IDX);

	/* ---------- Full server-side preamble, reusing existing public APIs
	 * that obj_ioc_begin/ds_obj_rw_handler call. Only RPC transport is
	 * skipped; all engine-side setup is identical to the RPC path. ---- */

	/* (0) cont_iv_hdl_fetch via ds_cont_find_hdl — RPC path's FIRST call.
	 * Uses the server's own srv_cont_hdl.sch_uuid (always populated on
	 * each target xstream's pool_child), so no client coh needed. */
	uuid_t pool_uuid_local;
	uuid_parse(AGG_POOL_UUID, pool_uuid_local);
	struct ds_pool_child *pc = ds_pool_child_lookup(pool_uuid_local);
	struct ds_cont_hdl   *coh_local = NULL;
	if (pc && !d_list_empty(&pc->spc_srv_cont_hdl)) {
		struct ds_cont_hdl *srv_hdl = d_list_entry(pc->spc_srv_cont_hdl.next,
							   struct ds_cont_hdl, sch_link);
		int fh_rc = ds_cont_find_hdl(pool_uuid_local, srv_hdl->sch_uuid, &coh_local);
		if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL ds_cont_find_hdl rc=%d coh=%p\n", fh_rc, coh_local);
	}
	if (pc) ds_pool_child_put(pc);

	/* (a) obj_ioc_begin → obj_ioc_init calls ds_cont_csummer_init */
	int csum_rc = ds_cont_csummer_init(a->cont);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL ds_cont_csummer_init rc=%d\n", csum_rc);

	/* (b) obj_ioc_begin → obj_ioc_begin_lite calls dss_rpc_cntr_enter */
	dss_rpc_cntr_enter(DSS_RC_OBJ);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL dss_rpc_cntr_enter(DSS_RC_OBJ)\n");

	/* (c) obj_ioc_init_oca calls daos_oclass_attr_find */
	uint32_t nr_grps = 0;
	struct daos_oclass_attr *oca = daos_oclass_attr_find(a->root_oid, &nr_grps);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL daos_oclass_attr_find oca=%p nr_grps=%u\n",
			     oca, nr_grps);

	/* (d) sched_req_get — scheduler registration (implicit in dss_rpc_hdlr) */
	struct sched_req_attr    sra = {0};
	sched_req_attr_init(&sra, SCHED_REQ_UPDATE, &a->cont->sc_pool->spc_uuid);
	struct sched_request    *sreq = sched_req_get(&sra, ABT_THREAD_NULL);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL sched_req_get -> %p\n", sreq);

	/* (e) dtx_leader_begin — matches srv_obj.c:3148 */
	struct dtx_id            dti;
	daos_dti_gen(&dti, false);
	struct dtx_epoch         dep = {.oe_value = epoch, .oe_first = epoch, .oe_flags = 0};
	struct dtx_leader_handle *dlh = NULL;
	uint32_t                 dtx_flags = DTX_SOLO | DTX_EPOCH_OWNER;

	clock_gettime(CLOCK_MONOTONIC, &t0);
	rc = dtx_leader_begin(a->cont->sc_hdl, &dti, &dep,
			      1 /*sub_modification_cnt — matches RPC path*/,
			      pm_ver, &root_uoid,
			      NULL, 0, NULL, 0, dtx_flags,
			      NULL /*mbs*/, NULL /*dce*/, &dlh);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL dtx_leader_begin rc=%d dlh=%p flags=%x sub_mod=1\n",
			     rc, dlh, dtx_flags);

	if (rc == 0) {
		/* (f-pre) dtx_sub_init — RPC path calls this inside obj_rw_complete
		 * before vos_update_end; we call it before vos_obj_update_ex so
		 * dth_op_seq is bumped by the time ilog_update runs. */
		uint64_t dkey_hash = d_hash_murmur64((const unsigned char *)a->name,
						     strlen(a->name), 5731);
		int si_rc = dtx_sub_init(&dlh->dlh_handle, &root_uoid, dkey_hash);
		if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL dtx_sub_init rc=%d dkey_hash=%lx\n",
				     si_rc, dkey_hash);

		/* (f) vos_obj_update_ex — same call RPC path makes */
		rc = vos_obj_update_ex(a->cont->sc_hdl, root_uoid, epoch, pm_ver,
				       VOS_OF_COND_DKEY_INSERT,
				       &dkey, 1, &iod, NULL /*csums*/, &sgl,
				       &dlh->dlh_handle);
		if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL vos_obj_update_ex rc=%d\n", rc);

		/* (g) dtx_leader_end — matches srv_obj.c:3169 */
		int end_rc = dtx_leader_end(dlh, a->cont, rc);
		if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL dtx_leader_end rc=%d (prev rc=%d)\n", end_rc, rc);
		if (rc == 0) rc = end_rc;
	}

	/* (h) teardown matching obj_ioc_end */
	if (sreq) sched_req_put(sreq);
	dss_rpc_cntr_exit(DSS_RC_OBJ, !!rc);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL sched_req_put + dss_rpc_cntr_exit done\n");

	clock_gettime(CLOCK_MONOTONIC, &t1);
	a->elapsed_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL +
			(t1.tv_nsec - t0.tv_nsec) / 1000;
	a->rc = rc;

	if (cont_xs) ds_cont_child_put(cont_xs);
	if (o_trace) D_INFO("agg: DFS_OPEN_LOCAL done rc=%d us=%lu\n", rc, a->elapsed_us);
	if (rc)
		D_ERROR("agg: DFS_OPEN_LOCAL FAIL name='%s' rc=%d us=%lu\n",
			a->name, rc, a->elapsed_us);
	return rc;
}

/* dfs_write_local is an alias for the existing write_one_ult — same shape. */
#define dfs_write_local_ult write_one_ult

/* Parallel worker: each pthread drives a slice of the N-file loop,
 * issuing open_local + write_local via dss_ult_execute.
 * Multiple pthreads concurrently → multiple target xstreams in flight.
 *
 * Written OIDs are stored into a caller-provided table for later read-back. */
struct written_oid_entry {
	daos_obj_id_t oid;
	uint32_t      file_shard;
	uint32_t      file_lv;
	char          name[64];
};

struct parallel_worker_arg {
	int               tid;
	struct dispatch_state *ds;        /* shared atomic work counter */
	struct dir_info  *dirs;           /* parent-dir array; iter %% num_dirs picks one */
	int               num_dirs;
	int               iodepth;        /* # of concurrent ULTs per pthread (>=1) */
	daos_oclass_id_t  file_oclass;
	daos_size_t       chunk_size;
	size_t            write_size;     /* 0 → default AGG_READ_SZ */
	uuid_t            pool_uuid;
	struct ds_cont_child *cont;
	uint64_t          name_hlc;
	int               success;
	struct written_oid_entry *oid_table;
};

/* void-return wrappers for dss_ult_create (which takes void (*)(void *),
 * while our existing ULTs return int). The return code is already stored
 * in the arg struct (oa.rc / wa.rc / ra.rc) so discarding the int is fine. */
static void dfs_open_local_ult_v(void *a)       { (void)dfs_open_local_ult(a); }
static void write_one_ult_v(void *a)            { (void)write_one_ult(a); }
static void dfs_open_local_read_ult_v(void *a);
static void fetch_one_ult_v(void *a);

/* Per-iter in-flight state for a batch of `iodepth` slots inside one pthread.
 * Each slot is at most one (open ULT, write ULT) pair at a time. */
struct iter_slot {
	int                    iter;
	int                    valid;      /* 0 → slot skipped / claimed nothing */
	char                   name[64];
	struct open_local_arg  oa;
	struct fetch_arg       wa;
	ABT_thread             open_ult;
	ABT_thread             write_ult;
	uint32_t               file_tgt;
	uint32_t               file_shard;
};

static void *
emulate_parallel_worker(void *varg)
{
	struct parallel_worker_arg *w = varg;
	int iodepth = w->iodepth > 0 ? w->iodepth : 1;
	struct iter_slot *slots = calloc(iodepth, sizeof(*slots));
	if (!slots) { D_ERROR("agg: worker calloc slots failed\n"); return NULL; }

	/* Pre-allocate and pre-fill a single shared source buffer for this worker,
	 * reused across all iters / slots. Mirrors the RPC path, which allocates
	 * once and reuses the buffer for every transfer. Avoids per-op malloc +
	 * 1 MiB memset, which otherwise consumes engine memory bandwidth and
	 * competes with VOS/BIO on the same DRAM channels. */
	size_t worker_src_sz = w->write_size > 0 ? w->write_size : (size_t)AGG_READ_SZ;
	unsigned char *worker_src_buf = malloc(worker_src_sz);
	if (!worker_src_buf) {
		D_ERROR("agg: worker src_buf malloc failed (size=%zu)\n", worker_src_sz);
		free(slots); return NULL;
	}
	memset(worker_src_buf, 0xEB, worker_src_sz);

	while (!g_stop) {
		/* ── Phase 1: claim up to iodepth iters + async-dispatch opens ── */
		int batch = 0;
		for (int j = 0; j < iodepth; ++j) {
			int iter = atomic_fetch_add_explicit(&w->ds->next_iter, 1,
							     memory_order_relaxed);
			if (iter >= w->ds->total_iters) break;
			slots[j].iter      = iter;
			slots[j].valid     = 1;
			slots[j].open_ult  = ABT_THREAD_NULL;
			slots[j].write_ult = ABT_THREAD_NULL;
			snprintf(slots[j].name, sizeof(slots[j].name),
				 "par_%lx_%06d", w->name_hlc, iter);

			struct dir_info *pd = &w->dirs[iter % w->num_dirs];
			uint64_t dh = d_hash_murmur64((const unsigned char *)slots[j].name,
						      strlen(slots[j].name), 5731);
			uint32_t grp = (pd->num_grps > 1)
				? d_hash_jump(dh, pd->num_grps) : 0;
			struct dir_grp *gg = &pd->grps[grp];

			slots[j].oa = (struct open_local_arg){
				.cont               = w->cont,
				.name               = slots[j].name,
				.root_oid           = pd->oid,
				.root_id_shard      = gg->shard,
				.root_id_layout_ver = pd->lv,
				.file_oclass        = w->file_oclass,
				.chunk_size         = w->chunk_size,
			};
			int rc = dss_ult_create(dfs_open_local_ult_v, &slots[j].oa,
						DSS_XS_VOS, (int)gg->tgt, 0,
						&slots[j].open_ult);
			if (rc) { slots[j].valid = 0; slots[j].open_ult = ABT_THREAD_NULL; }
			batch++;
		}
		if (batch == 0) break;

		/* ── Phase 2: wait for all in-flight open ULTs ── */
		for (int j = 0; j < batch; ++j) {
			if (slots[j].open_ult == ABT_THREAD_NULL) continue;
			ABT_thread_join(slots[j].open_ult);
			ABT_thread_free(&slots[j].open_ult);
		}

		/* ── Phase 3: compute placement + async-dispatch writes ── */
		for (int j = 0; j < batch; ++j) {
			if (!slots[j].valid || slots[j].oa.rc != 0) {
				slots[j].valid = 0;
				continue;
			}
			struct pl_map *pl2 = pl_map_find(w->pool_uuid, slots[j].oa.new_oid);
			slots[j].file_tgt = 0; slots[j].file_shard = 0;
			if (pl2) {
				struct daos_obj_md md2 = {0};
				md2.omd_id = slots[j].oa.new_oid; md2.omd_ver = 1;
				struct pl_obj_layout *lay2 = NULL;
				if (pl_obj_place(pl2, 2, &md2, 0, NULL, &lay2) == 0 && lay2) {
					slots[j].file_tgt   = lay2->ol_shards[0].po_index;
					slots[j].file_shard = lay2->ol_shards[0].po_shard;
					pl_obj_layout_free(lay2);
				}
				pl_map_decref(pl2);
			}
			slots[j].wa = (struct fetch_arg){
				.cont          = w->cont,
				.oid           = slots[j].oa.new_oid,
				.id_shard      = slots[j].file_shard,
				.id_layout_ver = 2,
				.write_size    = w->write_size,
				.src_buf       = worker_src_buf,
				.src_buf_size  = worker_src_sz,
				.first_byte    = 0xEB,
			};
			int rc = dss_ult_create(write_one_ult_v, &slots[j].wa,
						DSS_XS_VOS, (int)slots[j].file_tgt, 0,
						&slots[j].write_ult);
			if (rc) { slots[j].valid = 0; slots[j].write_ult = ABT_THREAD_NULL; }
		}

		/* ── Phase 4: wait for all writes, record successes ── */
		for (int j = 0; j < batch; ++j) {
			if (slots[j].write_ult == ABT_THREAD_NULL) continue;
			ABT_thread_join(slots[j].write_ult);
			ABT_thread_free(&slots[j].write_ult);
			if (slots[j].valid && slots[j].wa.rc == 0) {
				w->success++;
				D_INFO("agg: EMULATE PAR_FILE %d,%lu,%lu,%d\n",
				       slots[j].iter, slots[j].oa.elapsed_us,
				       slots[j].wa.elapsed_us, w->tid);
				if (w->oid_table) {
					int it = slots[j].iter;
					w->oid_table[it].oid        = slots[j].oa.new_oid;
					w->oid_table[it].file_shard = slots[j].file_shard;
					w->oid_table[it].file_lv    = 2;
					size_t nl = strlen(slots[j].name);
					if (nl >= sizeof(w->oid_table[it].name))
						nl = sizeof(w->oid_table[it].name) - 1;
					memcpy(w->oid_table[it].name, slots[j].name, nl);
					w->oid_table[it].name[nl] = '\0';
				}
			}
			slots[j].valid = 0;
		}
	}
	free(worker_src_buf);
	free(slots);
	return NULL;
}

static int fetch_one_ult(void *arg);

/* dfs_open_local for READ: given a filename, resolve dentry → get child OID.
 * Mirrors the server-side effect of dfs_open(O_RDONLY): fetch dentry from
 * parent directory's VOS object, parse dfs_entry bytes to extract the child
 * file's OID.  Does NOT run pl_obj_place (caller does that). */
struct open_local_read_arg {
	const char       *name;
	daos_obj_id_t     root_oid;
	uint32_t          root_shard;
	uint32_t          root_lv;
	/* outputs */
	daos_obj_id_t     resolved_oid;
	uint64_t          elapsed_us;
	int               rc;
};

static int
dfs_open_local_read_ult(void *arg)
{
	struct open_local_read_arg *a = arg;
	char             buf[END_IDX];
	daos_key_t       dkey;
	daos_iod_t       iod = {0};
	daos_recx_t      recx = { .rx_idx = 0, .rx_nr = END_IDX };
	d_sg_list_t      sgl = {0};
	d_iov_t          iov;
	struct timespec  t0, t1;
	int              rc;

	uuid_t pu, cu;
	uuid_parse(AGG_POOL_UUID, pu);
	uuid_parse(AGG_CONT_UUID, cu);
	struct ds_cont_child *cont_xs = NULL;
	rc = ds_cont_child_lookup(pu, cu, &cont_xs);
	if (rc || cont_xs == NULL) { a->rc = rc; return rc; }

	d_iov_set(&dkey, (void *)a->name, strlen(a->name));
	d_iov_set(&iod.iod_name, INODE_AKEY_NAME, sizeof(INODE_AKEY_NAME) - 1);
	iod.iod_nr    = 1;
	iod.iod_type  = DAOS_IOD_ARRAY;
	iod.iod_size  = 1;
	iod.iod_recxs = &recx;
	d_iov_set(&iov, buf, END_IDX);
	sgl.sg_nr   = 1;
	sgl.sg_iovs = &iov;

	daos_unit_oid_t root_uoid = {
		.id_pub        = a->root_oid,
		.id_shard      = a->root_shard,
		.id_layout_ver = a->root_lv,
	};

	clock_gettime(CLOCK_MONOTONIC, &t0);
	rc = vos_obj_fetch(cont_xs->sc_hdl, root_uoid, DAOS_EPOCH_MAX, 0,
			   &dkey, 1, &iod, &sgl);
	clock_gettime(CLOCK_MONOTONIC, &t1);
	a->elapsed_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL +
			(t1.tv_nsec - t0.tv_nsec) / 1000;
	a->rc = rc;

	if (rc == 0) {
		/* Parse dfs_entry — extract OID at offset OID_IDX */
		memcpy(&a->resolved_oid, buf + OID_IDX, sizeof(a->resolved_oid));
	}

	ds_cont_child_put(cont_xs);
	return rc;
}

/* Parallel READ worker: per file does dfs_open_local_read(name) then
 * dfs_read_local(resolved_oid) — mirrors RPC path's dfs_open + dfs_read.
 * Fetched 64 KiB lands directly at gather_buf[slot * AGG_READ_SZ] where
 * slot = iter % AGG_GATHER_SLOTS. No intermediate copy. */
struct read_worker_arg {
	int               tid;
	struct dispatch_state *ds;        /* shared atomic work counter */
	struct dir_info  *dirs;           /* parent-dir array; iter %% num_dirs picks one */
	int               num_dirs;
	int               iodepth;        /* # of concurrent ULTs per pthread (>=1) */
	size_t            read_size;      /* bytes per read; 0 → AGG_READ_SZ default */
	uuid_t            pool_uuid;
	struct written_oid_entry *oid_table;
	void             *gather_buf;     /* base of 256 MiB blob */
	int               success;
};

/* Per-iter in-flight state for read path.
 * Rolling-window pipeline: each slot advances through states independently.
 * - SLOT_EMPTY     : no live ULT; ready to be refilled with a new iter.
 * - OPEN_INFLIGHT  : open_ult is running dfs_open_local_read (dentry lookup).
 * - READ_INFLIGHT  : read_ult is running fetch_one_ult (data fetch).
 * After an open completes (oa.rc==0), slot transitions to READ_INFLIGHT.
 * After a read completes, slot drains to SLOT_EMPTY and is immediately
 * refilled with the next claimed iter — keeps pipeline full. */
enum slot_state { SLOT_EMPTY = 0, OPEN_INFLIGHT, READ_INFLIGHT };

/* Per-pthread completion ring for read workers.
 * Instead of polling ABT_thread_get_state on each slot in a hot loop
 * (~1.6 μs/scan × many scans per op = ~40-60 μs wasted per op), each
 * completing ULT atomically sets its slot's bit in completion_bits and,
 * *only if the ring was empty before*, writes to eventfd to wake the pthread.
 * This matches the io_uring/epoll edge-triggered pattern: near-zero producer
 * cost when pipeline is busy, genuine block when idle. */
struct worker_ring {
	_Atomic uint32_t completion_bits;      /* bit j set = slot j done */
	int              efd;                  /* eventfd for wakeup */
};

static inline void
signal_completion(struct worker_ring *r, uint32_t slot_id)
{
	/* atomic OR returns previous value. If previous was 0, the ring was
	 * empty and the consumer may be blocked on the eventfd — wake it. */
	uint32_t prev = atomic_fetch_or_explicit(&r->completion_bits,
						 (uint32_t)1u << slot_id,
						 memory_order_release);
	if (prev == 0) {
		uint64_t one = 1;
		ssize_t w = write(r->efd, &one, sizeof(one));
		(void)w;   /* suppress -Wunused-result */
	}
}

struct read_slot {
	int                         iter;
	enum slot_state             state;
	struct open_local_read_arg  oa;
	struct fetch_arg            ra;
	ABT_thread                  ult;       /* active ULT (only one per slot at a time) */
	uint32_t                    file_tgt;
	uint32_t                    file_shard;
	int                         slot;      /* gather buffer slot index */
	/* Ring/slot fields — read by the wrapper ULT to signal completion. */
	struct worker_ring         *ring;
	uint32_t                    slot_id;   /* == j (position in pthread's slots[]) */
};

/* kept for diagnostic builds / future single-ULT dispatch paths */
static void __attribute__((unused)) dfs_open_local_read_ult_v(void *a) { (void)dfs_open_local_read_ult(a); }
static void __attribute__((unused)) fetch_one_ult_v(void *a)           { (void)fetch_one_ult(a); }

/* Wrapper ULT — calls the right internal ULT based on slot state, then
 * signals completion via the worker_ring. Runs on the target xstream; pushes
 * to the pthread's ring on exit. */
static void
slot_ult_wrap(void *v)
{
	struct read_slot *s = v;
	if (s->state == OPEN_INFLIGHT)
		dfs_open_local_read_ult(&s->oa);
	else /* READ_INFLIGHT */
		fetch_one_ult(&s->ra);
	signal_completion(s->ring, s->slot_id);
}

/* Rolling-window helper: claim next iter and dispatch an open ULT for slot s.
 * On success returns 1 (state=OPEN_INFLIGHT); on work-exhausted or dispatch
 * failure returns 0 and leaves state=SLOT_EMPTY. */
static int
claim_and_dispatch_open(struct read_slot *s, struct read_worker_arg *w)
{
	int iter = atomic_fetch_add_explicit(&w->ds->next_iter, 1,
					     memory_order_relaxed);
	if (iter >= w->ds->total_iters) {
		s->state = SLOT_EMPTY;
		return 0;
	}
	s->iter = iter;
	struct dir_info *pd = &w->dirs[iter % w->num_dirs];
	const char *nm = w->oid_table[iter].name;
	uint64_t dh = d_hash_murmur64((const unsigned char *)nm, strlen(nm), 5731);
	uint32_t grp = (pd->num_grps > 1) ? d_hash_jump(dh, pd->num_grps) : 0;
	struct dir_grp *gg = &pd->grps[grp];

	s->oa = (struct open_local_read_arg){
		.name       = nm,
		.root_oid   = pd->oid,
		.root_shard = gg->shard,
		.root_lv    = pd->lv,
	};
	/* Set state BEFORE dispatching so the wrapper (which runs on the
	 * target xstream) sees the correct state when it reads s->state. */
	s->state = OPEN_INFLIGHT;
	int rc = dss_ult_create(slot_ult_wrap, s,
				DSS_XS_VOS, (int)gg->tgt, 0, &s->ult);
	if (rc) {
		s->state = SLOT_EMPTY;
		return 0;
	}
	return 1;
}

static void *
emulate_parallel_read_worker(void *varg)
{
	struct read_worker_arg *w = varg;
	int iodepth = w->iodepth > 0 ? w->iodepth : 1;
	if (iodepth > 32) iodepth = 32;   /* bitmap is 32 bits */
	struct read_slot *slots = calloc(iodepth, sizeof(*slots));
	if (!slots) { D_ERROR("agg: read worker calloc slots failed\n"); return NULL; }

	/* Per-pthread completion ring: ULTs signal via atomic fetch_or on a
	 * bitmap; pthread drains via atomic_exchange, blocks on eventfd when
	 * empty. Coalesced signaling = only write eventfd on empty→non-empty
	 * transition, so busy pipeline pays ~50ns per completion instead of
	 * ~1-2μs eventfd syscall each time. */
	struct worker_ring ring;
	atomic_init(&ring.completion_bits, 0u);
	ring.efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (ring.efd < 0) {
		D_ERROR("agg: read worker eventfd failed errno=%d\n", errno);
		free(slots);
		return NULL;
	}

	/* ── Prime the pipeline: fill all slots with open ULTs ── */
	int inflight = 0;
	for (int j = 0; j < iodepth && !g_stop; ++j) {
		slots[j].state   = SLOT_EMPTY;
		slots[j].ring    = &ring;
		slots[j].slot_id = (uint32_t)j;
		if (claim_and_dispatch_open(&slots[j], w))
			inflight++;
	}

	/* ── Steady-state rolling window: block on eventfd, drain bitmap ── */
	while (inflight > 0 && !g_stop) {
		/* Atomically grab & clear the completion bits in one op. */
		uint32_t bits = atomic_exchange_explicit(&ring.completion_bits,
							 0u, memory_order_acquire);

		if (bits == 0) {
			/* Nothing ready — block on eventfd (1ms timeout as safety
			 * net against any lost-wakeup race). */
			struct pollfd pfd = { .fd = ring.efd, .events = POLLIN };
			poll(&pfd, 1, 1);
			uint64_t val;
			ssize_t rr = read(ring.efd, &val, sizeof(val));
			(void)rr;   /* suppress -Wunused-result; drain eventfd */
			continue;   /* re-check bits on next iteration */
		}

		/* Process every completed slot this wake covers. */
		while (bits) {
			int j = __builtin_ctz(bits);
			bits &= bits - 1;

			ABT_thread_free(&slots[j].ult);

			if (slots[j].state == OPEN_INFLIGHT) {
				if (slots[j].oa.rc == 0) {
					/* Open succeeded — resolve placement and dispatch read */
					struct pl_map *pl = pl_map_find(w->pool_uuid,
									slots[j].oa.resolved_oid);
					slots[j].file_tgt = 0; slots[j].file_shard = 0;
					if (pl) {
						struct daos_obj_md md = {0};
						md.omd_id = slots[j].oa.resolved_oid;
						md.omd_ver = 1;
						struct pl_obj_layout *layout = NULL;
						if (pl_obj_place(pl, 2, &md, 0, NULL, &layout) == 0 && layout) {
							slots[j].file_tgt   = layout->ol_shards[0].po_index;
							slots[j].file_shard = layout->ol_shards[0].po_shard;
							pl_obj_layout_free(layout);
						}
						pl_map_decref(pl);
					}
					size_t rsz = w->read_size ? w->read_size : (size_t)AGG_READ_SZ;
					int slot_cnt = (int)(AGG_GATHER_BUF_SZ / rsz);
					if (slot_cnt < 1) slot_cnt = 1;
					slots[j].slot = slots[j].iter % slot_cnt;
					slots[j].ra = (struct fetch_arg){
						.cont          = NULL,
						.oid           = slots[j].oa.resolved_oid,
						.id_shard      = slots[j].file_shard,
						.id_layout_ver = 2,
						.dst           = (char *)w->gather_buf +
								 (size_t)slots[j].slot * rsz,
						.dst_size      = rsz,
					};
					/* Set state BEFORE dispatch so wrapper reads correct value. */
					slots[j].state = READ_INFLIGHT;
					int rc = dss_ult_create(slot_ult_wrap, &slots[j],
								DSS_XS_VOS, (int)slots[j].file_tgt,
								0, &slots[j].ult);
					if (rc != 0) {
						/* Dispatch failed — drain this slot */
						slots[j].state = SLOT_EMPTY;
						inflight--;
					}
				} else {
					/* Open failed — drain this slot */
					slots[j].state = SLOT_EMPTY;
					inflight--;
				}
			} else {    /* READ_INFLIGHT */
				if (slots[j].ra.rc == 0) {
					w->success++;
					D_INFO("agg: EMUREAD PAR_FILE %d,%lu,%lu,%d slot=%d\n",
					       slots[j].iter, slots[j].oa.elapsed_us,
					       slots[j].ra.elapsed_us, w->tid, slots[j].slot);
				}
				slots[j].state = SLOT_EMPTY;
				inflight--;
			}

			/* Immediately refill empty slot with next iter — keeps
			 * pipeline at iodepth continuously. */
			if (slots[j].state == SLOT_EMPTY) {
				if (claim_and_dispatch_open(&slots[j], w))
					inflight++;
			}
		}
	}

	close(ring.efd);
	free(slots);
	return NULL;
}

static int
fetch_one_ult(void *arg)
{
	struct fetch_arg	*a = arg;
	daos_unit_oid_t		 uoid = {.id_pub = a->oid,
					 .id_shard = a->id_shard,
					 .id_layout_ver = a->id_layout_ver};
	uint64_t		 dkey_val = 1;     /* chunk 0 = dkey 1 (dc_array compute_dkey) */
	char			 akey_val = '0';
	daos_key_t		 dkey;
	daos_iod_t		 iod = {0};
	size_t			 rsz = a->dst_size ? a->dst_size : AGG_READ_SZ;
	daos_recx_t		 recx = {.rx_idx = 0, .rx_nr = rsz};
	d_sg_list_t		 sgl = {0};
	d_iov_t			 iov;
	char			*buf;
	int			 buf_owned = 0;
	struct timespec		 t0, t1;
	int			 rc;

	if (a->dst) {
		buf = a->dst;
	} else {
		buf = calloc(1, rsz);
		if (!buf) { a->rc = -ENOMEM; return -1; }
		buf_owned = 1;
	}

	d_iov_set(&dkey, &dkey_val, sizeof(dkey_val));
	d_iov_set(&iod.iod_name, &akey_val, 1);
	iod.iod_type  = DAOS_IOD_ARRAY;
	iod.iod_size  = 1;
	iod.iod_nr    = 1;
	iod.iod_recxs = &recx;
	d_iov_set(&iov, buf, rsz);
	sgl.sg_nr = 1;
	sgl.sg_iovs = &iov;

	/* Per-xstream container lookup — sc_hdl is per-xstream. */
	uuid_t pu_f, cu_f;
	uuid_parse(AGG_POOL_UUID, pu_f);
	uuid_parse(AGG_CONT_UUID, cu_f);
	struct ds_cont_child *cont_xs = NULL;
	rc = ds_cont_child_lookup(pu_f, cu_f, &cont_xs);
	if (rc || cont_xs == NULL) {
		a->rc = rc;
		if (buf_owned) free(buf);
		return rc;
	}

	clock_gettime(CLOCK_MONOTONIC, &t0);
	rc = vos_obj_fetch(cont_xs->sc_hdl, uoid, DAOS_EPOCH_MAX, 0,
			   &dkey, 1, &iod, &sgl);
	clock_gettime(CLOCK_MONOTONIC, &t1);
	a->elapsed_us = (t1.tv_sec - t0.tv_sec) * 1000000ULL + (t1.tv_nsec - t0.tv_nsec) / 1000;
	a->rc = rc;
	a->got_size = iod.iod_size;
	a->first_byte = (unsigned char)buf[0];
	if (buf_owned) free(buf);
	ds_cont_child_put(cont_xs);
	return rc;
}

/* Search for a target OID across all objects on this target.  If found,
 * record id_shard and id_layout_ver back into fetch_arg so the caller can
 * do vos_obj_fetch with the right placement. */
static int
iter_dkeys_ult(void *arg)
{
	struct fetch_arg	*a = arg;
	vos_iter_param_t	 param = {0};
	daos_handle_t		 ih = DAOS_HDL_INVAL;
	int			 count = 0;
	int			 rc;

	param.ip_hdl = a->cont->sc_hdl;
	param.ip_epr.epr_lo = 0;
	param.ip_epr.epr_hi = DAOS_EPOCH_MAX;

	rc = vos_iter_prepare(VOS_ITER_OBJ, &param, &ih, NULL);
	if (rc) {
		D_ERROR("agg: vos_iter_prepare OBJ rc=%d\n", rc);
		a->rc = rc;
		return rc;
	}

	a->got_size = 0;
	a->first_byte = 0;

	rc = vos_iter_probe(ih, NULL);
	while (rc == 0) {
		vos_iter_entry_t entry = {0};
		rc = vos_iter_fetch(ih, &entry, NULL);
		if (rc != 0)
			break;
		if (entry.ie_oid.id_pub.hi == a->oid.hi &&
		    entry.ie_oid.id_pub.lo == a->oid.lo) {
			D_INFO("agg: MATCH count=%d oid.hi=%lx oid.lo=%lx shard=%u layout_ver=%u\n",
			       count, entry.ie_oid.id_pub.hi, entry.ie_oid.id_pub.lo,
			       entry.ie_oid.id_shard, entry.ie_oid.id_layout_ver);
			a->got_size = 1; /* flag: found */
			a->first_byte = (unsigned char)entry.ie_oid.id_shard;
			/* pack layout_ver into elapsed_us field (repurposed) */
			a->elapsed_us = entry.ie_oid.id_layout_ver;
			break;
		}
		count++;
		rc = vos_iter_next(ih, NULL);
	}

	vos_iter_finish(ih);
	a->rc = (rc == -DER_NONEXIST) ? 0 : rc;
	/* Record total scanned for diagnostic */
	if (a->got_size == 0)
		a->got_size = (uint64_t)count | 0x8000000000000000ULL; /* high bit = "not found, scanned N" */
	return 0;
}

/* Forward: slice_work struct defined inside bench_thread_fn. Must match exactly. */
struct slice_work_extern {
	daos_obj_id_t		*oids;
	uint32_t		*idx_list;
	uint32_t		 n;
	int			 target;
	int			 do_write;
	struct ds_cont_child	*cont;
	uint64_t		*lat_out;
	uint32_t		 success_count;
	atomic_int		*pending;
};

/* Runs on target xstream; processes one slice serially. */
void
agg_worker_ult(void *arg)
{
	struct slice_work_extern *s = (struct slice_work_extern *)arg;
	char *buf = malloc(AGG_READ_SZ);
	if (!buf) {
		atomic_fetch_sub(s->pending, 1);
		return;
	}

	for (uint32_t i = 0; i < s->n; ++i) {
		uint32_t oid_idx = s->idx_list[i];
		daos_obj_id_t oid = s->oids[oid_idx];
		daos_unit_oid_t uoid = {.id_pub = oid, .id_shard = 6, .id_layout_ver = 2};
		uint64_t dkey_val = 1;
		char akey_val = '0';
		daos_key_t dkey;
		daos_iod_t iod = {0};
		daos_recx_t recx = {.rx_idx = 0, .rx_nr = AGG_READ_SZ};
		d_iov_t iov;
		d_sg_list_t sgl = {0};
		struct timespec t0, t1;
		int rc;

		d_iov_set(&dkey, &dkey_val, sizeof(dkey_val));
		d_iov_set(&iod.iod_name, &akey_val, 1);
		iod.iod_type  = DAOS_IOD_ARRAY;
		iod.iod_size  = 1;
		iod.iod_nr    = 1;
		iod.iod_recxs = &recx;
		d_iov_set(&iov, buf, AGG_READ_SZ);
		sgl.sg_nr = 1;
		sgl.sg_iovs = &iov;

		clock_gettime(CLOCK_MONOTONIC, &t0);
		if (s->do_write) {
			memset(buf, 0xBC, AGG_READ_SZ);
			uint32_t pm_ver = s->cont->sc_pool->spc_map_version;
			daos_epoch_t epoch = d_hlc_get();
			rc = vos_obj_update(s->cont->sc_hdl, uoid, epoch, pm_ver, 0,
					    &dkey, 1, &iod, NULL, &sgl);
		} else {
			rc = vos_obj_fetch(s->cont->sc_hdl, uoid, DAOS_EPOCH_MAX, 0,
					   &dkey, 1, &iod, &sgl);
		}
		clock_gettime(CLOCK_MONOTONIC, &t1);
		uint64_t elapsed = (t1.tv_sec - t0.tv_sec) * 1000000ULL +
				   (t1.tv_nsec - t0.tv_nsec) / 1000;

		if (rc == 0) {
			s->lat_out[s->success_count++] = elapsed;
		}
	}

	free(buf);
	atomic_fetch_sub(s->pending, 1);
}

static int
cont_lookup_ult(void *arg)
{
	uuid_t			 pool_uuid, cont_uuid;
	struct ds_cont_child	**cont_out = arg;

	uuid_parse(AGG_POOL_UUID, pool_uuid);
	uuid_parse(AGG_CONT_UUID, cont_uuid);
	return ds_cont_child_lookup(pool_uuid, cont_uuid, cont_out);
}

static void *
bench_thread_fn(void *arg)
{
	(void)arg;
	D_INFO("agg: bench thread started\n");
	sleep(15);

	/* For debug: load single test OID */
	daos_obj_id_t test_oid = {0};
	FILE *tf = fopen(AGG_TEST_OID_FILE, "rb");
	if (tf) {
		if (fread(&test_oid, sizeof(test_oid), 1, tf) != 1) {
			D_ERROR("agg: failed to read %s\n", AGG_TEST_OID_FILE);
		}
		fclose(tf);
		D_INFO("agg: test oid from file: hi=%lx lo=%lx\n", test_oid.hi, test_oid.lo);
	}

	struct ds_cont_child *cont = NULL;
	int rc = dss_ult_execute(cont_lookup_ult, &cont, NULL, NULL, DSS_XS_VOS, 0, 0);
	if (rc || !cont) {
		D_ERROR("agg: cont lookup rc=%d cont=%p\n", rc, cont);
		return NULL;
	}
	D_INFO("agg: cont ready sc_hdl=%p\n", (void *)cont->sc_hdl.cookie);

	/* Search for the test OID on each of the 8 targets */
	int found_tgt = -1;
	uint32_t found_shard = 0, found_lv = 0;
	for (int tgt = 0; tgt < 8 && !g_stop && found_tgt < 0; ++tgt) {
		struct fetch_arg fa = {.cont = cont, .oid = test_oid};
		rc = dss_ult_execute(iter_dkeys_ult, &fa, NULL, NULL, DSS_XS_VOS, tgt, 0);
		if ((fa.got_size & 0x8000000000000000ULL) == 0 && fa.got_size != 0) {
			found_tgt = tgt;
			found_shard = fa.first_byte;
			found_lv = (uint32_t)fa.elapsed_us;
			D_INFO("agg: SEARCH tgt=%d FOUND shard=%u layout_ver=%u\n",
			       tgt, found_shard, found_lv);
		} else {
			D_INFO("agg: SEARCH tgt=%d NOT_FOUND scanned=%llu\n",
			       tgt, (unsigned long long)(fa.got_size & 0x7FFFFFFFFFFFFFFFULL));
		}
	}

	if (found_tgt >= 0) {
		struct fetch_arg fa = {.cont = cont, .oid = test_oid,
				       .id_shard = found_shard,
				       .id_layout_ver = found_lv};
		rc = dss_ult_execute(fetch_one_ult, &fa, NULL, NULL, DSS_XS_VOS, found_tgt, 0);
		D_INFO("agg: ACTUAL_FETCH tgt=%d shard=%u lv=%u dispatch_rc=%d worker_rc=%d got_size=%lu first=0x%02x elapsed=%luus\n",
		       found_tgt, found_shard, found_lv, rc, fa.rc,
		       fa.got_size, fa.first_byte, fa.elapsed_us);
	}

	/* ===================================================================
	 * OPTION C: resolve placement via pl_obj_place (no iteration needed).
	 * For dfs chunk 0 (dkey=uint64(1)), d_hash_jump(1, 8) == 6 is constant,
	 * so id_shard=6 always. id_layout_ver=2 is container-wide.
	 * Only the target index varies per OID — we get it from pl_obj_place.
	 * =================================================================== */
	struct placement_cache {
		daos_obj_id_t	oid;
		int		tgt;
		uint32_t	id_shard;
		uint32_t	id_layout_ver;
	};

	daos_obj_id_t bravo_oid = {0};
	FILE *bf = fopen(AGG_TEST_OID_FILE_BRAVO, "rb");
	if (bf) { if (fread(&bravo_oid, sizeof(bravo_oid), 1, bf) != 1) {} fclose(bf); }

	daos_obj_id_t test_oids[2] = {test_oid, bravo_oid};
	for (int k = 0; k < 2; ++k) {
		daos_obj_id_t oid = test_oids[k];
		if (oid.hi == 0 && oid.lo == 0) continue;

		uuid_t pool_uuid;
		uuid_parse(AGG_POOL_UUID, pool_uuid);
		struct pl_map *pl_map = pl_map_find(pool_uuid, oid);
		if (!pl_map) { D_ERROR("agg: pl_map_find NULL for oid[%d]\n", k); continue; }

		struct daos_obj_md md = {0};
		md.omd_id = oid;
		md.omd_ver = 1;

		struct pl_obj_layout *layout = NULL;
		int prc = pl_obj_place(pl_map, 2, &md, 0, NULL, &layout);
		if (prc || !layout) {
			D_ERROR("agg: pl_obj_place oid[%d] rc=%d\n", k, prc);
			pl_map_decref(pl_map);
			continue;
		}

		/* Shard 6 for chunk-0 (d_hash_jump(dkey=1, grp_nr=8) = 6) */
		uint32_t target_for_shard6 = layout->ol_shards[6].po_index;
		uint32_t shard_val = layout->ol_shards[6].po_shard;
		D_INFO("agg: OPT_C oid[%d]=%lx/%lx shard6_target=%u shard_val=%u\n",
		       k, oid.hi, oid.lo, target_for_shard6, shard_val);

		pl_obj_layout_free(layout);
		pl_map_decref(pl_map);

		/* Now actually fetch via this placement */
		struct fetch_arg fa = {.cont = cont, .oid = oid,
				       .id_shard = 6, .id_layout_ver = 2};
		rc = dss_ult_execute(fetch_one_ult, &fa, NULL, NULL,
				     DSS_XS_VOS, (int)target_for_shard6, 0);
		D_INFO("agg: OPT_C_FETCH oid[%d] tgt=%u dispatch_rc=%d worker_rc=%d got_size=%lu first=0x%02x elapsed=%luus\n",
		       k, target_for_shard6, rc, fa.rc, fa.got_size, fa.first_byte, fa.elapsed_us);

		/* ============ WRITE TEST ============ */
		/* Overwrite with pattern 0xBC via vos_obj_update */
		struct fetch_arg wa = {.cont = cont, .oid = oid,
				       .id_shard = 6, .id_layout_ver = 2,
				       .first_byte = 0xBC};
		rc = dss_ult_execute(write_one_ult, &wa, NULL, NULL,
				     DSS_XS_VOS, (int)target_for_shard6, 0);
		D_INFO("agg: WRITE oid[%d] tgt=%u dispatch_rc=%d worker_rc=%d elapsed=%luus\n",
		       k, target_for_shard6, rc, wa.rc, wa.elapsed_us);

		/* Read back via our VOS path — should see 0xBC */
		struct fetch_arg rba = {.cont = cont, .oid = oid,
					.id_shard = 6, .id_layout_ver = 2};
		rc = dss_ult_execute(fetch_one_ult, &rba, NULL, NULL,
				     DSS_XS_VOS, (int)target_for_shard6, 0);
		D_INFO("agg: READBACK oid[%d] tgt=%u worker_rc=%d first=0x%02x elapsed=%luus (expect 0xbc)\n",
		       k, target_for_shard6, rba.rc, rba.first_byte, rba.elapsed_us);
	}

	/* ===================================================================
	 * PHASE 2d-bis: SELF-WRITE — 16 fresh OC_S1 OIDs, no dfs prepop.
	 * Triggered by /tmp/agg_phase == "selfwrite".
	 * Generates OIDs in-module, computes placement, writes 64 KiB each
	 * via the same write_one_ult+dss_ult_execute(DSS_XS_VOS, tgt) path.
	 * =================================================================== */
	{
		char phase_check[32] = {0};
		FILE *pcf = fopen("/tmp/agg_phase", "r");
		if (pcf) {
			if (fgets(phase_check, sizeof(phase_check), pcf)) {
				for (int i = 0; phase_check[i]; ++i)
					if (phase_check[i]=='\n' || phase_check[i]=='\r') phase_check[i]=0;
			}
			fclose(pcf);
		}
		if (strcmp(phase_check, "selfwrite") == 0) {
			D_INFO("agg: SELFWRITE starting 16 OC_S1 fresh-OID writes\n");
			uuid_t pool_uuid;
			uuid_parse(AGG_POOL_UUID, pool_uuid);
			int ok = 0;
			for (int i = 0; i < 16 && !g_stop; ++i) {
				daos_obj_id_t oid = { .lo = 0x1000ULL + i, .hi = 0 };
				int src = daos_obj_set_oid_by_class(&oid, DAOS_OT_ARRAY, OC_S1, 0);
				if (src != 0) {
					D_ERROR("agg: SW oid[%d] set_oid rc=%d\n", i, src);
					continue;
				}
				struct pl_map *pl_map = pl_map_find(pool_uuid, oid);
				if (!pl_map) { D_ERROR("agg: SW pl_map_find NULL [%d]\n", i); continue; }
				struct daos_obj_md md = {0};
				md.omd_id = oid; md.omd_ver = 1;
				struct pl_obj_layout *layout = NULL;
				int prc = pl_obj_place(pl_map, 2, &md, 0, NULL, &layout);
				pl_map_decref(pl_map);
				if (prc != 0 || !layout) {
					D_ERROR("agg: SW pl_obj_place [%d] rc=%d\n", i, prc);
					continue;
				}
				/* OC_S1 → single shard, shard 0 */
				uint32_t tgt = layout->ol_shards[0].po_index;
				uint32_t shard = layout->ol_shards[0].po_shard;
				pl_obj_layout_free(layout);

				struct fetch_arg wa = { .cont = cont, .oid = oid,
							.id_shard = shard, .id_layout_ver = 2,
							.first_byte = 0xA5 };
				int drc = dss_ult_execute(write_one_ult, &wa, NULL, NULL,
							  DSS_XS_VOS, (int)tgt, 0);
				D_INFO("agg: SW[%d] oid=%lx/%lx tgt=%u shard=%u dispatch=%d worker=%d us=%lu\n",
				       i, oid.hi, oid.lo, tgt, shard, drc, wa.rc, wa.elapsed_us);
				if (drc == 0 && wa.rc == 0) {
					/* readback */
					struct fetch_arg ra = { .cont = cont, .oid = oid,
								.id_shard = shard, .id_layout_ver = 2 };
					(void)dss_ult_execute(fetch_one_ult, &ra, NULL, NULL,
							      DSS_XS_VOS, (int)tgt, 0);
					D_INFO("agg: SW[%d] READBACK rc=%d first=0x%02x (expect 0xa5) us=%lu\n",
					       i, ra.rc, ra.first_byte, ra.elapsed_us);
					if (ra.rc == 0 && ra.first_byte == 0xA5) ok++;
				}
			}
			D_INFO("agg: SELFWRITE DONE success=%d/16\n", ok);
			goto bench_done;
		}

		/* =============================================================
		 * EMULATE — full local emulation of dfs_open + dfs_write.
		 * Triggered by /tmp/agg_phase == "emulate".
		 * For object name "local_obj_0000":
		 *   1. dfs_open_local  → vos_obj_update_ex on root dir (dentry)
		 *   2. dfs_write_local → vos_obj_update_ex on new OID (data)
		 * Reads /tmp/agg_dfs_roots.bin for root OID + oclass + chunk size
		 * (produced once post-container-create by get_dfs_roots helper).
		 * ============================================================= */
		int is_emu_write = (strcmp(phase_check, "emulate") == 0) ||
				   (strcmp(phase_check, "emulate_write") == 0);
		int is_emu_read  = (strcmp(phase_check, "emulate_read") == 0);

		if (is_emu_write || is_emu_read) {
			int N = 128;
			const char *nn = getenv("AGG_EMULATE_N");
			if (nn) N = atoi(nn);
			D_INFO("agg: EMULATE starting full-local dfs_open+dfs_write loop, N=%d\n", N);
			static char local_name[32];
			snprintf(local_name, sizeof(local_name), "local_obj_%lx", d_hlc_get() & 0xFFFFFFFF);

			daos_obj_id_t     root_oid = {0};
			daos_oclass_id_t  file_oclass = OC_S1;
			daos_size_t       chunk_size = 1048576;
			FILE *rf = fopen(AGG_ROOTS_FILE, "rb");
			if (!rf) {
				D_ERROR("agg: EMULATE cannot open %s — run get_dfs_roots first\n",
				        AGG_ROOTS_FILE);
				goto bench_done;
			}
			if (fread(&root_oid, sizeof(root_oid), 1, rf) != 1 ||
			    fread(&file_oclass, sizeof(file_oclass), 1, rf) != 1 ||
			    fread(&chunk_size, sizeof(chunk_size), 1, rf) != 1) {
				D_ERROR("agg: EMULATE bad %s format\n", AGG_ROOTS_FILE);
				fclose(rf);
				goto bench_done;
			}
			fclose(rf);
			D_INFO("agg: EMULATE loaded root_oid=%lx/%lx file_oclass=%u chunk_size=%lu\n",
			       root_oid.hi, root_oid.lo, file_oclass, (unsigned long)chunk_size);

			/* Resolve placement for root OID to find which target xstream
			 * owns it. For OC_RP_XSF (typical dir class) any replica shard
			 * is valid — pick shard 0. */
			uuid_t pool_uuid; uuid_parse(AGG_POOL_UUID, pool_uuid);
			struct pl_map *pl_map = pl_map_find(pool_uuid, root_oid);
			uint32_t root_tgt = 0, root_shard = 0, root_lv = 0;
			if (pl_map) {
				struct daos_obj_md md = {0};
				md.omd_id = root_oid; md.omd_ver = 1;
				struct pl_obj_layout *layout = NULL;
				int prc = pl_obj_place(pl_map, 2, &md, 0, NULL, &layout);
				pl_map_decref(pl_map);
				if (prc == 0 && layout) {
					root_tgt   = layout->ol_shards[0].po_index;
					root_shard = layout->ol_shards[0].po_shard;
					root_lv    = 2;
					pl_obj_layout_free(layout);
				}
			}
			D_INFO("agg: EMULATE root placement tgt=%u shard=%u lv=%u\n",
			       root_tgt, root_shard, root_lv);

			/* Load pre-created parent dirs from /tmp/agg_dfs_dirs.bin
			 * and precompute placement per dir. If the file is absent
			 * or empty, fall back to a single entry pointing at root. */
			static struct dir_info dirs[AGG_MAX_DIRS];
			int num_dirs = 0;
			FILE *df = fopen(AGG_DIRS_FILE, "rb");
			if (df) {
				uint32_t cnt32 = 0;
				if (fread(&cnt32, sizeof(cnt32), 1, df) == 1 &&
				    cnt32 > 0 && cnt32 <= AGG_MAX_DIRS) {
					for (uint32_t i = 0; i < cnt32; ++i) {
						daos_obj_id_t oid;
						char nm[32];
						if (fread(&oid, sizeof(oid), 1, df) != 1 ||
						    fread(nm, sizeof(nm), 1, df) != 1)
							break;
						struct dir_info *di = &dirs[num_dirs];
						memset(di, 0, sizeof(*di));
						di->oid = oid;
						memcpy(di->name, nm, sizeof(nm));
						di->lv       = 2;
						di->num_grps = 1;           /* safe default */
						di->grps[0].tgt   = 0;
						di->grps[0].shard = 0;

						struct pl_map *plx = pl_map_find(pool_uuid, oid);
						if (plx) {
							struct daos_obj_md mdx = {0};
							mdx.omd_id = oid; mdx.omd_ver = 1;
							struct pl_obj_layout *layx = NULL;
							if (pl_obj_place(plx, 2, &mdx, 0, NULL, &layx) == 0 && layx) {
								uint32_t ngrps = layx->ol_grp_nr;
								uint32_t gsize = layx->ol_grp_size;
								if (ngrps > AGG_MAX_DIR_GRPS) ngrps = AGG_MAX_DIR_GRPS;
								di->num_grps = ngrps;
								for (uint32_t g = 0; g < ngrps; ++g) {
									/* Leader of group g is shard 0 of that
									 * group → index g * grp_size. */
									uint32_t sh = g * gsize;
									di->grps[g].tgt   = layx->ol_shards[sh].po_index;
									di->grps[g].shard = layx->ol_shards[sh].po_shard;
								}
								pl_obj_layout_free(layx);
							}
							pl_map_decref(plx);
						}
						D_INFO("agg: DIR[%d] name='%s' oid=%lx/%lx num_grps=%u lv=%u\n",
						       num_dirs, di->name,
						       oid.hi, oid.lo, di->num_grps, di->lv);
						for (uint32_t g = 0; g < di->num_grps && g < 16; ++g) {
							D_INFO("agg: DIR[%d] grp[%u] tgt=%u shard=%u\n",
							       num_dirs, g, di->grps[g].tgt, di->grps[g].shard);
						}
						num_dirs++;
					}
				}
				fclose(df);
			}
			if (num_dirs == 0) {
				/* Fallback: single "dir" = container root */
				memset(&dirs[0], 0, sizeof(dirs[0]));
				dirs[0].oid      = root_oid;
				dirs[0].num_grps = 1;
				dirs[0].grps[0].tgt   = root_tgt;
				dirs[0].grps[0].shard = root_shard;
				dirs[0].lv       = root_lv;
				strncpy(dirs[0].name, "<root>", sizeof(dirs[0].name) - 1);
				num_dirs = 1;
				D_INFO("agg: DIRS file missing or empty — using container root only\n");
			}

			int T = 1;
			const char *te = getenv("AGG_EMULATE_THREADS");
			if (te) T = atoi(te);
			if (T < 1) T = 1;

			int iodepth = 1;
			const char *iod_env = getenv("AGG_EMULATE_IODEPTH");
			if (iod_env) iodepth = atoi(iod_env);
			if (iodepth < 1)  iodepth = 1;
			if (iodepth > 64) iodepth = 64;   /* sanity cap */

			if (is_emu_write) {
				size_t emu_size = 0;
				const char *sz_env = getenv("AGG_EMULATE_SIZE");
				if (sz_env) emu_size = strtoull(sz_env, NULL, 10);
				D_INFO("agg: EMUWRITE PAR(dynamic) threads=%d iodepth=%d N=%d write_size=%zu\n",
				       T, iodepth, N, emu_size ? emu_size : (size_t)AGG_READ_SZ);

				struct parallel_worker_arg *wargs = calloc(T, sizeof(*wargs));
				pthread_t *wthreads = calloc(T, sizeof(pthread_t));
				struct written_oid_entry *table = calloc(N, sizeof(*table));
				uint64_t t_start = d_hlc_get();
				struct timespec wall_a, wall_b;
				clock_gettime(CLOCK_MONOTONIC, &wall_a);

				struct dispatch_state ds;
				atomic_init(&ds.next_iter, 0);
				ds.total_iters = N;

				for (int tid = 0; tid < T; ++tid) {
					wargs[tid].tid         = tid;
					wargs[tid].ds          = &ds;
					wargs[tid].dirs        = dirs;
					wargs[tid].num_dirs    = num_dirs;
					wargs[tid].iodepth     = iodepth;
					wargs[tid].file_oclass = file_oclass;
					wargs[tid].chunk_size  = chunk_size;
					wargs[tid].cont        = cont;
					wargs[tid].name_hlc    = t_start & 0xFFFFFFFF;
					wargs[tid].oid_table   = table;
					wargs[tid].write_size  = emu_size;
					uuid_copy(wargs[tid].pool_uuid, pool_uuid);
					pthread_create(&wthreads[tid], NULL,
						       emulate_parallel_worker, &wargs[tid]);
				}

				int total_success = 0;
				for (int tid = 0; tid < T; ++tid) {
					pthread_join(wthreads[tid], NULL);
					total_success += wargs[tid].success;
				}

				clock_gettime(CLOCK_MONOTONIC, &wall_b);
				uint64_t wall_us = (wall_b.tv_sec - wall_a.tv_sec) * 1000000ULL +
						   (wall_b.tv_nsec - wall_a.tv_nsec) / 1000;
				D_INFO("agg: EMUWRITE DONE T=%d N=%d success=%d wall_us=%lu\n",
				       T, N, total_success, wall_us);

				/* Persist OID table for subsequent read phase */
				FILE *of = fopen(AGG_EMUWRITE_OIDS, "wb");
				if (of) {
					uint64_t cnt = total_success;
					fwrite(&cnt, sizeof(cnt), 1, of);
					for (int i = 0; i < N; ++i) {
						if (table[i].oid.hi || table[i].oid.lo) {
							fwrite(&table[i], sizeof(table[i]), 1, of);
						}
					}
					fclose(of);
					D_INFO("agg: EMUWRITE saved %lu OIDs to %s\n",
					       cnt, AGG_EMUWRITE_OIDS);
				}

				free(table);
				free(wthreads);
				free(wargs);
			} else {
				/* emulate_read */
				FILE *rf2 = fopen(AGG_EMUWRITE_OIDS, "rb");
				if (!rf2) {
					D_ERROR("agg: EMUREAD cannot open %s\n", AGG_EMUWRITE_OIDS);
					goto bench_done;
				}
				uint64_t cnt = 0;
				if (fread(&cnt, sizeof(cnt), 1, rf2) != 1) {
					D_ERROR("agg: EMUREAD bad format\n");
					fclose(rf2); goto bench_done;
				}
				struct written_oid_entry *table = calloc(cnt, sizeof(*table));
				if (fread(table, sizeof(*table), cnt, rf2) != cnt) {
					D_ERROR("agg: EMUREAD truncated OID file\n");
					fclose(rf2); free(table); goto bench_done;
				}
				fclose(rf2);
				if (!g_gather_buf) {
					D_ERROR("agg: EMUREAD gather_buf is NULL (setup failed)\n");
					free(table); goto bench_done;
				}
				size_t emu_read_size = 0;
				const char *sz_r_env = getenv("AGG_EMULATE_SIZE");
				if (sz_r_env) emu_read_size = strtoull(sz_r_env, NULL, 10);
				size_t effective_rsz = emu_read_size ? emu_read_size : (size_t)AGG_READ_SZ;
				unsigned eff_slots = (unsigned)(AGG_GATHER_BUF_SZ / effective_rsz);
				D_INFO("agg: EMUREAD PAR(dynamic) threads=%d iodepth=%d N=%lu read_size=%zu gather_buf=%p slots=%u\n",
				       T, iodepth, cnt, effective_rsz, g_gather_buf, eff_slots);

				struct read_worker_arg *rargs = calloc(T, sizeof(*rargs));
				pthread_t *rthreads = calloc(T, sizeof(pthread_t));
				struct timespec wall_a, wall_b;
				clock_gettime(CLOCK_MONOTONIC, &wall_a);

				struct dispatch_state ds;
				atomic_init(&ds.next_iter, 0);
				ds.total_iters = (int)cnt;

				for (int tid = 0; tid < T; ++tid) {
					rargs[tid].tid        = tid;
					rargs[tid].ds         = &ds;
					rargs[tid].dirs       = dirs;
					rargs[tid].num_dirs   = num_dirs;
					rargs[tid].iodepth    = iodepth;
					rargs[tid].read_size  = emu_read_size;
					rargs[tid].oid_table  = table;
					rargs[tid].gather_buf = g_gather_buf;
					uuid_copy(rargs[tid].pool_uuid, pool_uuid);
					pthread_create(&rthreads[tid], NULL,
						       emulate_parallel_read_worker, &rargs[tid]);
				}

				int total_success = 0;
				for (int tid = 0; tid < T; ++tid) {
					pthread_join(rthreads[tid], NULL);
					total_success += rargs[tid].success;
				}

				clock_gettime(CLOCK_MONOTONIC, &wall_b);
				uint64_t wall_us = (wall_b.tv_sec - wall_a.tv_sec) * 1000000ULL +
						   (wall_b.tv_nsec - wall_a.tv_nsec) / 1000;
				D_INFO("agg: EMUREAD DONE T=%d N=%lu success=%d wall_us=%lu\n",
				       T, cnt, total_success, wall_us);

				free(table);
				free(rthreads);
				free(rargs);
			}
			goto bench_done;
		}
	}

	/* ===================================================================
	 * PHASE 2e: 16-way parallel benchmark (write or read phase)
	 * Phase selected by /tmp/agg_phase file: "write" or "read"
	 * =================================================================== */
	{
		FILE *bench_f = fopen(AGG_BENCH_OID_FILE, "rb");
		if (bench_f) {
			uint64_t cnt;
			if (fread(&cnt, sizeof(cnt), 1, bench_f) != 1) {
				D_ERROR("agg: BENCH failed to read count\n");
				fclose(bench_f);
			} else {
				daos_obj_id_t *oids = calloc(cnt, sizeof(daos_obj_id_t));
				uint64_t loaded = 0;
				for (uint64_t i = 0; i < cnt; ++i) {
					if (fread(&oids[i], sizeof(daos_obj_id_t), 1, bench_f) != 1) break;
					uint32_t nl;
					if (fread(&nl, sizeof(nl), 1, bench_f) != 1) break;
					char skip[64];
					if (nl > sizeof(skip)) nl = sizeof(skip);
					if (fread(skip, 1, nl, bench_f) != nl) break;
					loaded++;
				}
				fclose(bench_f);
				D_INFO("agg: BENCH loaded %lu/%lu OIDs from %s\n",
				       loaded, cnt, AGG_BENCH_OID_FILE);

				/* Placement resolution (Option C) per OID */
				uuid_t pool_uuid;
				uuid_parse(AGG_POOL_UUID, pool_uuid);
				int *tgts = calloc(loaded, sizeof(int));
				for (uint64_t i = 0; i < loaded; ++i) {
					struct pl_map *pl_map = pl_map_find(pool_uuid, oids[i]);
					if (!pl_map) { tgts[i] = -1; continue; }
					struct daos_obj_md md = {0};
					md.omd_id = oids[i]; md.omd_ver = 1;
					struct pl_obj_layout *layout = NULL;
					int prc = pl_obj_place(pl_map, 2, &md, 0, NULL, &layout);
					if (prc == 0 && layout) {
						tgts[i] = (int)layout->ol_shards[6].po_index;
						pl_obj_layout_free(layout);
					} else {
						tgts[i] = -1;
					}
					pl_map_decref(pl_map);
				}

				/* Read phase flag */
				char phase[16] = "read";
				FILE *pf = fopen("/tmp/agg_phase", "r");
				if (pf) {
					if (fgets(phase, sizeof(phase), pf)) {
						for (int i = 0; phase[i]; ++i)
							if (phase[i]=='\n' || phase[i]=='\r') phase[i]=0;
					}
					fclose(pf);
				}
				int do_write = (strcmp(phase, "write") == 0);

				/* Phase 1 cap: env AGG_BENCH_MAX_N bounds the loop so we can
				 * validate single-OID writes before scaling to 2048. */
				uint64_t bench_max = loaded;
				const char *cap = getenv("AGG_BENCH_MAX_N");
				if (cap) {
					uint64_t v = strtoull(cap, NULL, 10);
					if (v > 0 && v < bench_max) bench_max = v;
				}
				D_INFO("agg: BENCH phase='%s' do_write=%d max_n=%lu loaded=%lu\n",
				       phase, do_write, bench_max, loaded);

				/* TRUE single-thread mode: each op via synchronous dss_ult_execute.
				 * Only one ULT active across the whole benchmark — WAL-safe. */
				uint64_t *flat = calloc(loaded, sizeof(uint64_t));
				uint64_t total_success = 0;
				struct timespec ws, we;
				clock_gettime(CLOCK_MONOTONIC, &ws);
				for (uint64_t i = 0; i < bench_max && !g_stop; ++i) {
					if (tgts[i] < 0) continue;
					struct fetch_arg fa = {.cont = cont, .oid = oids[i],
							       .id_shard = 6, .id_layout_ver = 2,
							       .first_byte = 0xBC};
					int drc;
					if (do_write)
						drc = dss_ult_execute(write_one_ult, &fa, NULL, NULL,
								      DSS_XS_VOS, tgts[i], 0);
					else
						drc = dss_ult_execute(fetch_one_ult, &fa, NULL, NULL,
								      DSS_XS_VOS, tgts[i], 0);
					if (drc == 0 && fa.rc == 0)
						flat[total_success++] = fa.elapsed_us;
				}
				clock_gettime(CLOCK_MONOTONIC, &we);
				uint64_t wall_us = (we.tv_sec - ws.tv_sec) * 1000000ULL +
						   (we.tv_nsec - ws.tv_nsec) / 1000;

				/* Sort */
				for (uint64_t i = 1; i < total_success; ++i) {
					uint64_t v = flat[i]; int64_t j = (int64_t)i - 1;
					while (j >= 0 && flat[j] > v) { flat[j + 1] = flat[j]; j--; }
					flat[j + 1] = v;
				}
				uint64_t p50 = total_success ? flat[total_success * 50 / 100] : 0;
				uint64_t p90 = total_success ? flat[total_success * 90 / 100] : 0;
				uint64_t p99 = total_success ? flat[total_success * 99 / 100] : 0;
				uint64_t lmin = total_success ? flat[0] : 0;
				uint64_t lmax = total_success ? flat[total_success - 1] : 0;
				uint64_t bytes = total_success * AGG_READ_SZ;
				double bw_mbs = bytes / 1048576.0 / (wall_us / 1000000.0);

				D_INFO("agg: BENCH phase=%s RESULT success=%lu/%lu wall=%luus throughput=%.1fMB/s mode=serial\n",
				       phase, total_success, loaded, wall_us, bw_mbs);
				D_INFO("agg: BENCH phase=%s LATENCY(us) min=%lu p50=%lu p90=%lu p99=%lu max=%lu\n",
				       phase, lmin, p50, p90, p99, lmax);

				free(flat);
				free(tgts);
				free(oids);
			}
		} else {
			D_INFO("agg: BENCH no %s, skipping\n", AGG_BENCH_OID_FILE);
		}
	}

bench_done:
	D_INFO("agg: bench thread done\n");
	return NULL;
}

static int agg_sidecar_init(void)
{
	/* Override hardcoded UUID defaults from env vars (passed via
	 * daos_server_debug.yml engines[0].env_vars) so redeploying a new
	 * pool/container doesn't require a C-source edit + rebuild. */
	const char *ep = getenv("AGG_POOL_UUID");
	const char *ec = getenv("AGG_CONT_UUID");
	if (ep && strlen(ep) == 36) {
		memcpy(g_pool_uuid, ep, 36);
		g_pool_uuid[36] = '\0';
	}
	if (ec && strlen(ec) == 36) {
		memcpy(g_cont_uuid, ec, 36);
		g_cont_uuid[36] = '\0';
	}
	D_INFO("agg_sidecar init pool_uuid=%s cont_uuid=%s\n",
	       g_pool_uuid, g_cont_uuid);
	return 0;
}

static int agg_sidecar_setup(void)
{
	D_INFO("agg_sidecar setup - alloc gather buffer + spawn bench\n");

	/* 256 MiB gather buffer — try 2 MiB hugepages first, fall back to 4K. */
	g_gather_buf = mmap(NULL, AGG_GATHER_BUF_SZ,
			    PROT_READ | PROT_WRITE,
			    MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
			    -1, 0);
	if (g_gather_buf == MAP_FAILED) {
		D_WARN("agg: hugepage mmap failed (errno=%d), falling back to 4K pages\n",
		       errno);
		g_gather_buf = NULL;
		if (posix_memalign(&g_gather_buf, 4096, AGG_GATHER_BUF_SZ) != 0) {
			D_ERROR("agg: posix_memalign failed\n");
			return -DER_NOMEM;
		}
		g_gather_buf_is_hugepage = 0;
	} else {
		g_gather_buf_is_hugepage = 1;
	}
	memset(g_gather_buf, 0, AGG_GATHER_BUF_SZ);   /* page in */
	D_INFO("agg: gather_buf=%p size=%zu hugepage=%d slots=%u\n",
	       g_gather_buf, (size_t)AGG_GATHER_BUF_SZ,
	       g_gather_buf_is_hugepage, (unsigned)AGG_GATHER_SLOTS);

	int rc = pthread_create(&g_bench_thread, NULL, bench_thread_fn, NULL);
	if (rc) { D_ERROR("pthread_create rc=%d\n", rc); return -DER_MISC; }
	return 0;
}

static int agg_sidecar_cleanup(void)
{
	g_stop = 1;
	if (g_bench_thread) pthread_join(g_bench_thread, NULL);
	if (g_map.oids) { free(g_map.oids); g_map.oids = NULL; }
	if (g_gather_buf) {
		if (g_gather_buf_is_hugepage)
			munmap(g_gather_buf, AGG_GATHER_BUF_SZ);
		else
			free(g_gather_buf);
		g_gather_buf = NULL;
	}
	return 0;
}

static int agg_sidecar_fini(void)
{
	D_INFO("agg_sidecar fini\n");
	return 0;
}

struct dss_module agg_sidecar_module = {
	.sm_name	= "agg_sidecar",
	.sm_mod_id	= DAOS_AGG_SIDECAR_MODULE,
	.sm_ver		= 1,
	.sm_init	= agg_sidecar_init,
	.sm_setup	= agg_sidecar_setup,
	.sm_cleanup	= agg_sidecar_cleanup,
	.sm_fini	= agg_sidecar_fini,
	.sm_proto_count	= 0,
};
