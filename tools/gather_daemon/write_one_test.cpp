// write_one_test: create one dfs file with known content, verify via dfs_read,
// and persist its oid so the agg_sidecar module can iterate-and-match.
#include <daos.h>
#include <daos_fs.h>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>

int main(int argc, char **argv) {
    const char *pool = "Pool1", *cont = "lmcache_daemon_64k";
    const char *name = (argc > 1) ? argv[1] : "test_agg_key";
    const char *outfile = (argc > 2) ? argv[2] : "/tmp/agg_test_oid.bin";
    const size_t sz = 65536;

    if (daos_init() != 0) { fprintf(stderr, "daos_init\n"); return 1; }
    daos_handle_t poh{}, coh{};
    if (daos_pool_connect(pool, nullptr, DAOS_PC_RW, &poh, nullptr, nullptr) != 0) { fprintf(stderr, "pool_connect\n"); return 2; }
    if (daos_cont_open(poh, cont, DAOS_COO_RW, &coh, nullptr, nullptr) != 0) { fprintf(stderr, "cont_open\n"); return 3; }
    dfs_t *dfs = nullptr;
    if (dfs_mount(poh, coh, O_RDWR, &dfs) != 0) { fprintf(stderr, "dfs_mount\n"); return 4; }

    // Write 64 KiB of 0xA5
    dfs_obj_t *obj = nullptr;
    int rc = dfs_open(dfs, nullptr, name, S_IFREG | 0644,
                      O_RDWR | O_CREAT | O_TRUNC, 0, 0, nullptr, &obj);
    if (rc) { fprintf(stderr, "dfs_open(create) rc=%d\n", rc); return 5; }

    char buf[65536];
    memset(buf, 0xA5, sz);
    d_iov_t iov; d_iov_set(&iov, buf, sz);
    d_sg_list_t sgl{}; sgl.sg_nr = 1; sgl.sg_iovs = &iov;
    rc = dfs_write(dfs, obj, &sgl, 0, nullptr);
    if (rc) { fprintf(stderr, "dfs_write rc=%d\n", rc); return 6; }

    daos_obj_id_t oid{};
    dfs_obj2id(obj, &oid);
    dfs_release(obj);
    printf("WROTE name=%s oid.hi=%lx oid.lo=%lx\n", name, oid.hi, oid.lo);

    // Verify via dfs_read
    rc = dfs_lookup_rel(dfs, nullptr, name, O_RDONLY, &obj, nullptr, nullptr);
    if (rc || !obj) { fprintf(stderr, "lookup rc=%d\n", rc); return 7; }
    char rbuf[65536]{};
    d_iov_set(&iov, rbuf, sz);
    sgl.sg_nr = 1; sgl.sg_iovs = &iov;
    daos_size_t got = sz;
    rc = dfs_read(dfs, obj, &sgl, 0, &got, nullptr);
    dfs_release(obj);
    if (rc) { fprintf(stderr, "dfs_read rc=%d\n", rc); return 8; }
    printf("DFS_READ verify: got=%lu first=0x%02x last=0x%02x all_match_A5=%s\n",
           got, (unsigned char)rbuf[0], (unsigned char)rbuf[sz-1],
           (rbuf[0] == (char)0xA5 && rbuf[sz-1] == (char)0xA5) ? "YES" : "NO");

    // Save oid for module
    FILE *f = fopen(outfile, "wb");
    if (f) {
        fwrite(&oid, sizeof(oid), 1, f);
        fclose(f);
        printf("oid written to /tmp/agg_test_oid.bin\n");
    }

    dfs_umount(dfs);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();
    return 0;
}
