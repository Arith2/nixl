// dump_oids: connects as dfs client, resolves N keys to oid + cont uuid,
// writes to a binary file for the agg_sidecar module to read.
// Format: [u64 count][for each: u128 oid][u32 namelen][name]...
#include <daos.h>
#include <daos_fs.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <string>

int main(int argc, char **argv) {
    std::string pool = "Pool1", cont = "lmcache_daemon_64k";
    std::string key_prefix = "kv_65536B_";
    int count = 2048, width = 7;
    const char *outfile = "/tmp/agg_oid_map.bin";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w){ if(i+1>=argc){std::fprintf(stderr,"missing %s\n",w);std::exit(1);} return argv[++i]; };
        if      (a == "--pool") pool = nxt("--pool");
        else if (a == "--cont") cont = nxt("--cont");
        else if (a == "--key-prefix") key_prefix = nxt("--key-prefix");
        else if (a == "--count") count = std::atoi(nxt("--count"));
        else if (a == "--width") width = std::atoi(nxt("--width"));
        else if (a == "--out") outfile = nxt("--out");
    }

    if (daos_init() != 0) { std::fprintf(stderr,"daos_init failed\n"); return 1; }
    daos_handle_t poh{}, coh{};
    if (daos_pool_connect(pool.c_str(), nullptr, DAOS_PC_RO, &poh, nullptr, nullptr) != 0) { std::fprintf(stderr,"pool_connect\n"); return 2; }
    if (daos_cont_open(poh, cont.c_str(), DAOS_COO_RO, &coh, nullptr, nullptr) != 0) { std::fprintf(stderr,"cont_open\n"); return 3; }
    dfs_t *dfs = nullptr;
    if (dfs_mount(poh, coh, O_RDONLY, &dfs) != 0) { std::fprintf(stderr,"dfs_mount\n"); return 4; }

    FILE *f = std::fopen(outfile, "wb");
    if (!f) { std::fprintf(stderr,"fopen %s\n", outfile); return 5; }
    uint64_t cnt64 = count;
    std::fwrite(&cnt64, sizeof(cnt64), 1, f);

    char kbuf[64];
    int ok = 0;
    for (int i = 0; i < count; ++i) {
        std::snprintf(kbuf, sizeof(kbuf), "%s%0*d", key_prefix.c_str(), width, i);
        dfs_obj_t *obj = nullptr;
        int rc = dfs_lookup_rel(dfs, nullptr, kbuf, O_RDONLY, &obj, nullptr, nullptr);
        if (rc || !obj) { std::fprintf(stderr,"lookup_rel(%s) rc=%d\n", kbuf, rc); continue; }
        daos_obj_id_t oid{};
        rc = dfs_obj2id(obj, &oid);
        dfs_release(obj);
        if (rc) { std::fprintf(stderr,"obj2id rc=%d\n", rc); continue; }
        std::fwrite(&oid, sizeof(oid), 1, f);
        uint32_t nl = (uint32_t)std::strlen(kbuf);
        std::fwrite(&nl, sizeof(nl), 1, f);
        std::fwrite(kbuf, 1, nl, f);
        ok++;
    }
    std::fclose(f);
    std::printf("wrote %d/%d oids to %s\n", ok, count, outfile);

    dfs_umount(dfs);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();
    return (ok == count) ? 0 : 1;
}
