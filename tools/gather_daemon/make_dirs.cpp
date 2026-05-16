// make_dirs: create N subdirectories under a DFS container root and dump
// their OIDs + names to a binary file for the agg_sidecar bench to consume.
//
// File format (little-endian, host byte order):
//   [u32 count]
//   count × {
//       daos_obj_id_t oid   (16 bytes)
//       char          name[32]
//   }
//
// Usage:
//   make_dirs --pool Pool1 --cont testcont --num 8 --prefix par --out /tmp/agg_dfs_dirs.bin

#include <daos.h>
#include <daos_fs.h>
#include <daos_obj_class.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>

int main(int argc, char **argv) {
    std::string pool = "Pool1", cont = "testcont";
    std::string prefix = "par";
    int              num    = 8;
    const char      *out    = "/tmp/agg_dfs_dirs.bin";
    daos_oclass_id_t oclass = 0;                 /* 0 → container default */

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w) {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing arg for %s\n", w); std::exit(1); }
            return argv[++i];
        };
        if      (a == "--pool")   pool = nxt("--pool");
        else if (a == "--cont")   cont = nxt("--cont");
        else if (a == "--prefix") prefix = nxt("--prefix");
        else if (a == "--num")    num = std::atoi(nxt("--num"));
        else if (a == "--out")    out = nxt("--out");
        else if (a == "--oclass") {
            std::string v = nxt("--oclass");
            if      (v == "SX" || v == "OC_SX") oclass = OC_SX;
            else if (v == "S1" || v == "OC_S1") oclass = OC_S1;
            else if (v == "S2" || v == "OC_S2") oclass = OC_S2;
            else if (v == "S4" || v == "OC_S4") oclass = OC_S4;
            else { std::fprintf(stderr, "unknown --oclass %s\n", v.c_str()); std::exit(1); }
        }
    }

    if (daos_init() != 0) { std::fprintf(stderr, "daos_init\n"); return 1; }

    daos_handle_t poh{}, coh{};
    if (daos_pool_connect(pool.c_str(), nullptr, DAOS_PC_RW, &poh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "pool_connect %s\n", pool.c_str()); return 2;
    }
    if (daos_cont_open(poh, cont.c_str(), DAOS_COO_RW, &coh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "cont_open %s\n", cont.c_str()); return 3;
    }

    dfs_t *dfs = nullptr;
    if (dfs_mount(poh, coh, O_RDWR, &dfs) != 0) {
        std::fprintf(stderr, "dfs_mount\n"); return 4;
    }

    FILE *f = std::fopen(out, "wb");
    if (!f) { std::fprintf(stderr, "fopen %s\n", out); return 5; }
    uint32_t cnt32 = (uint32_t)num;
    std::fwrite(&cnt32, sizeof(cnt32), 1, f);

    for (int i = 0; i < num; ++i) {
        char name[32];
        std::snprintf(name, sizeof(name), "%s%02d", prefix.c_str(), i);

        // mkdir (idempotent — ignore EEXIST so reruns work)
        int mk_rc = dfs_mkdir(dfs, nullptr, name, S_IFDIR | 0755, oclass);
        if (mk_rc != 0 && mk_rc != EEXIST) {
            std::fprintf(stderr, "dfs_mkdir %s rc=%d\n", name, mk_rc);
            return 6;
        }

        // lookup to get the obj handle, then extract OID
        dfs_obj_t *dobj = nullptr;
        mode_t mode = 0;
        if (dfs_lookup_rel(dfs, nullptr, name, O_RDWR, &dobj, &mode, nullptr) != 0) {
            std::fprintf(stderr, "dfs_lookup_rel %s\n", name); return 7;
        }
        daos_obj_id_t oid = {};
        if (dfs_obj2id(dobj, &oid) != 0) {
            std::fprintf(stderr, "dfs_obj2id %s\n", name); return 8;
        }
        dfs_release(dobj);

        std::fwrite(&oid, sizeof(oid), 1, f);
        char name_pad[32] = {};
        std::strncpy(name_pad, name, sizeof(name_pad) - 1);
        std::fwrite(name_pad, sizeof(name_pad), 1, f);

        std::printf("dir[%d] name=%s oid=%lx/%lx mkdir_rc=%d\n",
                    i, name, oid.hi, oid.lo, mk_rc);
    }
    std::fclose(f);

    dfs_umount(dfs);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();

    std::printf("wrote %d dir OIDs → %s\n", num, out);
    return 0;
}
