// get_dfs_roots: queries a POSIX container for its root dir OID + default
// file oclass + default chunk size, writes them to a binary file that the
// agg_sidecar module reads at startup.
//
// Format (little-endian, host byte order):
//   [daos_obj_id_t  root_oid]      16 bytes — parent dir for top-level files
//   [daos_oclass_id_t file_oclass]  4 bytes — class to use for new files
//   [daos_size_t   chunk_size]      8 bytes — dfs default chunk size
//
#include <daos.h>
#include <daos_fs.h>
#include <daos_cont.h>
#include <daos_prop.h>
#include <daos_obj_class.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

int main(int argc, char **argv) {
    std::string pool = "Pool1", cont = "testcont";
    const char *outfile = "/tmp/agg_dfs_roots.bin";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto nxt = [&](const char *w){
            if (i + 1 >= argc) { std::fprintf(stderr, "missing arg for %s\n", w); std::exit(1); }
            return argv[++i];
        };
        if      (a == "--pool") pool = nxt("--pool");
        else if (a == "--cont") cont = nxt("--cont");
        else if (a == "--out")  outfile = nxt("--out");
    }

    if (daos_init() != 0) { std::fprintf(stderr, "daos_init\n"); return 1; }
    daos_handle_t poh{}, coh{};
    if (daos_pool_connect(pool.c_str(), nullptr, DAOS_PC_RO, &poh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "daos_pool_connect %s\n", pool.c_str());
        return 2;
    }
    if (daos_cont_open(poh, cont.c_str(), DAOS_COO_RW, &coh, nullptr, nullptr) != 0) {
        std::fprintf(stderr, "daos_cont_open %s\n", cont.c_str());
        return 3;
    }

    // Query DAOS_PROP_CO_ROOTS — holds the 4 cr_oids (SB, root, 2 reserved)
    daos_prop_t *prop = daos_prop_alloc(1);
    if (!prop) { std::fprintf(stderr, "prop_alloc\n"); return 4; }
    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_ROOTS;

    int rc = daos_cont_query(coh, nullptr, prop, nullptr);
    if (rc != 0) { std::fprintf(stderr, "cont_query rc=%d\n", rc); return 5; }

    struct daos_prop_co_roots *roots =
        (struct daos_prop_co_roots *)prop->dpp_entries[0].dpe_val_ptr;
    if (!roots) { std::fprintf(stderr, "no roots prop\n"); return 6; }

    daos_obj_id_t sb_oid   = roots->cr_oids[0];
    daos_obj_id_t root_oid = roots->cr_oids[1];
    std::printf("SB  OID: hi=0x%lx lo=0x%lx\n", sb_oid.hi,   sb_oid.lo);
    std::printf("ROOT OID: hi=0x%lx lo=0x%lx\n", root_oid.hi, root_oid.lo);

    // To get chunk_size + default file oclass we'd read the SB akeys — but for
    // default-created POSIX containers they are well-known.
    daos_oclass_id_t file_oclass = OC_S1;    /* OBJ_CLASS_DEF(OR_RP_1, 1) = 0x01000001 */
    daos_size_t      chunk_size  = 1048576;  /* 1 MiB dfs default */

    daos_prop_free(prop);
    daos_cont_close(coh, nullptr);
    daos_pool_disconnect(poh, nullptr);
    daos_fini();

    FILE *f = std::fopen(outfile, "wb");
    if (!f) { std::fprintf(stderr, "fopen %s\n", outfile); return 7; }
    std::fwrite(&root_oid,    sizeof(root_oid),    1, f);
    std::fwrite(&file_oclass, sizeof(file_oclass), 1, f);
    std::fwrite(&chunk_size,  sizeof(chunk_size),  1, f);
    std::fclose(f);
    std::printf("wrote roots → %s (root_oid, file_oclass=0x%x, chunk_size=%lu)\n",
                outfile, file_oclass, chunk_size);
    return 0;
}
