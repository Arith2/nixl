# gather_daemon — build with DAOS client libs + libibverbs
#
# Run `make` on a node with both DAOS install and libibverbs-dev.
#   make            -> gather_daemon (server, needs DAOS)
#   make test_client -> test_client (no DAOS deps, only libibverbs + httplib)

DAOS_ROOT := $(HOME)/daos/install
CXX       := g++
CXXFLAGS  := -std=c++17 -O2 -Wall -Wextra -pthread -Wno-unused-parameter
INCLUDES  := -I$(DAOS_ROOT)/include -I.
DAOS_LIBS := -L$(DAOS_ROOT)/lib64 -Wl,-rpath,$(DAOS_ROOT)/lib64 \
             -ldaos -ldaos_common -ldfs -lgurt -lcart -luuid
IB_LIBS   := -libverbs
COMMON_LIBS := -lpthread

.PHONY: all clean
all: gather_daemon test_client prepop_key dfs_bench get_dfs_roots make_dirs

gather_daemon: main.cpp agg_proto.h rdma_common.h third_party/httplib.h third_party/json.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ main.cpp $(DAOS_LIBS) $(IB_LIBS) $(COMMON_LIBS)

dfs_bench: dfs_bench.cpp
	$(CXX) $(CXXFLAGS) -I$(DAOS_ROOT)/include -o $@ dfs_bench.cpp $(DAOS_LIBS) $(COMMON_LIBS)

# libcurl: requires a build with CURLOPT_AWS_SIGV4 (curl >= 7.75). If your
# distro's libcurl4 package lacks the dev headers / SIGV4 support, point
# CURL_ROOT at a local install (e.g. a conda or vcpkg prefix).
CURL_ROOT ?= /usr
CURL_INC  := -I$(CURL_ROOT)/include
CURL_LIBS := -L$(CURL_ROOT)/lib -Wl,-rpath,$(CURL_ROOT)/lib -lcurl

test_client: test_client.cpp agg_proto.h rdma_common.h third_party/httplib.h third_party/json.hpp
	$(CXX) $(CXXFLAGS) -I. $(CURL_INC) -o $@ test_client.cpp $(IB_LIBS) $(CURL_LIBS) $(COMMON_LIBS)

prepop_key: prepop_key.cpp
	$(CXX) $(CXXFLAGS) -I$(DAOS_ROOT)/include -o $@ prepop_key.cpp $(DAOS_LIBS) $(COMMON_LIBS)

get_dfs_roots: get_dfs_roots.cpp
	$(CXX) $(CXXFLAGS) -I$(DAOS_ROOT)/include -o $@ get_dfs_roots.cpp $(DAOS_LIBS) $(COMMON_LIBS)

make_dirs: make_dirs.cpp
	$(CXX) $(CXXFLAGS) -I$(DAOS_ROOT)/include -o $@ make_dirs.cpp $(DAOS_LIBS) $(COMMON_LIBS)

clean:
	rm -f gather_daemon test_client prepop_key dfs_bench get_dfs_roots make_dirs
