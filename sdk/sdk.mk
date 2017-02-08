# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# We extract header files for the development of external plugins, until such
# time as we have a fully separate API.  We start with a set of headers we
# want to include, and we add to those a list of dependency headers that are
# required to include them.
#
# We do this by running GCC in dependency mode on the header itself, and then
# looking at the files that are included by the header, and capturing the
# output.

ifneq ($(PREMAKE),1)

header_deps_onefile=$(shell $(CXX) $(CXXFLAGS) -MM -o - $(1) | tr ' ' '\n' | tr -d '\\' | sort | uniq | grep -v sdk_include_seed | grep -v ':' | grep -v '^/' | sed -e 's!^mldb/mldb/!!' -e 's!^mldb/!!')

$(INC)/mldb/%.h:			./mldb/%.h
	mkdir -p $(dir $@) && cp ./mldb/$*.h $@

MLDB_ALL_HEADERS=$(call header_deps_onefile,mldb/sdk/sdk_include_seed.cc)

mldb_dev_headers: $(MLDB_ALL_HEADERS:%=$(INC)/mldb/%)

expand_wildcards=$(shell find $(1) -type f)

mldb_sdk: $(MLDB_ALL_HEADERS:%=$(INC)/mldb/%)


docker_mldb_sdk: docker_mldb


docker_mldb_sdk: \
	DOCKER_BASE_IMAGE=quay.io/mldb/mldb:$(WHOAMI)-$(CURRENT_BRANCH) \
	DOCKER_POST_INSTALL_SCRIPT=sdk/install_sdk.sh \
	DOCKER_COMMIT_ARGS=-run='{"Cmd": [ "/sbin/my_init" ], "PortSpecs": ["80"], "Volumes": { "$(MLDB_DATA_DIR)": {} } }'

endif
