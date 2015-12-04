# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# We extract header files for the development of external plugins, until such
# time as we have a fully separate API.  We start with a set of headers we
# want to include, and we add to those a list of dependency headers that are
# required to include them.
#
# We do this by running GCC in dependency mode on the header itself, and then
# looking at the files that are included by the header, and capturing the
# output.

ifneq ($(PREMAKE),1)

header_deps_onefile=$(shell $(CXX) $(CXXFLAGS) -MM -o - $(1) | tr ' ' '\n' | tr -d '\\' | sort | uniq | grep -v ':' | grep -v '^/' | sed 's!^mldb/!!')

$(INC)/mldb/%.h:			./%.h
	mkdir -p $(dir $@) && cp $*.h $@


MLDB_HEADER_SEED:=mldb/server/dataset.h mldb/server/plugin.h mldb/server/function.h mldb/server/procedure.h mldb/types/basic_value_descriptions.h

MLDB_ALL_HEADERS:=$(sort $(foreach header,$(MLDB_HEADER_SEED),$(header) $(call header_deps_onefile,$(header))))

mldb_dev_headers: $(MLDB_ALL_HEADERS:%=$(INC)/mldb/%)

#MLDB_LOCAL_SEED := $(HOME)/local/include/boost

expand_wildcards=$(shell find $(1) -type f)

#MLDB_LOCAL_HEADERS := $(foreach dir,$(MLDB_LOCAL_SEED),$(call expand_wildcards,$(dir)))

#MLDB_LOCAL_HEADER_FILES := $(MLDB_LOCAL_HEADERS:$(HOME)/local/include/%=%)

#$(warning mldb_local_headers=$(MLDB_LOCAL_HEADER_FILES))

$(INC)/%: $(HOME)/local/include/%
	mkdir -p $(dir $@) && cp $< $@


mldb_sdk: \
	$(MLDB_ALL_HEADERS:%=$(INC)/mldb/%) \
	$(MLDB_LOCAL_HEADER_FILES:%=$(INC)/%)


docker_mldb_sdk: docker_mldb


docker_mldb_sdk: \
	DOCKER_BASE_IMAGE=quay.io/datacratic/mldb:$(WHOAMI)-$(CURRENT_BRANCH) \
	DOCKER_POST_INSTALL_SCRIPT=sdk/install_sdk.sh \
	DOCKER_COMMIT_ARGS=-run='{"Cmd": [ "/sbin/my_init" ], "PortSpecs": ["80"], "Volumes": { "$(MLDB_DATA_DIR)": {} } }'

endif
