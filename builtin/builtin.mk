# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBMLDB_BUILTIN_SOURCES:= \
	merged_dataset.cc \
	transposed_dataset.cc \
	joined_dataset.cc \
	sub_dataset.cc \
	filtered_dataset.cc \
	sampled_dataset.cc \
	union_dataset.cc \

LIBMLDB_BUILTIN_LINK:= mldb_core runner


$(eval $(call library,mldb_builtin,$(LIBMLDB_BUILTIN_SOURCES),$(LIBMLDB_BUILTIN_LINK)))

