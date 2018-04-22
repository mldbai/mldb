# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Behavioral dataset plugin
LIBMLDB_TABULAR_PLUGIN_SOURCES:= \
	tabular_dataset.cc \
	frozen_column.cc \
	frozen_tables.cc \
	string_frozen_column.cc \
	column_types.cc \
	tabular_dataset_column.cc \
	tabular_dataset_chunk.cc \
	transducer.cc

LIBMLDB_TABULAR_PLUGIN_LINK := \
	mldb block zstd


$(eval $(call library,mldb_tabular_plugin,$(LIBMLDB_TABULAR_PLUGIN_SOURCES),$(LIBMLDB_TABULAR_PLUGIN_LINK)))

$(eval $(call include_sub_make,tabular_testing,testing,testing.mk))
