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
	transducer.cc \
	suffix_array.cc \
	bit_compressed_int_table.cc \
	mmap.cc \
	mapped_int_table.cc \
    run_length_int_table.cc \
	factored_int_table.cc \
	string_table.cc \
	cluster.cc \
	raw_mapped_int_table.cc \
	mapped_selector_table.cc \
    int_table.cc \
	predictor.cc \
	fit_accumulator.cc \

LIBMLDB_TABULAR_PLUGIN_LINK := \
	block \
	zstd \
	sql_expression \
	mldb_engine \
	mldb_core \
	value_description \
	arch \
	types \
	progress \
	base \
	vfs \
	log \
	rest \
	algebra


$(eval $(call library,mldb_tabular_plugin,$(LIBMLDB_TABULAR_PLUGIN_SOURCES),$(LIBMLDB_TABULAR_PLUGIN_LINK)))

$(eval $(call include_sub_make,tabular_testing,testing,testing.mk))
