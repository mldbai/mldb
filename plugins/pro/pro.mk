# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Makefile for "pro" plugins for MLDB

# Pro plugins
LIBMLDB_PRO_PLUGIN_SOURCES:= \
	pro_plugin.cc \
	list_files_procedure.cc \
	dataset_stats_procedure.cc \

$(eval $(call set_compile_option,$(LIBMLDB_PRO_PLUGIN_SOURCES),-Ipro))

$(eval $(call mldb_plugin_library,pro,mldb_pro_plugin,$(LIBMLDB_PRO_PLUGIN_SOURCES),ml))

$(eval $(call mldb_builtin_plugin,pro,mldb_pro_plugin,doc))

$(eval $(call include_sub_make,pro_testing,testing,pro_testing.mk))

