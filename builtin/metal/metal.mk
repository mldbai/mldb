# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Metal loader

ifneq ($(PREMAKE),1)

METAL_PLUGIN_SOURCE := \
	metal.cc compute_kernel_metal.cc

METAL_PLUGIN_LINK := \
	value_description \
	v8 \
	v8_libplatform \
	arch \
	utils \
	types \
	mldb_engine \
	log \
	any \
	rest \
	rest_entity \
	json_diff \
	logging \
	vfs \
	base \
	mldb_core \
	mldb_builtin_base \
	sql_types \
	http \
	sql_expression \
	Metal \
	block \
	command_expression \
	mtlpp \


$(eval $(call set_compile_option,$(METAL_PLUGIN_SOURCE),-Imldb/ext))
$(eval $(call library,mldb_metal_plugin,$(METAL_PLUGIN_SOURCE),value_description sql_expression $(METAL_PLUGIN_LINK)))
endif
