# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Opencl loader

ifneq ($(PREMAKE),1)

OPENCL_PLUGIN_SOURCE := \
	opencl.cc opencl_types.cc compute_kernel_opencl.cc

OPENCL_PLUGIN_LINK := \
	value_description \
	v8 \
	v8_libplatform \
	arch \
	utils \
	types \
	mldb_engine \
	log \
	any \
	mldb_core \
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
	OpenCL \
	block \
	command_expression \


ifeq ($(OSNAME),Linux)

LIB_OpenCL_DEPS:=$(LIB)/libOpenCL.so

OPENCL_ICD_LIB_DIR_x86_64:=/usr/lib/x86_64-linux-gnu/
# TODO: for non-x86_64

OPENCL_ICD_LIB_DIR:=$(OPENCL_ICD_LIB_DIR_$(ARCH))

$(if $(OPENCL_ICD_LIB_DIR),,$(error Need to set OPENCL_ICD_LIB_DIR_$(ARCH) for your platform))

$(LIB)/libOpenCL.so:	$(OPENCL_ICD_LIB_DIR)/libOpenCL.so | $(LIB)/libOpenCL.so.1 $(LIB)/libOpenCL.so.1.0.0
	@cp $< $@~
	@touch $@~
	@mv $@~ $@

$(LIB)/libOpenCL.so.%:	$(OPENCL_ICD_LIB_DIR)/libOpenCL.so.%
	@cp $< $@~
	@touch $@~
	@mv $@~ $@

endif # OSNAME=Linux

$(eval $(call set_compile_option,$(OPENCL_PLUGIN_SOURCE) opencl_replay_trace.cc,-Imldb/ext))
$(eval $(call library,mldb_opencl_plugin,$(OPENCL_PLUGIN_SOURCE),value_description sql_expression $(OPENCL_PLUGIN_LINK)))

$(eval $(call program,opencl_replay_trace,mldb_opencl_plugin $(OPENCL_PLUGIN_LINK)))
endif
