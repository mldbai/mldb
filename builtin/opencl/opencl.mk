# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Opencl loader

ifneq ($(PREMAKE),1)

OPENCL_PLUGIN_SOURCE := \
	opencl.cc opencl_types.cc

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



$(eval $(call set_compile_option,$(OPENCL_PLUGIN_SOURCE),-Wno-ignored-attributes))

$(eval $(call library,mldb_opencl_plugin,$(OPENCL_PLUGIN_SOURCE),value_description sql_expression OpenCL))

endif
