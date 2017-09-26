# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Opencl loader

ifneq ($(PREMAKE),1)

OPENCL_PLUGIN_SOURCE := \
	opencl.cc

$(eval $(call set_compile_option,$(OPENCL_PLUGIN_SOURCE),-I$(CUDA_SYSTEM_HEADER_DIR)))

$(eval $(call library,mldb_opencl_plugin,$(OPENCL_PLUGIN_SOURCE),value_description sql_expression OpenCL))

endif
