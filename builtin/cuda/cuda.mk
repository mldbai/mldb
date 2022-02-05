# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Cuda loader

ifneq ($(PREMAKE),1)

# Cuda library for random forests
$(eval $(call library,base_kernels_cuda,base_kernels.cu))

CUDA_PLUGIN_SOURCE := \
	cuda.cc compute_kernel_cuda.cc

CUDA_PLUGIN_LINK := \
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
	block \
	command_expression \
	base_kernels_cuda \
	cuda

$(eval $(call set_compile_option,$(CUDA_PLUGIN_SOURCE),-I$(CUDA_INCLUDE_PATH) -Imldb/ext/cuda-api-wrappers/src))
$(eval $(call library,mldb_cuda_plugin,$(CUDA_PLUGIN_SOURCE),value_description sql_expression $(CUDA_PLUGIN_LINK)))

$(eval $(call include_sub_make,cuda_testing,testing))

endif
