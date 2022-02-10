# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBMLDB_BUILTIN_BASE_SOURCES:= \
	merged_dataset.cc \
	transposed_dataset.cc \
	joined_dataset.cc \
	sub_dataset.cc \
	filtered_dataset.cc \
	sampled_dataset.cc \
	union_dataset.cc \
	\
	basic_procedures.cc \
	sql_functions.cc \
	fetcher.cc \
	\
	shared_library_plugin.cc \
	script_output.cc \
	plugin_resource.cc \
	script_procedure.cc \
	docker_plugin.cc \
	external_python_procedure.cc \
	script_function.cc \
	\
	mock_procedure.cc \
	\
	continuous_dataset.cc \
	\
	melt_procedure.cc \
	ranking_procedure.cc \
	pooling_function.cc \
	permuter_procedure.cc \
	datasetsplit_procedure.cc \
	summary_statistics_proc.cc \
	\
	metric_space.cc \
	matrix.cc \
	intersection_utils.cc \

LIBMLDB_BUILTIN_BASE_LINK:= \
	mldb_core \
	mldb_engine \
	runner \
	git2 \
	ssh2 \
	arch \
	value_description \
	types \
	base \
	utils \
	sql_expression \
	vfs \
	rest \
	link \
	rest_entity \
	any \
	json_diff \
	progress \
	log \
	watch \
	gc \
	mldb_core \
	utils \
	sql_types \

# Shared_mutex only in C++17
$(eval $(call set_compile_option,dist_table_procedure.cc,-std=c++1z))

$(eval $(call library,mldb_builtin_base,$(LIBMLDB_BUILTIN_BASE_SOURCES),$(LIBMLDB_BUILTIN_BASE_LINK)))

# Builtin programming language support for MLDB

$(eval $(call include_sub_make,mldb_js_plugin,js))
$(eval $(call include_sub_make,mldb_python_plugin,python))
$(eval $(call include_sub_make,opencl))

ifeq ($(OSNAME),Darwin)
$(eval $(call include_mldb_plugin,metal))
MLDB_METAL_PLUGIN_NAME:=mldb_metal_plugin
endif

ifeq ($(CUDA_ENABLED),1)
$(eval $(call include_mldb_plugin,cuda))
MLDB_CUDA_PLUGIN_NAME:=mldb_cuda_plugin
endif

LIBMLDB_BUILTIN_LINK := \
	mldb_builtin_base \
	mldb_js_plugin \
	mldb_python_plugin \
	mldb_opencl_plugin \
	$(MLDB_METAL_PLUGIN_NAME) \
	$(MLDB_CUDA_PLUGIN_NAME) \


$(eval $(call library,mldb_builtin,,$(LIBMLDB_BUILTIN_LINK)))
