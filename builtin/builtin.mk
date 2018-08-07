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
	for_each_line.cc \
	metric_space.cc \
	mock_procedure.cc \
	matrix.cc \
	\

LIBMLDB_BUILTIN_BASE_LINK:= \
	mldb_core \
	runner \
	git2 \
	ssh2 \

$(eval $(call library,mldb_builtin_base,$(LIBMLDB_BUILTIN_BASE_SOURCES),$(LIBMLDB_BUILTIN_BASE_LINK)))

# Builtin programming language support for MLDB

$(eval $(call include_sub_make,mldb_js_plugin,js))
$(eval $(call include_sub_make,mldb_python_plugin,python))

LIBMLDB_BUILTIN_LINK := \
	mldb_builtin_base \
	mldb_js_plugin \
	mldb_python_plugin \


$(eval $(call library,mldb_builtin,,$(LIBMLDB_BUILTIN_LINK)))
