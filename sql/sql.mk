# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

SQL_EXPRESSION_SOURCES := \
	cell_value.cc \
	sql_expression.cc \
	expression_value.cc \
	table_expression_operations.cc \
	binding_contexts.cc \
	builtin_functions.cc \
	builtin_geo_functions.cc \
	builtin_dataset_functions.cc \
	builtin_aggregators.cc \
	interval.cc \
	join_utils.cc \
	tokenize.cc \
	execution_pipeline.cc \
	execution_pipeline_impl.cc \
	sql_utils.cc \
	path.cc \
	dataset_types.cc \
	sql_expression_operations.cc \

# Unfortunately the S2 library needs you to mess with the include path as its includes
# aren't prefixed.
$(eval $(call set_compile_option,cell_value.cc builtin_geo_functions.cc,$(S2_COMPILE_OPTIONS)))

# NOTE: the SQL library should NOT depend on MLDB.  See the comment in testing/testing.mk
$(eval $(call library,sql_expression,$(SQL_EXPRESSION_SOURCES),types utils value_description any ml services_base json_diff siphash hash s2 edlib))

$(eval $(call include_sub_make,sql_testing,testing,sql_testing.mk))


