LIBMLDB_ENGINE_SOURCES:= \
	dataset_utils.cc \
	analytics.cc \
	dataset_scope.cc \
	bound_queries.cc \
	forwarded_dataset.cc \
	column_scope.cc \
	bucket.cc \
	dataset_collection.cc \
	procedure_collection.cc \
	procedure_run_collection.cc \
	function_collection.cc \
	credential_collection.cc \
	type_collection.cc \
	plugin_collection.cc \
	external_plugin.cc \

LIBMLDB_ENGINE_LINK:= \
	sql_expression credentials mldb_core command_expression


$(eval $(call library,mldb_engine,$(LIBMLDB_ENGINE_SOURCES),$(LIBMLDB_ENGINE_LINK)))
