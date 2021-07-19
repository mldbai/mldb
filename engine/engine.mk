LIBMLDB_ENGINE_SOURCES:= \
	dataset_utils.cc \
	forwarded_dataset.cc \
	column_scope.cc \
	dataset_collection.cc \
	procedure_collection.cc \
	procedure_run_collection.cc \
	function_collection.cc \
	credential_collection.cc \
	type_collection.cc \
	plugin_collection.cc \
	external_plugin.cc \
	sensor_collection.cc \
	static_content_macro.cc \
	static_content_handler.cc \

LIBMLDB_ENGINE_LINK:= \
	sql_expression \
	credentials \
	mldb_core \
	command_expression \
	hoedown \



$(eval $(call library,mldb_engine,$(LIBMLDB_ENGINE_SOURCES),$(LIBMLDB_ENGINE_LINK)))
