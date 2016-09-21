# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# mldb.mk

$(eval $(call python_program,validator_api.wsgi,validator_api.wsgi,))

LIBMLDB_SOURCES:= \
	mldb_server.cc \
	plugin_collection.cc \
	plugin_manifest.cc \
	dataset_utils.cc \
	dataset_collection.cc \
	procedure_collection.cc \
	procedure_run_collection.cc \
	function_collection.cc \
	credential_collection.cc \
	type_collection.cc \
	analytics.cc \
	plugin_resource.cc \
	dataset_context.cc \
	static_content_handler.cc \
	static_content_macro.cc \
	external_plugin.cc \
	bound_queries.cc \
	script_output.cc \
	forwarded_dataset.cc \
	column_scope.cc \
	bucket.cc \
	sensor_collection.cc \

LIBMLDB_LINK:= \
	service_peer mldb_builtin_plugins sql_expression runner credentials git2 hoedown mldb_builtin command_expression vfs_handlers mldb_core


$(eval $(call library,mldb,$(LIBMLDB_SOURCES),$(LIBMLDB_LINK)))
$(eval $(call library_forward_dependency,mldb,mldb_builtin))
$(eval $(call library_forward_dependency,mldb,mldb_builtin_plugins))

$(eval $(call program,mldb_runner,mldb boost_program_options config))
