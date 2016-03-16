# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
	function_contexts.cc \
	sql_expression_function_operations.cc \
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
	serial_function.cc \
    column_scope.cc \
    bucket.cc

LIBMLDB_LINK:= \
	service_peer mldb_builtin_plugins sql_expression git2 hoedown credentials_daemon mldb_builtin command_expression cloud mldb_core


$(eval $(call library,mldb,$(LIBMLDB_SOURCES),$(LIBMLDB_LINK)))
$(eval $(call library_forward_dependency,mldb,mldb_builtin))
$(eval $(call library_forward_dependency,mldb,mldb_builtin_plugins))

$(eval $(call program,mldb_doc_server,mldb boost_program_options))
$(eval $(call program,mldb_runner,mldb boost_program_options config))
