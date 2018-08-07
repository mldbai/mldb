# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# mldb.mk

$(eval $(call python_program,validator_api.wsgi,validator_api.wsgi,))

LIBMLDB_SOURCES:= \
	mldb_server.cc \
	plugin_manifest.cc \
	static_content_handler.cc \
	static_content_macro.cc \

LIBMLDB_LINK:= \
	service_peer mldb_builtin_plugins sql_expression runner credentials git2 hoedown mldb_builtin command_expression vfs_handlers mldb_core mldb_engine


$(eval $(call library,mldb,$(LIBMLDB_SOURCES),$(LIBMLDB_LINK)))
$(eval $(call library_forward_dependency,mldb,mldb_builtin))
$(eval $(call library_forward_dependency,mldb,mldb_builtin_plugins))

$(eval $(call program,mldb_runner,mldb boost_program_options config))
