# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# mldb.mk

$(eval $(call python_program,validator_api.wsgi,validator_api.wsgi,))

LIBMLDB_SOURCES:= \
	mldb_server.cc \
	plugin_manifest.cc \
	static_content_handler.cc \
	static_content_macro.cc \

LIBMLDB_LINK:= \
	service_peer \
	mldb_builtin_plugins \
	sql_expression \
	runner \
	credentials \
	git2 \
	hoedown \
	mldb_builtin \
	command_expression \
	vfs_handlers \
	mldb_core \
	mldb_engine \
	rest \


$(eval $(call library,mldb,$(LIBMLDB_SOURCES),$(LIBMLDB_LINK)))
$(eval $(call library_forward_dependency,mldb,mldb_builtin))
$(eval $(call library_forward_dependency,mldb,mldb_builtin_plugins))

$(eval $(call program,mldb_runner,mldb boost_program_options config))

LIBMLDB_PLATFORM_PYTHON_SOURCES:= \
	mldb_platform_python.cc

LIBMLDB_PLATFORM_PYTHON_LINK:= \
	mldb

$(eval $(call library,mldb_platform_python,$(LIBMLDB_PLATFORM_PYTHON_SOURCES),$(LIBMLDB_PLATFORM_PYTHON_LINK)))

$(eval $(call set_compile_option,$(LIBMLDB_PLATFORM_PYTHON_SOURCES),-I$(PYTHON_INCLUDE_PATH)))
