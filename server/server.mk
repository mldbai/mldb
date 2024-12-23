# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# mldb.mk

$(eval $(call python_program,validator_api.wsgi,validator_api.wsgi,))

LIBMLDB_SOURCES:= \
	mldb_server.cc \
	plugin_manifest.cc \

LIBMLDB_LINK:= \
	service_peer \
	mldb_builtin_plugins \
	sql_expression \
	runner \
	credentials \
	git2 \
	mldb_builtin \
	command_expression \
	vfs_handlers \
	mldb_core \
	mldb_engine \
	rest \
	mldb_core \
	mldb_engine \
	arch \
	base \
	types \
	utils \
	sql_expression \
	value_description \
	base \
	progress \
	rest \
	db \
	vfs \
	log \
	link \
	rest \
	any \
	watch \
	rest_entity \
	mldb_builtin_base \
	mldb_builtin \
	sql_types \
	http \


$(eval $(call library,mldb,$(LIBMLDB_SOURCES),$(LIBMLDB_LINK)))
$(eval $(call library_forward_dependency,mldb,mldb_builtin))
$(eval $(call library_forward_dependency,mldb,mldb_builtin_plugins))

$(eval $(call program,mldb_runner,mldb boost_program_options config $(LIBMLDB_LINK) io_base))

LIBMLDB_PLATFORM_PYTHON_SOURCES:= \
	mldb_platform_python.cc

LIBMLDB_PLATFORM_PYTHON_LINK:= \
	mldb \
	$(PYTHON_LIBRARY) \
	 types \
	 arch \
	 base \
	 value_description \
	 mldb_python_plugin \
	 io_base \
	 python_interpreter \
	 rest \

$(eval $(call library,mldb_platform_python,$(LIBMLDB_PLATFORM_PYTHON_SOURCES),$(LIBMLDB_PLATFORM_PYTHON_LINK)))

$(eval $(call set_compile_option,$(LIBMLDB_PLATFORM_PYTHON_SOURCES),-I$(PYTHON_INCLUDE_PATH)))
