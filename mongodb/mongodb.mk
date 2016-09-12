# Makefile for tensorflow plugin for MLDB

# External repos required
$(eval $(call include_sub_make,mongodb_ext,ext,mongodb_ext.mk))

# Mongodb plugin
LIBMLDB_MONGODB_PLUGIN_SOURCES:= \
	mongo_common.cc \
	mongo_record.cc \
	mongo_import.cc \
	mongo_query.cc \
	mongo_dataset.cc \

$(eval $(call set_compile_option,$(LIBMLDB_MONGODB_PLUGIN_SOURCES),$(MONGOCXX_INCLUDE_FLAGS)))

$(eval $(call mldb_plugin_library,mongodb,mldb_mongodb_plugin,$(LIBMLDB_MONGODB_PLUGIN_SOURCES),mongocxx))

$(eval $(call mldb_builtin_plugin,mongodb,mldb_mongodb_plugin,doc))

$(eval $(call library,mongo_tmp_server,mongo_temporary_server.cc, services runner))

$(eval $(call python_addon,python_mongo_temp_server_wrapping,mongo_temp_server_wrapping.cc,mongo_tmp_server boost_filesystem))
$(eval $(call python_module,mongo_temp_server_wrapping,$(notdir $(wildcard $(CWD)/*.py)),python_mongo_temp_server_wrapping))

$(eval $(call include_sub_make,testing))
