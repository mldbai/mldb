# Makefile for sqlite plugin for MLDB

#$(eval $(call include_sub_make,sqlite_ext,ext,sqlite_ext.mk))


# Sqlite plugins
LIBMLDB_SQLITE_PLUGIN_SOURCES:= \
	sqlite_plugin.cc \
	sqlite_dataset.cc \


LIBMLDB_SQLITE_PLUGIN_LINK:= \

$(eval $(call library,mldb_sqlite_plugin,$(LIBMLDB_SQLITE_PLUGIN_SOURCES),$(LIBMLDB_SQLITE_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_SQLITE_PLUGIN_SOURCES),-Imldb/sqlite/ext))

#$(eval $(call mldb_plugin_library,sqlite,mldb_sqlite_plugin,$(LIBMLDB_SQLITE_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,sqlite,mldb_sqlite_plugin,doc))

#$(eval $(call include_sub_make,sqlite_testing,testing,sqlite_testing.mk))
