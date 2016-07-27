# Makefile for postgresql plugin for MLDB

# External repos required
# $(eval $(call include_sub_make,mongodb_ext,ext,mongodb_ext.mk))

# postgresql plugin
LIBMLDB_POSTGRESQL_PLUGIN_SOURCES:= \
   postgresql_plugin.cc \

# $(eval $(call set_compile_option,$(LIBMLDB_POSTGRESQL_PLUGIN_SOURCES),))

# $(eval $(call mldb_plugin_library,postgresql,mldb_postgresql_plugin,$(LIBMLDB_POSTGRESQL_PLUGIN_SOURCES),))

# $(eval $(call mldb_builtin_plugin,postgresql,mldb_postgresql_plugin,doc))

$(eval $(call library,postgresql,$(LIBMLDB_POSTGRESQL_PLUGIN_SOURCES),types utils value_description any))

$(eval $(call mldb_unit_test,MLDB-1850-postgresql.py,postgresql,manual))