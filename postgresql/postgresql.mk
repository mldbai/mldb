# Makefile for postgresql plugin for MLDB

# postgresql plugin
LIBMLDB_POSTGRESQL_PLUGIN_SOURCES:= \
   postgresql_plugin.cc \

$(eval $(call mldb_plugin_library,postgresql,mldb_postgresql_plugin,$(LIBMLDB_POSTGRESQL_PLUGIN_SOURCES),pq))

$(eval $(call mldb_builtin_plugin,postgresql,mldb_postgresql_plugin,doc))

$(eval $(call mldb_unit_test,MLDB-1850-postgresql.py,postgresql,manual))