# Makefile for tensorflow plugin for MLDB

# Tensorflow plugins
LIBMLDB_TENSORFLOW_PLUGIN_SOURCES:= \
	tensorflow_plugin.cc

$(eval $(call set_compile_option,$(LIBMLDB_TENSORFLOW_PLUGIN_SOURCES),-Imldb/ext))

$(eval $(call mldb_plugin_library,tensorflow,mldb_tensorflow_plugin,$(LIBMLDB_TENSORFLOW_PLUGIN_SOURCES),tensorflow))

$(eval $(call mldb_builtin_plugin,tensorflow,mldb_tensorflow_plugin,doc))

$(eval $(call mldb_unit_test,MLDB-1203-tensorflow-plugin.js,tensorflow))

#$(eval $(call include_sub_make,pro_testing,testing,pro_testing.mk))
