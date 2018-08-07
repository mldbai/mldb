# Makefile for feature_gen plugin for MLDB

#$(eval $(call include_sub_make,feature_gen_ext,ext,feature_gen_ext.mk))


# Feature_Gen plugins
LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES:= \
	feature_gen_plugin.cc \
	stats_table_procedure.cc \
	dist_table_procedure.cc \
	feature_generators.cc \
	bucketize_procedure.cc \


LIBMLDB_FEATURE_GEN_PLUGIN_LINK:= \

$(eval $(call library,mldb_feature_gen_plugin,$(LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES),$(LIBMLDB_FEATURE_GEN_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES),-Imldb/feature_gen/ext))

#$(eval $(call mldb_plugin_library,feature_gen,mldb_feature_gen_plugin,$(LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,feature_gen,mldb_feature_gen_plugin,doc))

#$(eval $(call include_sub_make,feature_gen_testing,testing,feature_gen_testing.mk))
