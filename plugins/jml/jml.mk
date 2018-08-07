# Makefile for jml plugin for MLDB

#$(eval $(call include_sub_make,jml_ext,ext,jml_ext.mk))


# Jml plugins
LIBMLDB_JML_PLUGIN_SOURCES:= \
	randomforest_procedure.cc \
	classifier.cc \
	probabilizer.cc \
	svm.cc \
	jml_plugin.cc \
	accuracy.cc \
	experiment_procedure.cc \
	randomforest.cc \
	dataset_feature_space.cc \


LIBMLDB_JML_PLUGIN_LINK:= \
	ml

$(eval $(call library,mldb_jml_plugin,$(LIBMLDB_JML_PLUGIN_SOURCES),$(LIBMLDB_JML_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_JML_PLUGIN_SOURCES),-Imldb/jml/ext))

#$(eval $(call mldb_plugin_library,jml,mldb_jml_plugin,$(LIBMLDB_JML_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,jml,mldb_jml_plugin,doc))

#$(eval $(call include_sub_make,jml_testing,testing,jml_testing.mk))


