# Makefile for jml plugin for MLDB

#$(eval $(call include_sub_make,jml_ext,ext,jml_ext.mk))

LIBJML_UTILS_SOURCES := \
	configuration.cc

LIBJML_UTILS_LINK := \
	utils arch base

$(eval $(call library,jml_utils,$(LIBJML_UTILS_SOURCES),$(LIBJML_UTILS_LINK)))

$(eval $(call include_sub_makes,algebra stats tsne jml))

LIBML_SOURCES := \
	dense_classifier.cc \
	separation_stats.cc \
	dense_classifier_scorer.cc \
	dense_feature_generator.cc \
	data_partition.cc \
	scorer.cc \
	prediction_accumulator.cc \
	bucketing_probabilizer.cc \
	distribution_pooler.cc \
	kmeans.cc \
	em.cc \
	value_descriptions.cc \
	configuration.cc

LIBML_LINK := \
	$(STD_FILESYSTEM_LIBNAME) \
	types \
	value_description \
	algebra \
	boosting \
	arch \
	base \
	utils \
	db \
	vfs \

$(eval $(call library,ml,$(LIBML_SOURCES),$(LIBML_LINK)))

$(eval $(call include_sub_make,ml_testing,testing,ml_testing.mk))


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
	kmeans_interface.cc \
	em_interface.cc \
	tsne_interface.cc \


LIBMLDB_JML_PLUGIN_LINK:= \
	ml \
	boosting \
	mldb_core \
	mldb_engine \
	arch \
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
	tsne \
	link \
	svm \
	rest \
	any \
	watch \
	rest_entity \
	jml_utils \
	mldb_builtin_base \
	mldb_builtin \
	sql_types \
	algebra \


$(eval $(call library,mldb_jml_plugin,$(LIBMLDB_JML_PLUGIN_SOURCES),$(LIBMLDB_JML_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_JML_PLUGIN_SOURCES),-Imldb/jml/ext))

#$(eval $(call mldb_plugin_library,jml,mldb_jml_plugin,$(LIBMLDB_JML_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,jml,mldb_jml_plugin,doc))

#$(eval $(call include_sub_make,jml_testing,testing,jml_testing.mk))


