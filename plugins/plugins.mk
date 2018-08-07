# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call include_sub_make,behavior))
$(eval $(call include_sub_make,tabular))
$(eval $(call include_sub_make,jml))
$(eval $(call include_sub_make,nlp))
$(eval $(call include_sub_make,cluster))
$(eval $(call include_sub_make,html))

# Behavioral dataset plugin
LIBMLDB_BUILTIN_PLUGIN_SOURCES:= \
	sqlite_dataset.cc \
	sparse_matrix_dataset.cc \
	continuous_dataset.cc \
	\
	melt_procedure.cc \
	ranking_procedure.cc \
	pooling_function.cc \
	permuter_procedure.cc \
	datasetsplit_procedure.cc \
	summary_statistics_proc.cc \
	\
	csv_export_procedure.cc \
	csv_writer.cc \
	xlsx_importer.cc \
	json_importer.cc \
	importtext_procedure.cc \
	sql_csv_scope.cc \
	tokensplit.cc \
	\
	embedding.cc \
	svd.cc \
	\
	stats_table_procedure.cc \
	dist_table_procedure.cc \
	feature_generators.cc \
	bucketize_procedure.cc \
	\
	git.cc \


# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,python_plugin_loader.cc,-I$(PYTHON_INCLUDE_PATH)))

# Shared_mutex only in C++17
$(eval $(call set_compile_option,dist_table_procedure.cc,-std=c++1z))

LIBMLDB_BUILTIN_PLUGIN_LINK:= \
	mldb_tabular_plugin \
	mldb_behavior_plugin \
	mldb_jml_plugin \
	mldb_nlp_plugin \
	mldb_cluster_plugin \
	sqlite-mldb \
	ml \
	tsne \
	svm \
	libstemmer \
	edlib \
	algebra \
	svdlibc \
	uap \

$(eval $(call library,mldb_builtin_plugins,$(LIBMLDB_BUILTIN_PLUGIN_SOURCES),$(LIBMLDB_BUILTIN_PLUGIN_LINK)))


