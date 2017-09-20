# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call include_sub_make,behavior))

# Behavioral dataset plugin
LIBMLDB_BUILTIN_PLUGIN_SOURCES:= \
	matrix.cc \
	for_each_line.cc \
	dataset_feature_space.cc \
	accuracy.cc \
	metric_space.cc \
	sqlite_dataset.cc \
	sparse_matrix_dataset.cc \
	script_procedure.cc \
	permuter_procedure.cc \
	external_python_procedure.cc \
	experiment_procedure.cc \
	docker_plugin.cc \
	continuous_dataset.cc \
	word2vec.cc \
	nlp.cc \
	sentiwordnet.cc \
	bucketize_procedure.cc \
	git.cc \
	csv_export_procedure.cc \
	xlsx_importer.cc \
	json_importer.cc \
	melt_procedure.cc \
	ranking_procedure.cc \
	fetcher.cc \
	importtext_procedure.cc \
	tabular_dataset.cc \
	frozen_column.cc \
	column_types.cc \
	tabular_dataset_column.cc \
	tabular_dataset_chunk.cc \
	randomforest_procedure.cc \
	classifier.cc \
	sql_functions.cc \
	embedding.cc \
	svd.cc \
	kmeans.cc \
	probabilizer.cc \
	pooling_function.cc \
	feature_generators.cc \
	em.cc \
	tsne.cc \
	svm.cc \
	stats_table_procedure.cc \
	dist_table_procedure.cc \
	tfidf.cc \
	tokensplit.cc \
	script_function.cc \
	useragent_function.cc \
	summary_statistics_proc.cc \
	csv_writer.cc \
	mock_procedure.cc \
	datasetsplit_procedure.cc \
	behavior_dataset.cc \
	binary_behavior_dataset.cc \
	llvm.cc

# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,python_plugin_loader.cc,-I$(PYTHON_INCLUDE_PATH)))
$(eval $(call set_compile_option,importtext_procedure.cc,-I$(RE2_INCLUDE_PATH)))

$(eval $(call library,mldb_builtin_plugins,$(LIBMLDB_BUILTIN_PLUGIN_SOURCES),datacratic_sqlite ml mldb_lang_plugins mldb_algo_plugins mldb_misc_plugins mldb_ui_plugins tsne svm libstemmer edlib algebra svdlibc uap re2 behavior))
$(eval $(call library_forward_dependency,mldb_builtin_plugins,mldb_lang_plugins mldb_algo_plugins mldb_misc_plugins mldb_ui_plugins))

$(eval $(call include_sub_make,lang))
$(eval $(call include_sub_make,algo))
$(eval $(call include_sub_make,misc))
$(eval $(call include_sub_make,ui))


$(LIB)/libmldb_builtin_plugins.so: $(LIB)/libmldb_lang_plugins.so $(LIB)/libmldb_js_plugin.so $(LIB)/libmldb_misc_plugins.so $(LIB)/libmldb_ui_plugins.so


