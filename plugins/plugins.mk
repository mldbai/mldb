# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Behavioural dataset plugin
LIBMLDB_BUILTIN_PLUGIN_SOURCES:= \
	svd.cc \
	matrix.cc \
	sql_functions.cc \
	for_each_line.cc \
	classifier.cc \
	probabilizer.cc \
	dataset_feature_space.cc \
	accuracy.cc \
	tsne.cc \
	metric_space.cc \
	kmeans.cc \
	embedding.cc \
	sqlite_dataset.cc \
	sparse_matrix_dataset.cc \
	script_procedure.cc \
	script_function.cc \
	permuter_procedure.cc \
	csv_dataset.cc \
	svm.cc \
	feature_generators.cc \
	external_python_procedure.cc \
	stats_table_procedure.cc \
	experiment_procedure.cc \
	docker_plugin.cc \
	continuous_dataset.cc \
	word2vec.cc \
	nlp.cc \
	sentiwordnet.cc \
	bucketize_procedure.cc \
	git.cc \
	tfidf.cc \
	tokensplit.cc \
	csv_export_procedure.cc \
	em.cc \
	pooling_function.cc \
	xlsx_importer.cc \
	json_importer.cc \
	twitter_importer.cc \
	melt_procedure.cc \
	ranking_procedure.cc \
	fetcher.cc \


# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,python_plugin_loader.cc,-I$(PYTHON_INCLUDE_PATH)))

$(eval $(call library,mldb_builtin_plugins,$(LIBMLDB_BUILTIN_PLUGIN_SOURCES),datacratic_sqlite ml mldb_lang_plugins mldb_algo_plugins mldb_misc_plugins mldb_ui_plugins tsne svm libstemmer algebra svdlibc csv_writer libtwitcurl))
$(eval $(call library_forward_dependency,mldb_builtin_plugins,mldb_lang_plugins mldb_algo_plugins mldb_misc_plugins mldb_ui_plugins))

$(eval $(call include_sub_make,lang))
$(eval $(call include_sub_make,algo))
$(eval $(call include_sub_make,misc))
$(eval $(call include_sub_make,ui))


$(LIB)/libmldb_builtin_plugins.so: $(LIB)/libmldb_lang_plugins.so $(LIB)/libmldb_js_plugin.so $(LIB)/libmldb_misc_plugins.so $(LIB)/libmldb_ui_plugins.so


