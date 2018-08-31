# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call include_sub_make,behavior))
$(eval $(call include_sub_make,tabular))
$(eval $(call include_sub_make,jml))
$(eval $(call include_sub_make,nlp))
$(eval $(call include_sub_make,feature_gen))
$(eval $(call include_sub_make,textual))
$(eval $(call include_sub_make,msoffice))
$(eval $(call include_sub_make,git))
$(eval $(call include_sub_make,sqlite))
$(eval $(call include_sub_make,sparse))
$(eval $(call include_sub_make,embedding))

$(eval $(call include_sub_make,html))
$(eval $(call include_sub_make,pro))
$(eval $(call include_sub_make,av))

# No source code, this is just a library to bring everything together
LIBMLDB_BUILTIN_PLUGIN_SOURCES:=

# Needed so that Python plugin can find its header
$(eval $(call set_compile_option,python_plugin_loader.cc,-I$(PYTHON_INCLUDE_PATH)))

LIBMLDB_BUILTIN_PLUGIN_LINK:= \
	mldb_tabular_plugin \
	mldb_behavior_plugin \
	mldb_jml_plugin \
	mldb_nlp_plugin \
	mldb_feature_gen_plugin \
	mldb_textual_plugin \
	mldb_msoffice_plugin \
	mldb_git_plugin \
	mldb_sqlite_plugin \
	mldb_sparse_plugin \
	mldb_embedding_plugin \
	sqlite-mldb \
	ml \
	tsne \
	svm \
	libstemmer \
	edlib \
	algebra \
	svdlibc \

$(eval $(call library,mldb_builtin_plugins,$(LIBMLDB_BUILTIN_PLUGIN_SOURCES),$(LIBMLDB_BUILTIN_PLUGIN_LINK)))


