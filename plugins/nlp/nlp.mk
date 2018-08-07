# Makefile for nlp plugin for MLDB

#$(eval $(call include_sub_make,nlp_ext,ext,nlp_ext.mk))


# Nlp plugins
LIBMLDB_NLP_PLUGIN_SOURCES:= \
	nlp_plugin.cc \
	tfidf.cc \
	word2vec.cc \
	nlp.cc \
	sentiwordnet.cc \


LIBMLDB_NLP_PLUGIN_LINK:= \

$(eval $(call library,mldb_nlp_plugin,$(LIBMLDB_NLP_PLUGIN_SOURCES),$(LIBMLDB_NLP_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_NLP_PLUGIN_SOURCES),-Imldb/nlp/ext))

#$(eval $(call mldb_plugin_library,nlp,mldb_nlp_plugin,$(LIBMLDB_NLP_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,nlp,mldb_nlp_plugin,doc))

#$(eval $(call include_sub_make,nlp_testing,testing,nlp_testing.mk))
