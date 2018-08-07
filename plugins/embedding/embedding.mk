# Makefile for embedding plugin for MLDB

#$(eval $(call include_sub_make,embedding_ext,ext,embedding_ext.mk))


# Embedding plugins
LIBMLDB_EMBEDDING_PLUGIN_SOURCES:= \
	embedding_plugin.cc \
	embedding.cc \
	svd.cc \


LIBMLDB_EMBEDDING_PLUGIN_LINK:= \

$(eval $(call library,mldb_embedding_plugin,$(LIBMLDB_EMBEDDING_PLUGIN_SOURCES),$(LIBMLDB_EMBEDDING_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_EMBEDDING_PLUGIN_SOURCES),-Imldb/embedding/ext))

#$(eval $(call mldb_plugin_library,embedding,mldb_embedding_plugin,$(LIBMLDB_EMBEDDING_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,embedding,mldb_embedding_plugin,doc))

#$(eval $(call include_sub_make,embedding_testing,testing,embedding_testing.mk))
