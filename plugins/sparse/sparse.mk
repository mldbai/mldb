# Makefile for sparse plugin for MLDB

#$(eval $(call include_sub_make,sparse_ext,ext,sparse_ext.mk))


# Sparse plugins
LIBMLDB_SPARSE_PLUGIN_SOURCES:= \
	sparse_plugin.cc \
	sparse_matrix_dataset.cc \


LIBMLDB_SPARSE_PLUGIN_LINK:= \

$(eval $(call library,mldb_sparse_plugin,$(LIBMLDB_SPARSE_PLUGIN_SOURCES),$(LIBMLDB_SPARSE_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_SPARSE_PLUGIN_SOURCES),-Imldb/sparse/ext))

#$(eval $(call mldb_plugin_library,sparse,mldb_sparse_plugin,$(LIBMLDB_SPARSE_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,sparse,mldb_sparse_plugin,doc))

#$(eval $(call include_sub_make,sparse_testing,testing,sparse_testing.mk))
