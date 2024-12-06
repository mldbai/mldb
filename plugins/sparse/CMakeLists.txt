# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Makefile for sparse plugin for MLDB

#input: #$(eval $(call include_sub_make,sparse_ext,ext,sparse_ext.mk))
# add_subdirectory(ext)

# Sparse plugins

set(LIBMLDB_SPARSE_PLUGIN_SOURCES
    sparse_plugin.cc
    sparse_matrix_dataset.cc
)

set(LIBMLDB_SPARSE_PLUGIN_LINK
    mldb_core
    mldb_engine
    arch
    types
    utils
    sql_expression
    value_description
    base
    progress
    rest
    db
    vfs
    log
    link
    rest
    any
    watch
    rest_entity
    mldb_builtin_base
    mldb_builtin
    sql_types
)

add_library(mldb_sparse_plugin ${LIBMLDB_SPARSE_PLUGIN_SOURCES})
target_link_libraries(mldb_sparse_plugin ${LIBMLDB_SPARSE_PLUGIN_LINK})

#input: #$(eval $(call set_compile_option,$(LIBMLDB_SPARSE_PLUGIN_SOURCES),-Imldb/sparse/ext))
target_include_directories(mldb_sparse_plugin PRIVATE mldb/sparse/ext)

#input: #$(eval $(call mldb_plugin_library,sparse,mldb_sparse_plugin,$(LIBMLDB_SPARSE_PLUGIN_SOURCES),hubbub tinyxpath))
# The mldb_plugin_library is a custom macro/function in the project. Assuming it's defined elsewhere:
# mldb_plugin_library(sparse mldb_sparse_plugin ${LIBMLDB_SPARSE_PLUGIN_SOURCES} hubbub tinyxpath)

#input: #$(eval $(call mldb_builtin_plugin,sparse,mldb_sparse_plugin,doc))
# The mldb_builtin_plugin is a custom macro/function in the project. Assuming it's defined elsewhere:
# mldb_builtin_plugin(sparse mldb_sparse_plugin doc)

#input: #$(eval $(call include_sub_make,sparse_testing,testing,sparse_testing.mk))
# add_subdirectory(testing)
