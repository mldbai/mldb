# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# CMake for embedding plugin for MLDB

#input: $(eval $(call include_sub_make,embedding_ext,ext,embedding_ext.mk))

set(LIBMLDB_EMBEDDING_PLUGIN_SOURCES
    embedding_plugin.cc
    embedding.cc
    svd.cc
)

set(LIBMLDB_EMBEDDING_PLUGIN_LINK
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
    gc
    svdlibc
    db
    mldb_feature_gen_plugin
    block
)

add_library(mldb_embedding_plugin ${LIBMLDB_EMBEDDING_PLUGIN_SOURCES})
target_link_libraries(mldb_embedding_plugin ${LIBMLDB_EMBEDDING_PLUGIN_LINK})

# Uncomment the following lines to replicate Makefile options and library setups in CMake

#input: $(eval $(call set_compile_option,$(LIBMLDB_EMBEDDING_PLUGIN_SOURCES),-Imldb/embedding/ext))
# target_include_directories(mldb_embedding_plugin PRIVATE -Imldb/embedding/ext)

#input: $(eval $(call mldb_plugin_library,embedding,mldb_embedding_plugin,$(LIBMLDB_EMBEDDING_PLUGIN_SOURCES),hubbub tinyxpath))
# target_link_libraries(mldb_embedding_plugin hubbub tinyxpath)

#input: $(eval $(call mldb_builtin_plugin,embedding,mldb_embedding_plugin,doc))
# add_mldb_builtin_plugin(embedding mldb_embedding_plugin doc)

#input: $(eval $(call include_sub_make,embedding_testing,testing,embedding_testing.mk))
