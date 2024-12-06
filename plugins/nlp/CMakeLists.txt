# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Makefile for nlp plugin for MLDB

# Source files for the NLP plugin
set(LIBMLDB_NLP_PLUGIN_SOURCES
    nlp_plugin.cc
    tfidf.cc
    word2vec.cc
    nlp.cc
    sentiwordnet.cc
)

# Libraries to link with the NLP plugin
set(LIBMLDB_NLP_PLUGIN_LINK
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
    libstemmer
)

# Define the mldb_nlp_plugin library
add_library(mldb_nlp_plugin ${LIBMLDB_NLP_PLUGIN_SOURCES})
target_link_libraries(mldb_nlp_plugin ${LIBMLDB_NLP_PLUGIN_LINK})

# Include subdirectory for nlp testing
# add_subdirectory(testing) # This line is commented out as per the original Makefile
