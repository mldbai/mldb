# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# CMake configuration for feature_gen plugin for MLDB

# The following source files comprise the feature_gen plugin
set(LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES
    feature_gen_plugin.cc
    stats_table_procedure.cc
    dist_table_procedure.cc
    feature_generators.cc
    bucketize_procedure.cc
)

# Libraries that need to be linked with the feature_gen plugin
set(LIBMLDB_FEATURE_GEN_PLUGIN_LINK
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
    highwayhash
)

# Define a library target for the feature_gen plugin
add_library(mldb_feature_gen_plugin ${LIBMLDB_FEATURE_GEN_PLUGIN_SOURCES})

# Link required libraries to the feature_gen plugin target
target_link_libraries(mldb_feature_gen_plugin PRIVATE ${LIBMLDB_FEATURE_GEN_PLUGIN_LINK})

# Optionally, include subdirectory for additional builds (commented out in original Makefile)
# add_subdirectory(ext)
# add_subdirectory(testing)

# Set compile options if necessary (commented out in original Makefile)
# Uncomment and adjust the following line if additional include directories are needed
# target_include_directories(mldb_feature_gen_plugin PRIVATE mldb/feature_gen/ext)
