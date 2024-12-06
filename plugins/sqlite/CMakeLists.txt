# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Library sources
set(LIBMLDB_SQLITE_PLUGIN_SOURCES
    sqlite_plugin.cc
    sqlite_dataset.cc
)

# Library dependencies
set(LIBMLDB_SQLITE_PLUGIN_LINK
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
    sqlite-mldb
)

# Create the sqlite plugin library
add_library(mldb_sqlite_plugin ${LIBMLDB_SQLITE_PLUGIN_SOURCES})
target_link_libraries(mldb_sqlite_plugin ${LIBMLDB_SQLITE_PLUGIN_LINK})

# Additional includes, compiler options, or macros can be added here if needed
# For example, setting include directories:
# target_include_directories(mldb_sqlite_plugin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/sqlite/ext)

# Uncomment and use the following lines if integrating with MLDB's plugin build system
# include(mldb_plugin_library)
# mldb_plugin_library(sqlite mldb_sqlite_plugin ${LIBMLDB_SQLITE_PLUGIN_SOURCES} hubbub tinyxpath)

# Documentation/build related configuration for builtin plugins if needed
# include(mldb_builtin_plugin)
# mldb_builtin_plugin(sqlite mldb_sqlite_plugin doc)

# Add subdirectory for additional testing if required
# add_subdirectory(testing/sqlite_testing)
