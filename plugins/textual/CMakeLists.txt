# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Textual plugins

set(LIBMLDB_TEXTUAL_PLUGIN_SOURCES
    textual_plugin.cc
    csv_export_procedure.cc
    csv_writer.cc
    json_importer.cc
    importtext_procedure.cc
    sql_csv_scope.cc
    tokensplit.cc
)

set(LIBMLDB_TEXTUAL_PLUGIN_LINK
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

add_library(mldb_textual_plugin ${LIBMLDB_TEXTUAL_PLUGIN_SOURCES})

target_link_libraries(mldb_textual_plugin ${LIBMLDB_TEXTUAL_PLUGIN_LINK})

# Additional compile options or plugin specifics can be added here, if needed.
# Uncomment the following lines to include them

# target_include_directories(mldb_textual_plugin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/ext)

# Include subdirectory for testing if needed
# add_subdirectory(testing)
