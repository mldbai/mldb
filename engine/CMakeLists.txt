# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Define source files for the mldb_engine library
set(LIBMLDB_ENGINE_SOURCES
    dataset_utils.cc
    forwarded_dataset.cc
    column_scope.cc
    dataset_collection.cc
    procedure_collection.cc
    procedure_run_collection.cc
    function_collection.cc
    credential_collection.cc
    type_collection.cc
    plugin_collection.cc
    external_plugin.cc
    sensor_collection.cc
    static_content_macro.cc
    static_content_handler.cc
)

# Define libraries to link with mldb_engine
set(LIBMLDB_ENGINE_LINK
    sql_expression
    credentials
    mldb_core
    command_expression
    hoedown
    arch
    value_description
    types
    base
    watch
    any
    utils
    rest
    link
    rest_entity
    runner
    log
    logging
    gc
    io_base
    http
    utils
    db
    vfs
    json_diff
    sql_types
)

# Create the mldb_engine library
add_library(mldb_engine ${LIBMLDB_ENGINE_SOURCES})

# Link necessary libraries
target_link_libraries(mldb_engine
    ${LIBMLDB_ENGINE_LINK}
)
