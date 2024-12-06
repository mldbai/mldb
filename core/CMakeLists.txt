# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBMLDB_CORE_SOURCES
    plugin.cc
    dataset.cc
    procedure.cc
    recorder.cc
    function.cc
    sensor.cc
    value_function.cc
    mldb_entity.cc
    mldb_engine.cc
    dataset_scope.cc
    bound_queries.cc
    bucket.cc
    analytics.cc
)

set(LIBMLDB_CORE_LINK
    sql_expression
    rest_entity
    rest
    arch
    value_description
    types
    base
    utils
    any
    watch
    logging
    log
    progress
    any
    block
)

add_library(mldb_core ${LIBMLDB_CORE_SOURCES})

target_link_libraries(mldb_core ${LIBMLDB_CORE_LINK})
