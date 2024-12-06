# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

find_library(MAGIC_LIBRARY magic)

# Files for the SQL types library
set(SQL_TYPES_SOURCES
    cell_value.cc
    dataset_types.cc
    interval.cc
)

# Set compile options for specific files to ensure optimization
set_source_files_properties(cell_value.cc PROPERTIES COMPILE_OPTIONS "-O3")

# Create SQL types library
add_library(sql_types ${SQL_TYPES_SOURCES})
target_link_libraries(sql_types PRIVATE
    arch
    base
    types
    utils
    value_description
    any
    json_diff
    highwayhash
    hash)

# Files for the SQL expression library
set(SQL_EXPRESSION_SOURCES
    cell_value.cc
    sql_expression.cc
    expression_value.cc
    table_expression_operations.cc
    binding_contexts.cc
    builtin_functions.cc
    builtin_geo_functions.cc
    builtin_image_functions.cc
    builtin_http_functions.cc
    builtin_dataset_functions.cc
    builtin_aggregators.cc
    builtin_signal_functions.cc
    builtin_constants.cc
    interval.cc
    join_utils.cc
    tokenize.cc
    regex_helper.cc
    execution_pipeline.cc
    execution_pipeline_impl.cc
    sql_utils.cc
    sql_expression_operations.cc
    eval_sql.cc
    expression_value_conversions.cc
    expression_value_description.cc
)

# Set custom compile options for the S2 library related files
set_source_files_properties(builtin_geo_functions.cc PROPERTIES INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/ext/abseil-cpp;${CMAKE_SOURCE_DIR}/ext/s2geometry/src")
set_source_files_properties(builtin_geo_functions.cc PROPERTIES COMPILE_OPTIONS "-Wno-deprecated")

# Create SQL expression library
add_library(sql_expression ${SQL_EXPRESSION_SOURCES})
target_link_libraries(sql_expression PRIVATE
    arch
    base
    types
    value_description
    vfs
    sql_types
    utils
    any
    json_diff
    highwayhash
    hash
    s2
    edlib
    log
    pffft
    easyexif
    progress
    ${MAGIC_LIBRARY}
)

# Include the SQL testing subdirectory
add_subdirectory(testing)
