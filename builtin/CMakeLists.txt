# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

find_library(SSH2 ssh2)

set(LIBMLDB_BUILTIN_BASE_SOURCES
    merged_dataset.cc
    transposed_dataset.cc
    joined_dataset.cc
    sub_dataset.cc
    filtered_dataset.cc
    sampled_dataset.cc
    union_dataset.cc
    basic_procedures.cc
    sql_functions.cc
    fetcher.cc
    shared_library_plugin.cc
    script_output.cc
    plugin_resource.cc
    script_procedure.cc
    docker_plugin.cc
    external_python_procedure.cc
    script_function.cc
    mock_procedure.cc
    continuous_dataset.cc
    melt_procedure.cc
    ranking_procedure.cc
    pooling_function.cc
    permuter_procedure.cc
    datasetsplit_procedure.cc
    summary_statistics_proc.cc
    metric_space.cc
    matrix.cc
    intersection_utils.cc
)

set(LIBMLDB_BUILTIN_BASE_LINK
    mldb_core
    mldb_engine
    runner
    libgit2
    ${SSH2_LIBRARIES}
    arch
    value_description
    types
    base
    utils
    sql_expression
    vfs
    rest
    link
    rest_entity
    any
    json_diff
    progress
    log
    watch
    gc
    mldb_core
    utils
    sql_types
)

# Shared_mutex only in C++17
set_source_files_properties(plugin_resource.cc PROPERTIES COMPILE_OPTIONS "-Imldb/ext/libgit2/include")

add_library(mldb_builtin_base ${LIBMLDB_BUILTIN_BASE_SOURCES})
target_link_libraries(mldb_builtin_base ${LIBMLDB_BUILTIN_BASE_LINK})

# Builtin programming language support for MLDB

add_subdirectory(js)
add_subdirectory(python)

set(LIBMLDB_BUILTIN_LINK
    mldb_builtin_base
    mldb_js_plugin
    mldb_python_plugin
)

# For now, this library is just a convenient way to link the other libraries
set(LIBMLDB_BUILTIN_SOURCES
    mldb_builtin.cc
)


add_library(mldb_builtin ${LIBMLDB_BUILTIN_SOURCES})
target_link_libraries(mldb_builtin ${LIBMLDB_BUILTIN_LINK})
