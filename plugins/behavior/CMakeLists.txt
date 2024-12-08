# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.
# Behaviour makefile
# Jeremy Barnes, 29 July 2010

set(LIBBEHAVIOR_SOURCES
    behavior_domain.cc
    mapped_behavior_domain.cc
    mutable_behavior_domain.cc
    merged_behavior_domain.cc
    mapped_value.cc
    behavior_svd.cc
    behavior_manager.cc
    boolean_expression.cc
    boolean_expression_parser.cc
    behavior_types.cc
    behavior_utils.cc
    tranches.cc
    id.cc
)

set(LIBBEHAVIOR_LINK
    arch
    utils
    db
    gc
    runner
    vfs_handlers
    svdlibc
    types
    value_description
    sql_expression
    mldb_builtin_base
    utils
    base
    cityhash
    vfs
)

add_library(behavior ${LIBBEHAVIOR_SOURCES})
target_link_libraries(behavior ${LIBBEHAVIOR_LINK})
target_include_directories(behavior PRIVATE "pro")

set(LIBBEHAVIOR_PLUGIN_SOURCES
    behavior_dataset.cc
    binary_behavior_dataset.cc 
)

set(LIBBEHAVIOR_PLUGIN_LINK
    behavior 
    arch 
    value_description 
    sql_types 
    types 
    log 
    vfs 
    rest 
    sql_expression 
    base 
    mldb_core 
    mldb_engine 
    progress
)

add_library(mldb_behavior_plugin ${LIBBEHAVIOR_PLUGIN_SOURCES})
target_link_libraries(mldb_behavior_plugin ${LIBBEHAVIOR_PLUGIN_LINK})

add_subdirectory(testing)
