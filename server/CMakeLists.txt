# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

find_package(Python REQUIRED COMPONENTS Development)

# Library mldb
set(LIBMLDB_SOURCES
    mldb_server.cc
    plugin_manifest.cc
)

# Original Makefile line for reference: 
set(LIBMLDB_LINK
    service_peer
    mldb_builtin_plugins
    sql_expression
    runner
    credentials
    libgit2
    mldb_builtin
    command_expression
    vfs_handlers
    mldb_core
    mldb_engine
    rest
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
    http
)

add_library(mldb ${LIBMLDB_SOURCES})
target_link_libraries(mldb PUBLIC ${LIBMLDB_LINK})

# Forward dependencies
#add_library_forward_dependency(mldb mldb_builtin)
#add_library_forward_dependency(mldb mldb_builtin_plugins)

# Program mldb_runner
add_executable(mldb_runner mldb_runner.cc)
target_link_libraries(mldb_runner mldb Boost::program_options config ${LIBMLDB_LINK} io_base)

# Library mldb_platform_python
set(LIBMLDB_PLATFORM_PYTHON_SOURCES
    mldb_platform_python.cc
)
set(LIBMLDB_PLATFORM_PYTHON_LINK
    mldb
    ${Python_LIBRARIES}
    types
    arch
    value_description
    mldb_python_plugin
    io_base
    python_interpreter
    rest
)

set_source_files_properties(${LIBMLDB_PLATFORM_PYTHON_SOURCES} PROPERTIES INCLUDE_DIRECTORIES "${Python_INCLUDE_DIRS}")

add_library(mldb_platform_python ${LIBMLDB_PLATFORM_PYTHON_SOURCES})
target_link_libraries(mldb_platform_python PUBLIC ${Python_LIBRARIES})


#target_include_directories(mldb_platform_python PRIVATE ${PYTHON_INCLUDE_PATH})
