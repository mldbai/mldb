# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# CMake configuration for msoffice plugin for MLDB

# add_subdirectory(ext)  # Equivalent to the commented-out include_sub_make for msoffice_ext

# Msoffice plugins
set(LIBMLDB_MSOFFICE_PLUGIN_SOURCES
    msoffice_plugin.cc
    xlsx_importer.cc
)

set(LIBMLDB_MSOFFICE_PLUGIN_LINK
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
    tinyxml2
)

add_library(mldb_msoffice_plugin ${LIBMLDB_MSOFFICE_PLUGIN_SOURCES})
target_link_libraries(mldb_msoffice_plugin PRIVATE ${LIBMLDB_MSOFFICE_PLUGIN_LINK})

# target_include_directories(mldb_msoffice_plugin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/ext)  # Equivalent to the commented-out set_compile_option

# mldb_plugin_library(msoffice mldb_msoffice_plugin ${LIBMLDB_MSOFFICE_PLUGIN_SOURCES} hubbub tinyxpath)  # Placeholder for original custom command

# mldb_builtin_plugin(msoffice mldb_msoffice_plugin doc)  # Placeholder for original custom command

# add_subdirectory(testing)  # Equivalent to the commented-out include_sub_make for msoffice_testing
