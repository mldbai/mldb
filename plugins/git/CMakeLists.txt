# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# # Makefile for git plugin for MLDB

# add_subdirectory(ext) # Input: #$(eval $(call include_sub_make,git_ext,ext,git_ext.mk))

# Git plugins
set(LIBMLDB_GIT_PLUGIN_SOURCES
    git_plugin.cc
    git.cc
)

set(LIBMLDB_GIT_PLUGIN_LINK
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
    libgit2package
)

add_library(mldb_git_plugin ${LIBMLDB_GIT_PLUGIN_SOURCES})
target_link_libraries(mldb_git_plugin ${LIBMLDB_GIT_PLUGIN_LINK})

# target_include_directories(mldb_git_plugin PRIVATE mldb/git/ext) # Input: #$(eval $(call set_compile_option,$(LIBMLDB_GIT_PLUGIN_SOURCES),-Imldb/git/ext))

# mldb_plugin_library(git mldb_git_plugin ${LIBMLDB_GIT_PLUGIN_SOURCES} hubbub tinyxpath) # Input: #$(eval $(call mldb_plugin_library,git,mldb_git_plugin,$(LIBMLDB_GIT_PLUGIN_SOURCES),hubbub tinyxpath))

# mldb_builtin_plugin(git mldb_git_plugin doc) # Input: #$(eval $(call mldb_builtin_plugin,git,mldb_git_plugin,doc))

# add_subdirectory(testing) # Input: #$(eval $(call include_sub_make,git_testing,testing,git_testing.mk))
