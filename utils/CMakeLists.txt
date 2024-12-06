# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Libraries
add_library(config config.cc)
target_link_libraries(config PUBLIC Boost::program_options)

add_library(log log.cc)
target_link_libraries(log PUBLIC arch config)

# Utils Library
set(LIBUTILS_SOURCES
    environment.cc
    string_functions.cc
    csv.cc
    floating_point.cc
    buckets.cc
    confidence_intervals.cc
    quadtree.cc
    for_each_line.cc
    tmpdir.cc
)

set(LIBUTILS_LINK
    arch
    vfs
    db
    block
    log
    arch
    base
    types
    ${STD_FILESYSTEM_LIBNAME}
)

add_library(utils ${LIBUTILS_SOURCES})
target_link_libraries(utils PUBLIC ${LIBUTILS_LINK})

# Set compile options
set_source_files_properties(hash.cc PROPERTIES COMPILE_FLAGS -fpermissive)
set_source_files_properties(confidence_intervals.cc PROPERTIES COMPILE_FLAGS -O3)

add_library(progress progress.cc)
target_link_libraries(progress PUBLIC types value_description arch)

add_library(json_diff json_diff.cc json_utils.cc)
target_link_libraries(json_diff PUBLIC arch base value_description types utils highwayhash)

# Runner Common
set(LIBRUNNERCOMMON_SOURCES
    runner_common.cc
)

set(LIBRUNNERCOMMON_LINK
    arch
    types
    value_description
)

add_library(runner_common ${LIBRUNNERCOMMON_SOURCES})
target_link_libraries(runner_common PUBLIC ${LIBRUNNERCOMMON_LINK})

add_executable(runner_helper runner_helper.cc)
target_link_libraries(runner_helper PRIVATE runner_common arch)

# Runner
set(LIBRUNNER_SOURCES
    sink.cc
    runner.cc
)

set(LIBRUNNER_LINK
    runner_common
    io_base
    value_description
    logging
    utils
    arch
    types
)

set_source_files_properties(runner.cc PROPERTIES COMPILE_FLAGS "-DBIN=\"${BIN}\"")

add_library(runner ${LIBRUNNER_SOURCES})
target_link_libraries(runner PUBLIC ${LIBRUNNER_LINK})

add_custom_target(check_runner_helper DEPENDS ${CMAKE_BINARY_DIR}/runner_helper)

# Command Expression
set(LIBCOMMAND_EXPRESSION_SOURCES
    command.cc
    command_expression.cc
)

set(LIBCOMMAND_EXPRESSION_LINK
    runner
    value_description
    arch
    utils
    json_diff
    types
    base
    any
)

add_library(command_expression ${LIBCOMMAND_EXPRESSION_SOURCES})
target_link_libraries(command_expression PUBLIC ${LIBCOMMAND_EXPRESSION_LINK})

add_executable(json_format json_format.cc)
target_link_libraries(json_format PRIVATE command_expression Boost::program_options value_description base vfs)

# Subdirectories
add_subdirectory(testing)
