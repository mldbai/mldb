# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Library for test utilities
add_library(test_utils
    benchmarks.cc
    fixtures.cc
    threaded_test.cc
)

target_link_libraries(test_utils
    arch
    utils
    ${STD_FILESYSTEM_LIBNAME}
)

# Test definitions
add_mldb_test(json_diff_test "json_diff;vfs;value_description;arch" "boost")
add_mldb_test(json_hash_test "json_diff;value_description;arch" "boost")
add_mldb_test(command_expression_test "command_expression;value_description;arch;types;test_utils" "boost")
add_mldb_test(config_test "config;boost_program_options" "boost")
add_mldb_test(logger_test "log" "boost")
add_mldb_test(compact_vector_test "arch" "boost")
add_mldb_test(fixture_test "test_utils" "boost")
add_mldb_test(print_utils_test "" "boost")

# Program definition
add_executable(runner_test_helper runner_test_helper.cc)
target_link_libraries(runner_test_helper
    utils
    arch
    io_base
    value_description
)

add_mldb_test(runner_test "runner;arch;value_description;io_base;utils;vfs;types" "boost")
add_mldb_test(runner_stress_test "runner;io_base;arch" "boost;manual")

# Test dependencies
add_dependencies(runner_test runner_test_helper)
add_dependencies(runner_stress_test runner_test_helper)

add_mldb_test(sink_test "runner;utils;io_base;arch" "boost")

add_mldb_test(lightweight_hash_test "arch;utils" "boost")
add_mldb_test(parse_context_test "utils;arch;vfs;base" "boost")

add_mldb_test(environment_test "utils;arch" "boost")
add_mldb_test(string_functions_test "arch;utils" "boost")
add_mldb_test(csv_parsing_test "arch;utils;base" "boost")
add_mldb_test(round_test "" "boost")
add_mldb_test(for_each_line_test "utils;log;arch;block;types;value_description;vfs" "boost")
add_mldb_test(floating_point_test "utils;log;arch" "boost")
