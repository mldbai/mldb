# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# mldb.ai behavior testing code

set(BEHAVIOR_TESTING_EXTRA_LINK "arch;value_description;types;base;vfs;db;gc")

add_mldb_test(behavior_domain_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;timed")
add_mldb_test(mutable_behavior_domain_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;timed")
add_mldb_test(mapped_behavior_domain_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;timed")
add_mldb_test(behavior_domain_valgrind_test "behavior;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;valgrind;manual")
add_mldb_test(boolean_expression_test "behavior;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;timed")
#add_mldb_test(bridged_behavior_domain_test "behavior" "boost;timed") # about to be removed
add_mldb_test(behavior_manager_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost")
add_mldb_test(id_serialization_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost")
add_mldb_test(root_behavior_manager_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;manual")
add_mldb_test(tranches_test "behavior" "boost")

#add_mldb_test(guarded_fs_lock_test "behavior;test_utils;arch" "boost")

add_mldb_test(mutable_behavior_stress_test "behavior;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost;manual")
add_executable(mutable_behavior_bench mutable_behavior_bench.cc)
target_link_libraries(mutable_behavior_bench base arch types behavior test_utils utils Boost::program_options)

add_executable(behavior_domain_read_bench behavior_domain_read_bench.cc)
target_link_libraries(behavior_domain_read_bench base arch types Boost::program_options behavior test_utils)

add_mldb_test(id_test "behavior;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost")
add_executable(id_profile id_profile.cc)
target_link_libraries(id_profile types behavior)
add_mldb_test(legacy_behavior_file_test "behavior;test_utils;${BEHAVIOR_TESTING_EXTRA_LINK}" "boost")
