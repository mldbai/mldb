# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Define extra libraries for boosting testing
set(BOOSTING_TESTING_EXTRA_LIBS base utils value_description arch boosting jml_utils)

# Add tests
add_mldb_test(decision_tree_xor_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(split_test "boosting;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(decision_tree_multithreaded_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(decision_tree_unlimited_depth_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(glz_classifier_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(probabilizer_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(feature_info_test "boosting;utils;arch;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(weighted_training_test "boosting;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")
add_mldb_test(feature_set_test "boosting;${BOOSTING_TESTING_EXTRA_LIBS}" "boost")

# Add program
add_executable(dataset_nan_test dataset_nan_test.cc)
target_link_libraries(dataset_nan_test boosting utils arch boosting_tools ${BOOSTING_TESTING_EXTRA_LIBS})

# Conditional test for CUDA
if(CUDA_ENABLED)
    add_mldb_test(split_cuda_test "boosting_cuda" "boost")
endif()
