# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Variable to enable or disable manual tests
option(MLDB_RUN_MANUAL_TESTS "Enable running manual tests" OFF)

# Helper function to add tests
# test_name: globally unique name of the test
# dependencies: list of dependencies for the test (libraries that need linking). Semi-colon separated.
# options: list of options. Semi-colon separated.
function(add_mldb_test test_name dependencies options)
    add_executable(${test_name} ${test_name}.cc)
    target_link_libraries(${test_name} PRIVATE ${dependencies})

    # Handle test options
    if("boost" IN_LIST options)
        target_link_libraries(${test_name} PRIVATE Boost::unit_test_framework)
    endif()

    # Register the test based on options
    if("manual" IN_LIST options)
        if(MLDB_RUN_MANUAL_TESTS)
            add_test(NAME ${test_name} COMMAND ${test_name})
        endif()
    else()
        add_test(NAME ${test_name} COMMAND ${test_name})
    endif()

    if("timed" IN_LIST options)
        target_compile_definitions(${test_name} PRIVATE INCLUDE_TIMING)
    endif()

    add_test(NAME run_${test_name} COMMAND ${test_name})
endfunction()

# Add tests
add_mldb_test(bitops_test arch "boost")
add_mldb_test(wait_on_address_test arch "boost")
add_mldb_test(fslock_test arch "boost")
add_mldb_test(simd_vector_test arch "boost")
add_mldb_test(simd_vector_benchmark arch "boost;manual")
add_mldb_test(backtrace_test arch "boost")
add_mldb_test(bit_range_ops_test arch "boost")
add_mldb_test(info_test arch "boost")
add_mldb_test(rtti_utils_test arch "boost")
add_mldb_test(thread_specific_test arch "boost")
add_mldb_test(gc_test "gc;arch" "boost")
add_mldb_test(shared_gc_lock_test "gc;arch" "boost;manual") # Note: Known issues in some environments
add_mldb_test(rcu_protected_test "gc;arch" "boost;timed")

# Add Linux-specific test
if(UNIX AND NOT APPLE)
    add_mldb_test(vm_test arch "boost;manual") # May fail on recent Linux kernel versions
endif()

# Add x86_64-specific tests
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    add_mldb_test(sse2_math_test arch "boost")
    add_mldb_test(simd_test arch "boost")
    add_mldb_test(cpuid_test arch "boost")
    add_mldb_test(tick_counter_test arch "boost")
endif()

# Add Darwin ARM64-specific test
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    add_mldb_test(tick_counter_test arch "boost")
endif()

# Add general test
add_mldb_test(bit_array_test arch "boost")

# CUDA-related tests (conditional)
if(WITH_CUDA)
    # Uncomment and adjust the following lines if CUDA tests are to be included:
    # set_source_files_properties(cuda_device_query_test.cc PROPERTIES COMPILE_OPTIONS "-I${CUDA_INCLUDE_DIRS}")
    # add_mldb_test(cuda_device_query_test cudart "plain")
    # add_mldb_test(cuda_init_test arch_cuda "plain")
endif()
