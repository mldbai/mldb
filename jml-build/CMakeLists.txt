# Variable to enable or disable manual tests
option(MLDB_RUN_MANUAL_TESTS "Enable running manual tests" OFF)

# Helper function to add tests
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

# Function to set compile options for specific source files
function(set_compile_options source_file options)
    set_source_files_properties(${source_file} PROPERTIES COMPILE_FLAGS ${options})
endfunction()

# Function to set include paths for specific source files
function(set_include_paths source_file options)
    set_source_files_properties(${source_file} PROPERTIES INCLUDE_DIRECTORIES ${options})
endfunction()
