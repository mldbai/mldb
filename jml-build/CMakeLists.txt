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


# From https://github.com/google/libcxx/blob/master/cmake/Modules/GetTriple.cmake
# Define functions to get the host and target triple.

function(get_host_triple out out_arch out_vendor out_os)
  # Get the architecture.
  set(arch ${CMAKE_HOST_SYSTEM_PROCESSOR})
  if (arch STREQUAL "x86")
    set(arch "i686")
  endif()
  # Get the vendor.
  if (${CMAKE_HOST_SYSTEM_NAME} STREQUAL "Darwin")
    set(vendor "apple")
  else()
    set(vendor "pc")
  endif()
  # Get os.
  if (${CMAKE_HOST_SYSTEM_NAME} STREQUAL "Windows")
    set(os "win32")
  else()
    string(TOLOWER ${CMAKE_HOST_SYSTEM_NAME} os)
  endif()
  set(triple "${arch}-${vendor}-${os}")
  set(${out} ${triple} PARENT_SCOPE)
  set(${out_arch} ${arch} PARENT_SCOPE)
  set(${out_vendor} ${vendor} PARENT_SCOPE)
  set(${out_os} ${os} PARENT_SCOPE)
  message(STATUS "Host triple: ${triple}")
endfunction()

function(get_target_triple out out_arch out_vendor out_os)
  # Get the architecture.
  set(arch ${CMAKE_SYSTEM_PROCESSOR})
  if (arch STREQUAL "x86")
    set(arch "i686")
  endif()
  # Get the vendor.
  if (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    set(vendor "apple")
  else()
    set(vendor "pc")
  endif()
  # Get os.
  if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(os "win32")
  else()
    string(TOLOWER ${CMAKE_SYSTEM_NAME} os)
  endif()
  set(triple "${arch}-${vendor}-${os}")
  set(${out} ${triple} PARENT_SCOPE)
  set(${out_arch} ${arch} PARENT_SCOPE)
  set(${out_vendor} ${vendor} PARENT_SCOPE)
  set(${out_os} ${os} PARENT_SCOPE)
  message(STATUS "Target triple: ${triple}")
endfunction()