# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
# Jeremy Barnes, 2 November 2011
# Copyright (c) 2011 mldb.ai inc.  All rights reserved.

if(APPLE)
    # Find the relevant Homebrew directories for macOS
    find_path(HOMEBREW_OPT_DIR icu4c
        PATHS /opt/homebrew/opt /usr/local/opt
        NO_DEFAULT_PATH
    )
    if(NOT HOMEBREW_OPT_DIR)
        message(FATAL_ERROR "Couldn't find Homebrew opt directories")
    endif()

    set(ICU_ROOT "${HOMEBREW_OPT_DIR}/icu4c")
endif()

#message("ICU_ROOT: ${ICU_ROOT}")
set(ICU_DEBUG ON)

find_package(ICU REQUIRED COMPONENTS data i18n)
message("ICU_INCLUDE_DIRS: ${ICU_INCLUDE_DIRS}")

set(JSONCPP_SOURCES
    ../ext/jsoncpp/json_reader.cpp
    ../ext/jsoncpp/json_writer.cpp
    ../ext/jsoncpp/json_value.cpp
)

set(LIBVALUE_DESCRIPTION_SOURCES
    string.cc
    value_description.cc
    basic_value_descriptions.cc
    libc_value_descriptions.cc
    json_parsing.cc
    json_printing.cc
    dtoa.c
    dtoa_impl.cc
    meta_value_description.cc
    distribution_description.cc
)

set(LIBVALUE_DESCRIPTION_LINK
    arch
    base
    ICU::i18n
    ICU::data
)

#set(JSONCPP_COMPILER_FLAGS "-Wno-unused-result")

#foreach(source IN LISTS JSONCPP_SOURCES)
#    set_compile_options(${source} "${JSONCPP_COMPILER_FLAGS}")
#endforeach()

add_library(value_description ${LIBVALUE_DESCRIPTION_SOURCES} ${JSONCPP_SOURCES})
target_link_libraries(value_description PRIVATE ${LIBVALUE_DESCRIPTION_LINK})

add_library(any any.cc)
target_link_libraries(any PRIVATE value_description arch base)

set(LIBTYPES_SOURCES
    date.cc
    localdate.cc
    url.cc
    periodic_utils.cc
    dtoa.c
    regex.cc
    periodic_utils_value_descriptions.cc
    path.cc
    annotated_exception.cc
)

set(LIBTYPES_LINK
    ${CMAKE_DL_LIBS}
    Boost::regex
    Boost::date_time
    googleurl
    cityhash
    value_description
    highwayhash
    re2
    any
    ICU::i18n
    ICU::data
    ICU::uc
    base
    arch
)

# Should always be optimized since otherwise everything will be way too slow
set_source_files_properties(path.cc PROPERTIES COMPILE_OPTIONS "-O3")

#message("ICU_INCLUDE_DIRS: ${ICU_INCLUDE_DIRS}")
set_source_files_properties(string.cc PROPERTIES INCLUDE_DIRECTORIES "${ICU_INCLUDE_DIRS}")
set_source_files_properties(regex.cc  PROPERTIES  INCLUDE_DIRECTORIES "${ICU_INCLUDE_DIRS};${RE2_INCLUDE_PATH}" COMPILE_FLAGS -Wno-unused-variable)

#set_include_paths(regex.cc "${ICU_INCLUDE_DIRS}")

#set_compile_options(regex.cc "-I${RE2_INCLUDE_PATH} -I${ICU_INCLUDE_DIRS} -Wno-unused-variable")

#message("CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
#message("LIBRARY_OUTPUT_DIRECTORY: ${LIBRARY_OUTPUT_DIRECTORY}")

# Handle the prerequisite for building libtypes
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/lib/date_timezone_spec.csv
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${CMAKE_BINARY_DIR}/lib/date_timezone_spec.csv ${CMAKE_CURRENT_SOURCE_DIR}/date_timezone_spec.csv
#    COMMENT "Copying ${CMAKE_CURRENT_SOURCE_DIR}/date_timezone_spec.csv to ${CMAKE_BINARY_DIR}/lib/date_timezone_spec.csv"
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/date_timezone_spec.csv
)

#add_custom_target(date_timezone_spec ALL
#    DEPENDS ${LIB}/date_timezone_spec.csv
#)

add_library(types ${LIBTYPES_SOURCES})
target_link_libraries(types PRIVATE ${LIBTYPES_LINK})

add_subdirectory(testing)
add_subdirectory(db)
