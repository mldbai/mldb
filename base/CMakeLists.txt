# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
#set(ICU_DEBUG ON)

find_package(ICU REQUIRED COMPONENTS data i18n uc)
#message("ICU_INCLUDE_DIRS: ${ICU_INCLUDE_DIRS}")

# Define the base library
set(LIBBASE_SOURCES
    parse_context.cc
    thread_pool.cc
    parallel.cc
    optimized_path.cc
    hex_dump.cc
    ../types/string.cc
)

set(LIBBASE_LINK
    PRIVATE
        arch
        gc
    PUBLIC
        ICU::i18n
        ICU::data
        ICU::uc
)

# Create the base library target
add_library(base ${LIBBASE_SOURCES})
target_link_libraries(base ${LIBBASE_LINK})
#target_link_libraries(base PRIVATE arch gc PUBLIC ICU::i18n ICU::data ICU::uc)

# Set compile options for specific files
add_library(hash hash.cc)
target_link_libraries(hash PRIVATE arch cryptopp)
set_source_files_properties(hash.cc PROPERTIES COMPILE_FLAGS "-fpermissive")
set_source_files_properties(../types/string.cc PROPERTIES INCLUDE_DIRECTORIES "${ICU_INCLUDE_DIRS}")


message("CRYPTOPP_INCLUDE_DIR: ${CRYPTOPP_INCLUDE_DIR}")
target_include_directories(hash PRIVATE ${CRYPTOPP_INCLUDE_DIR})

# Define the hash library

# Include the base_testing subdirectory
add_subdirectory(testing)
