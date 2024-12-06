# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Define the base library
set(LIBBASE_SOURCES
    parse_context.cc
    thread_pool.cc
    parallel.cc
    optimized_path.cc
    hex_dump.cc
)

# Create the base library target
add_library(base ${LIBBASE_SOURCES})
target_link_libraries(base PRIVATE arch gc)

# Set compile options for specific files
add_library(hash hash.cc)
target_link_libraries(hash PRIVATE arch cryptopp)
set_source_files_properties(hash.cc PROPERTIES COMPILE_FLAGS "-fpermissive")

message("CRYPTOPP_INCLUDE_DIR: ${CRYPTOPP_INCLUDE_DIR}")
target_include_directories(hash PRIVATE ${CRYPTOPP_INCLUDE_DIR})

# Define the hash library

# Include the base_testing subdirectory
add_subdirectory(testing)
