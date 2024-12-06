# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# List source files for the 'db' library
set(LIBDB_SOURCES
    compact_size_types.cc
    nested_archive.cc
    portable_iarchive.cc
    portable_oarchive.cc
    file_read_buffer.cc
)

# Create the 'db' library and specify its dependencies
add_library(db ${LIBDB_SOURCES})
target_link_libraries(db PRIVATE arch)

# Include the 'db_testing' subdirectory
add_subdirectory(testing)
