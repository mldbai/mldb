# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Create the block library
add_library(block
    memory_region.cc
    zip_serializer.cc
    file_serializer.cc
    content_descriptor.cc
    content.cc
)

# Link block library to required libraries
target_link_libraries(block
    vfs
    ${LIBARCHIVE_LIB_NAME}
    types
    arch
    db
    base
    value_description
    any
)

# Include the subdirectory for testing
add_subdirectory(testing)
