# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

include(FindSnappy.cmake)

# Define the sources for the VFS library
set(LIBVFS_SOURCES
    fs_utils.cc
    filter_streams.cc
    http_streambuf.cc
    compressor.cc
    exception_ptr.cc
    libdb_init.cc
    gzip.cc
    bzip2.cc
    lzma.cc
    zstandard.cc
    snappy.cc
    lz4.cc
)

# Define the libraries to link with the VFS library
set(LIBVFS_LINK
    arch
    types
    http
    lz4
    lzma
    xxhash
    zstd
    ${SNAPPY_LIBRARY}
    db
    bz2
    value_description
    googleurl
    z
    base
)

# Create the VFS library
add_library(vfs ${LIBVFS_SOURCES})
target_link_libraries(vfs ${LIBVFS_LINK})

# Include the subdirectory for testing
add_subdirectory(testing)
