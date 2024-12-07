# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Add subdirectory for AWS VFS Handlers
add_subdirectory(aws)

# We need the libssh2 library
find_library(SSH2 ssh2)

# Library for VFS Handlers
add_library(vfs_handlers sftp.cc archive.cc docker.cc)
target_link_libraries(vfs_handlers 
    hash 
    ${LIBARCHIVE_LIBRARIES}
    ${SSH2_LIBRARIES}
    aws_vfs_handlers 
    arch 
    value_description 
    vfs 
    http 
    types 
    credentials 
    utils
)

# Add subdirectory for Testing
add_subdirectory(testing)
