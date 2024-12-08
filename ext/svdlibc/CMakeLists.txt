# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Build instructions for svdlibc, the Lanczos solver

set(SVDLIBC_SOURCES
    svdlib.cc
    svdutil.cc
    las2.cc
)

set(SVDLIBC_LINK
    m
)

add_library(svdlibc ${SVDLIBC_SOURCES})
target_link_libraries(svdlibc ${SVDLIBC_LINK})
