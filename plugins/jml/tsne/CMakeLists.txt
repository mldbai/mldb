# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Makefile for tsne functionality

set(LIBTSNE_SOURCES
    tsne.cc)

set(LIBTSNE_LINK
    utils
    algebra
    arch
    stats
    base
)

add_library(tsne ${LIBTSNE_SOURCES})
target_link_libraries(tsne ${LIBTSNE_LINK})

add_subdirectory(testing)
