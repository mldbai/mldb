# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

#input: # Makefile for the fasttext library

set(FASTTEXT_SOURCE
    args.cc
    dictionary.cc
    matrix.cc
    vector.cc
    model.cc
    utils.cc
    fasttext.cc
)

list(TRANSFORM FASTTEXT_SOURCE PREPEND "fasttext/src/")

message(STATUS "FASTTEXT_SOURCE: ${FASTTEXT_SOURCE}")

# Set compile options for the fasttext source files
set_source_files_properties(${FASTTEXT_SOURCE} PROPERTIES
    INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/fasttext"
    COMPILE_FLAGS "-Wno-tautological-constant-out-of-range-compare"
)

# Create a library target for fasttext
add_library(fasttext ${FASTTEXT_SOURCE})
