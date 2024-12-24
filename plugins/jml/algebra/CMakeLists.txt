# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBALGEBRA_SOURCES
    least_squares.cc
    irls.cc
    lapack.cc
    ilaenv.c
    svd.cc
    matrix.cc
    matrix_ops.cc
)

add_library(algebra ${LIBALGEBRA_SOURCES})
target_link_libraries(algebra PUBLIC arch base types value_description utils PRIVATE lapack blas db)

add_subdirectory(testing)
