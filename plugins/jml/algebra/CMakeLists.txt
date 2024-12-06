# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

set(LIBALGEBRA_SOURCES
    least_squares.cc
    irls.cc
    lapack.cc
    ilaenv.c
    svd.cc
    matrix_ops.cc
)

add_library(algebra ${LIBALGEBRA_SOURCES})
target_link_libraries(algebra utils lapack blas db arch base value_description Boost::timer)

add_subdirectory(testing)
