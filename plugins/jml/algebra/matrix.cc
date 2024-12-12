// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* matrix_ops.cc
   Jeremy Barnes, 15 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.

   Matrix-matrix and matrix-vector operations.
*/

#include "matrix.h"
#include "mldb/arch/exception.h"
#include <string>


namespace MLDB {

void matrix_throw_out_of_range(const char * function, int line, const char * what, ssize_t index, ssize_t min, ssize_t max)
{
    throw Exception("%s:%d: %s: matrix index %ld out of range [%ld, %ld)",
                           function, line, what, index, min, max);
}

void matrix_throw_incompatible_ranges(const char * function, int line, ssize_t dim1, ssize_t dim2, const char * what)
{
    throw Exception("%s:%d: %s: matrices have imcompatible dimensions %ld and %ld",
                           function, line, what, dim1, dim2);
}

std::string dims_to_string(const size_t * dims, size_t nd)
{
    std::string result = "(";
    for (size_t i = 0;  i < nd;  ++i) {
        if (i != 0) result += "x";
        result += std::to_string(dims[i]);
    }
    result += ")";
    return result;
}

void matrix_throw_incompatible_dimensions(const char * function, int line, const size_t * dims1, size_t nd1, const size_t * dims2, size_t nd2, const char * what)
{
    std::string dims1_str = dims_to_string(dims1, nd1);
    std::string dims2_str = dims_to_string(dims2, nd2);
    throw Exception("%s:%d: %s: matrices have incompatible dimensions %s and %s",
                           function, line, what, dims1_str.c_str(), dims2_str.c_str());
}

} // namespace MLDB


