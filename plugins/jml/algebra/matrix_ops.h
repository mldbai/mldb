/* matrix_ops.h                                                    -*- C++ -*-
   Jeremy Barnes, 15 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Operations on matrices.
*/

#pragma once


#include "mldb/utils/distribution.h"
#include "mldb/arch/exception.h"
#include "mldb/plugins/jml/algebra/matrix.h"
#include <iostream>
#include "mldb/utils/float_traits.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/utils/string_functions.h"
#include "mldb/arch/cache.h"
#include "mldb/utils/possibly_dynamic_buffer.h"

namespace MLDB {

#if 0
template<class Float>
std::ostream &
operator << (std::ostream & stream, const MLDB::MatrixRef<Float, 2> & m)
{
    for (unsigned i = 0;  i < m.dim(0);  ++i) {
        stream << "    [";
        for (unsigned j = 0;  j < m.dim(1);  ++j)
            stream << MLDB::format(" %8.3g", m[i][j]);
        stream << " ]" << std::endl;
    }
    return stream;
}
#endif

inline void warmup_cache_all_levels(const float * mem, size_t n)
{
    // TODO: assumes 64 byte cache lines
    // TODO: prefetch?
    float total MLDB_UNUSED = 0.0;
    for (unsigned i = 0;  i < n;  i += 16)
        total += mem[n];
}

inline void warmup_cache_all_levels(const double * mem, size_t n)
{
    // TODO: assumes 64 byte cache lines
    // TODO: prefetch?
    double total MLDB_UNUSED = 0.0;
    for (unsigned i = 0;  i < n;  i += 8)
        total += mem[n];
}

// Copy a chunk of a matrix transposed to another place
template<typename Float>
void copy_transposed(MLDB::MatrixRef<Float, 2> & A,
                     int i0, int i1, int j0, int j1)
{
    // How much cache will be needed to hold the input data?
    size_t mem = (i1 - i0) * (j1 - j0) * sizeof(float);

    // Fits in memory (with some allowance for loss): copy directly
    if (mem * 4 / 3 < l1_cache_size) {
        // 1.  Prefetch everything we need to access with non-unit stride
        //     in cache in order
        for (unsigned i = i0;  i < i1;  ++i)
            warmup_cache_all_levels(&A[i][j0], j1 - j0);

        // 2.  Do the work
        for (unsigned j = j0;  j < j1;  ++j)
            streaming_copy_from_strided(&A[j][i0], &A[i0][j], A.strides()[0],
                                        i1 - i0);
        return;
    }

    // Otherwise, we recurse
    int spliti = (i0 + i1) / 2;
    int splitj = (j0 + j1) / 2;

    // TODO: try to ensure a power of 2

    copy_transposed(A, i0, spliti, j0, splitj);
    copy_transposed(A, i0, spliti, splitj, j1);
    copy_transposed(A, spliti, i1, j0, splitj);
    copy_transposed(A, spliti, i1, splitj, j1);
}

// Copy everything above the diagonal below the diagonal of the given part
// of the matrix.  We do it by divide-and-conquer, sub-dividing the problem
// until we get something small enough to fit in the cache.
template<typename Float>
void copy_lower_to_upper(MLDB::MatrixRef<Float, 2> & A,
                         int i0, int i1)
{
    int j0 = i0;
    int j1 = i1;

    // How much cache will be needed to hold the input data?
    size_t mem = (i1 - i0) * (j1 - j0) * sizeof(float) / 2;

    // Fits in memory (with some allowance for loss): copy directly
    if (mem * 4 / 3 < l1_cache_size) {
        // 1.  Prefetch everything in cache in order
        for (unsigned i = i0;  i < i1;  ++i)
            warmup_cache_all_levels(&A[i][j0], i - j0);

        // 2.  Do the work
        for (unsigned j = i0;  j < j1;  ++j)
            streaming_copy_from_strided(&A[j][j], &A[j][j], A.strides()[0],
                                        j1 - j);


        return;
    }

    // Otherwise, we recurse
    int split = (i0 + i1) / 2;
    // TODO: try to ensure a power of 2

    /* i0+
         |\
         | \
         |  \
         |   \
       s +----\
         |    |\
         |    | \
         |    |  \
       i1+----+---+
        i0    s   i1
    */

    copy_lower_to_upper(A, i0, split);
    copy_lower_to_upper(A, split, i1);
    copy_transposed(A, split, i1, j0, split);
}

template<typename Float>
void copy_lower_to_upper(MLDB::MatrixRef<Float, 2> & A)
{
    int n = A.dim(0);
    if (n != A.dim(1))
        throw Exception("copy_upper_to_lower: matrix is not square");

    copy_lower_to_upper(A, 0, n);
}

template<typename Float>
MLDB::Matrix<Float, 2>
transpose(const MLDB::MatrixRef<Float, 2> & A)
{
    MLDB::Matrix<Float, 2> X(A.dim(1), A.dim(0));
    for (unsigned i = 0;  i < A.dim(0);  ++i)
        for (unsigned j = 0;  j < A.dim(1);  ++j)
            X[j][i] = A[i][j];
    return X;
}

template<typename Float>
MLDB::Matrix<Float, 2>
diag(const distribution<Float> & d)
{
    MLDB::Matrix<Float, 2> D(d.size(), d.size());
    for (unsigned i = 0;  i < d.size();  ++i)
        D[i][i] = d[i];
    return D;
}

template<typename Float>
MLDB::Matrix<Float, 2> &
setIdentity(int numDim, MLDB::Matrix<Float, 2> & D)
{
    D.resize(numDim, numDim);
    for (unsigned i = 0;  i < numDim;  ++i)
    {
        for (unsigned j = 0;  j < numDim;  ++j)
        {
            if (i == j)
                D[i][j] = Float(1.0f);    
            else
                D[i][j] = Float(0.0f);
        }
    }
    return D;
}

template<typename Float>
std::string print_size(const MLDB::MatrixRef<Float, 2> & array)
{
    return format("%dx%d", (int)array.dim(0), (int)array.dim(1));
}


/*****************************************************************************/
/* MATRIX VECTOR                                                             */
/*****************************************************************************/

template<typename FloatR, typename Float1, typename Float2>
distribution<FloatR>
multiply_r(const MLDB::MatrixRef<Float1, 2> & A,
           const distribution<Float2> & b)
{
    if (b.size() != A.dim(1))
        throw Exception(format("multiply(matrix, vector): "
                               "shape (%dx%d) x (%dx1) wrong",
                               (int)A.dim(0), (int)A.dim(1),
                               (int)b.size()));
    distribution<FloatR> result(A.dim(0), 0.0);
    for (unsigned i = 0;  i < A.dim(0);  ++i)
        result[i] = SIMD::vec_dotprod_dp(&A[i][0], &b[0], A.dim(1));
    //for (unsigned j = 0;  j < A.dim(1);  ++j)
    //    result[i] += b[j] * A[i][j];
    return result;
}

template<typename Float1, typename Float2>
//MLDB_ALWAYS_INLINE
distribution<typename float_traits<Float1, Float2>::return_type>
multiply(const MLDB::MatrixRef<Float1, 2> & A,
         const distribution<Float2> & b)
{
    return multiply_r<typename float_traits<Float1, Float2>::return_type>(A, b);
}

template<typename Float1, typename Float2>
//MLDB_ALWAYS_INLINE
distribution<typename float_traits<Float1, Float2>::return_type>
operator * (const MLDB::MatrixRef<Float1, 2> & A,
            const distribution<Float2> & b)
{
    typedef typename float_traits<Float1, Float2>::return_type FloatR;
    return multiply_r<FloatR, Float1, Float2>(A, b);
}


/*****************************************************************************/
/* VECTOR MATRIX                                                             */
/*****************************************************************************/

template<typename FloatR, typename Float1, typename Float2>
distribution<FloatR>
multiply_r(const distribution<Float2> & b,
           const MLDB::MatrixRef<Float1, 2> & A)
{
    if (b.size() != A.dim(0))
        throw Exception(format("multiply(vector, matrix): "
                               "shape (1x%d) x (%dx%d) wrong",
                               (int)b.size(),
                               (int)A.dim(0), (int)A.dim(1)));

    distribution<FloatR> result(A.dim(1), 0.0);
#if 1 // more accurate
    std::vector<double> accum(A.dim(1), 0.0);
    using namespace std;
    //cerr << "A.dim(1) = " << A.dim(1) << endl;
    //cerr << "accum = " << accum << endl;
    //for (unsigned j = 0;  j < A.dim(1);  ++j)
    //    accum[j] = 0.0;
    for (unsigned i = 0;  i < A.dim(0);  ++i) {
        //for (unsigned j = 0;  j < A.dim(1);  ++j)
        //    result[j] += b[i] * A[i][j];
        SIMD::vec_add(&accum[0], b[i], &A[i][0], &accum[0], A.dim(1));
    }
    std::copy(accum.begin(), accum.end(), result.begin());
#else
    for (unsigned i = 0;  i < A.dim(0);  ++i)
        SIMD::vec_add(&result[0], b[i], &A[i][0], &result[0], A.dim(1));
#endif
    return result;
}

template<typename Float1, typename Float2>
//MLDB_ALWAYS_INLINE
distribution<typename float_traits<Float1, Float2>::return_type>
multiply(const distribution<Float2> & b,
         const MLDB::MatrixRef<Float1, 2> & A)
{
    return multiply_r<typename float_traits<Float1, Float2>::return_type>(b, A);
}

template<typename Float1, typename Float2>
//MLDB_ALWAYS_INLINE
distribution<typename float_traits<Float1, Float2>::return_type>
operator * (const distribution<Float2> & b,
            const MLDB::MatrixRef<Float1, 2> & A)
{
    typedef typename float_traits<Float1, Float2>::return_type FloatR;
    return multiply_r<FloatR, Float1, Float2>(b, A);
}


/*****************************************************************************/
/* MATRIX MATRIX                                                             */
/*****************************************************************************/

template<typename FloatR, typename Float1, typename Float2>
MLDB::Matrix<FloatR, 2>
multiply_r(const MLDB::MatrixRef<Float1, 2> & A,
           const MLDB::MatrixRef<Float2, 2> & B)
{
    auto m = A.dim(0);
    auto x = A.dim(1);
    auto n = B.dim(1);

    if (x != B.dim(0))
        MLDB_MATRIX_THROW_INCOMPATIBLE_DIMENSIONS(A, B, "multiply");

    MLDB::Matrix<FloatR, 2> X(m, n);

    bool a_contiguous = A.is_contiguous();

    PossiblyDynamicBuffer<Float2> bentries_storage(A.dim(1));
    Float2 * bentries = bentries_storage.data();
    for (unsigned j = 0;  j < B.dim(1);  ++j) {
        for (unsigned k = 0;  k < A.dim(1);  ++k)
            bentries[k] = B[k][j];

        if (a_contiguous) {
            for (unsigned i = 0;  i < m;  ++i)
                X[i][j] = SIMD::vec_dotprod_dp(A[i].data(), bentries, x);
        }
        else {
            for (unsigned i = 0;  i < m;  ++i) {
                FloatR result{};
                for (unsigned k = 0;  k < x;  ++k)
                    result += A[i][k] * bentries[k];
                
                X[i][j] = result;
            }
        }
    }
    return X;
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
multiply(const MLDB::MatrixRef<Float1, 2> & A,
         const MLDB::MatrixRef<Float2, 2> & B)
{
    return multiply_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
operator * (const MLDB::MatrixRef<Float1, 2> & A,
            const MLDB::MatrixRef<Float2, 2> & B)
{
    return multiply_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

/*****************************************************************************/
/* MULTIPLY_TRANSPOSED                                                       */
/*****************************************************************************/

// Multiply A * transpose(A)
template<typename FloatR, typename Float>
MLDB::Matrix<FloatR, 2>
multiply_transposed(const MLDB::MatrixRef<Float, 2> & A)
{
    int As0 = A.dim(0);
    int As1 = A.dim(1);

    MLDB::Matrix<FloatR, 2> X(As0, As0);
    for (unsigned i = 0;  i < As0;  ++i) 
        for (unsigned j = 0;  j <= i;  ++j)
            X[i][j] = X[j][i] = SIMD::vec_dotprod_dp(&A[i][0], &A[j][0], As1);
    
    return X;
}

template<typename FloatR, typename Float1, typename Float2>
MLDB::Matrix<FloatR, 2>
multiply_transposed(const MLDB::MatrixRef<Float1, 2> & A,
                    const MLDB::MatrixRef<Float2, 2> & BT)
{
    int As0 = A.dim(0);
    int Bs0 = BT.dim(0);
    int As1 = A.dim(1);

    if (A.dim(1) != BT.dim(1))
        throw MLDB::Exception("Incompatible matrix sizes");

    MLDB::Matrix<FloatR, 2> X(As0, Bs0);
    for (unsigned j = 0;  j < Bs0;  ++j) {
        for (unsigned i = 0;  i < As0;  ++i)
            X[i][j] = SIMD::vec_dotprod_dp(&A[i][0], &BT[j][0], As1);
    }

    return X;
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
multiply_transposed(const MLDB::MatrixRef<Float1, 2> & A,
                    const MLDB::MatrixRef<Float2, 2> & B)
{
    // Special case for A * A^T
    if (&A == &B)
        return multiply_transposed<typename float_traits<Float1, Float2>::return_type>(A);

    return multiply_transposed
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

/*****************************************************************************/
/* MATRIX ADDITION                                                           */
/*****************************************************************************/

template<typename FloatR, typename Float1, typename Float2>
MLDB::Matrix<FloatR, 2>
add_r(const MLDB::MatrixRef<Float1, 2> & A,
           const MLDB::MatrixRef<Float2, 2> & B)
{
    if (A.dim(0) != B.dim(0)
        || A.dim(1) != B.dim(1))
        throw MLDB::Exception("Incompatible matrix sizes");
    
    MLDB::Matrix<FloatR, 2> X(A.dim(0), A.dim(1));

    for (unsigned i = 0;  i < A.dim(0);  ++i)
        SIMD::vec_add(&A[i][0], &B[i][0], &X[i][0], A.dim(1));

    return X;
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
add(const MLDB::MatrixRef<Float1, 2> & A,
    const MLDB::MatrixRef<Float2, 2> & B)
{
    return add_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
operator + (const MLDB::MatrixRef<Float1, 2> & A,
            const MLDB::MatrixRef<Float2, 2> & B)
{
    return add_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}


/*****************************************************************************/
/* MATRIX SUBTRACTION                                                        */
/*****************************************************************************/

template<typename FloatR, typename Float1, typename Float2>
MLDB::Matrix<FloatR, 2>
subtract_r(const MLDB::MatrixRef<Float1, 2> & A,
           const MLDB::MatrixRef<Float2, 2> & B)
{
    if (A.dim(0) != B.dim(0)
        || A.dim(1) != B.dim(1))
        throw MLDB::Exception("Incompatible matrix sizes");
    
    MLDB::Matrix<FloatR, 2> X(A.dim(0), A.dim(1));

    for (unsigned i = 0;  i < A.dim(0);  ++i)
        SIMD::vec_minus(&A[i][0], &B[i][0], &X[i][0], A.dim(1));
    
    return X;
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
subtract(const MLDB::MatrixRef<Float1, 2> & A,
    const MLDB::MatrixRef<Float2, 2> & B)
{
    return subtract_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
operator - (const MLDB::MatrixRef<Float1, 2> & A,
            const MLDB::MatrixRef<Float2, 2> & B)
{
    return subtract_r
        <typename float_traits<Float1, Float2>::return_type, Float1, Float2>
        (A, B);
}

/*****************************************************************************/
/* MATRIX SCALAR                                                             */
/*****************************************************************************/

inline 
MLDB::Matrix<double, 2>
operator * (const MLDB::MatrixRef<double, 2> & A,
            double B)
{
    int As0 = A.dim(0);
    int As1 = A.dim(1);
    MLDB::Matrix<double, 2> X(As0, As1);
    for (unsigned i = 0;  i < As0;  ++i) {
        for (unsigned j = 0;  j < As1;  ++j)
            X[i][j] = A[i][j] * B;
    }
    return X;
}

template<typename Float1, typename Float2>
MLDB::Matrix<typename float_traits<Float1, Float2>::return_type, 2>
operator / (const MLDB::MatrixRef<Float1, 2> & A,
            Float2 B)
{
    int As0 = A.dim(0);
    int As1 = A.dim(1);
    MLDB::Matrix<Float1, 2> X(As0, As1);
    for (unsigned i = 0;  i < As0;  ++i) {
        for (unsigned j = 0;  j < As1;  ++j)
            X[i][j] = A[i][j] / B;
    }
    return X;
}

/*****************************************************************************/
/* MATRIX COMPARISONS                                                        */
/*****************************************************************************/

template<typename Float1, typename Float2, size_t Dims>
bool operator == (const MLDB::MatrixRef<Float1, Dims> & A,
                  const MLDB::MatrixRef<Float2, Dims> & B)
{
    if (A.shape() != B.shape())
        return false;

    for (unsigned i = 0;  i < A.dim(0);  ++i)
        if (A[i] != B[i])
            return false;

    return true;
}

} // namespace MLDB
