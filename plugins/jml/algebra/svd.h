// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* svd.h                                                           -*- C++ -*-
   Jeremy Barnes, 15 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.
   $Source$

   Singular value decompositions.
*/

#pragma once

#include "mldb/utils/distribution.h"
#include "mldb/plugins/jml/algebra/matrix.h"



namespace MLDB {

/*****************************************************************************/
/* SVD                                                                       */
/*****************************************************************************/

/** Calculates the "economy size" SVD of the matrix A, including the left and
    right singular vectors.  The output has the relationship

    \f[
        A = U\Sigma V^T
    \f]
    
    where \f$\Sigma\f$ is a diagonal matrix of the singular values:

    \f[
        \Sigma = \left[
            \begin{array}{cccc}
              \sigma_0  &    0   & \cdots &    0   \\
               0   &   \sigma_1  & \cdots &    0   \\
            \vdots & \vdots & \ddots & \vdots \\
               0   &    0   &    0   &   \sigma_n 
            \end{array}
            \right]
    \f]

    Apropos:

    \code
    MLDB::MatrixRef<float, 2> A;

    ...

    distribution<float> E;
    MLDB::MatrixRef<float, 2> U, V;
    std::tie(E, U, V) = svd(A);
    \endcode

    \param A        the matrix of which to take the SVD

    \returns E      a vector of the singular values, in order from the
                    highest value to the lowest value
    \returns U      the left-singular vectors
    \returns V      the right-singular vectors.

    \pre            A has full rank
    \post           \f$A = U \Sigma V^T\f$

    Again, a wrapper around ARPACK.  Note that the ARPACK++ manual gives
    pretty good instructions on how to do all of this, although it neglects
    to mention that U and V need to be multiplied by \f$\sqrt{2}\f$!
*/
std::tuple<distribution<float>, MLDB::MatrixRef<float, 2>,
             MLDB::MatrixRef<float, 2> >
svd(const MLDB::MatrixRef<float, 2> & A);

std::tuple<distribution<double>, MLDB::MatrixRef<double, 2>,
             MLDB::MatrixRef<double, 2> >
svd(const MLDB::MatrixRef<double, 2> & A);

/** Same as above, but calculates only the first \p n singular values.
 */
std::tuple<distribution<float>, MLDB::MatrixRef<float, 2>,
             MLDB::MatrixRef<float, 2> >
svd(const MLDB::MatrixRef<float, 2> & A, size_t n);

std::tuple<distribution<double>, MLDB::MatrixRef<double, 2>,
             MLDB::MatrixRef<double, 2> >
svd(const MLDB::MatrixRef<double, 2> & A, size_t n);

} // namespace MLDB
