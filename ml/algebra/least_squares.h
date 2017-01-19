/* least_squares.h                                                 -*- C++ -*-
   Jeremy Barnes, 15 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Least squares solution.
*/

#pragma once

#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/arch/exception.h"
#include "mldb/jml/stats/distribution_simd.h"
#include "mldb/arch/timers.h"
#include <iostream>
#include "mldb/jml/db/persistent.h"
#include "mldb/jml/utils/enum_info.h"
#include "matrix_ops.h"

namespace ML {

/*****************************************************************************/
/* LEAST_SQUARES                                                             */
/*****************************************************************************/

/** Solves an equality constrained least squares problem.  This calculates
    the vector x that minimises ||c - Ax||2 such that Bx = d holds.

    \param A                     a (m x n) matrix
    \param c                     a n element vector
    \param B                     a (p x n) matrix
    \param d                     a p element vector

    Note that this routine requires p <n n <= m + p.

    It uses the LAPACK routine xGGLSE to perform the dirty work.
*/
distribution<float>
least_squares(const boost::multi_array<float, 2> & A,
              const distribution<float> & c,
              const boost::multi_array<float, 2> & B,
              const distribution<float> & d);

distribution<double>
least_squares(const boost::multi_array<double, 2> & A,
              const distribution<double> & c,
              const boost::multi_array<double, 2> & B,
              const distribution<double> & d);

//extern __thread std::ostream * debug_irls;

/** Solve a least squares linear problem.
    This solves the linear least squares problem
    \f[
        A\mathbf{x} = \mathbf{b}
    \f]
 
    for the parameter <bf>x</bf>.

    \param    A the coefficient matrix
    \param    b the required output vector
    \returns  x

    \pre      A.shape()[0] == b.size()

    This <em>should</em> work for any shape of A, but has only been verified
    for A square.  It uses a SVD internally to do its work; this is not the
    best way for a square system but is general enough to handle the other
    cases.
 */
distribution<float>
least_squares(const boost::multi_array<float, 2> & A,
              const distribution<float> & b);

distribution<double>
least_squares(const boost::multi_array<double, 2> & A,
              const distribution<double> & b);


/* Returns U * diag(d) * V */
boost::multi_array<float, 2>
diag_mult(const boost::multi_array<float, 2> & U,
          const distribution<float> & d,
          const boost::multi_array<float, 2> & V,
          bool parallel);

boost::multi_array<double, 2>
diag_mult(const boost::multi_array<double, 2> & U,
          const distribution<double> & d,
          const boost::multi_array<double, 2> & V,
          bool parallel);

/** Solve a singular value decomposition problem over a symmetric
    square matrix (probably another matrix multiplied by its transpose).

    NOTE: the X array will be *overwritten* by this call.
*/

void svd_square(boost::multi_array<float, 2> & X,
                boost::multi_array<float, 2> & VT,
                boost::multi_array<float, 2> & U,
                distribution<float> & svalues);

void svd_square(boost::multi_array<double, 2> & X,
                boost::multi_array<double, 2> & VT,
                boost::multi_array<double, 2> & U,
                distribution<double> & svalues);


/** Solve a least squares linear problem using ridge regression.

    This solves the linear least squares problem
    \f[
        A\mathbf{x} = \mathbf{b}
    \f]
 
    for the parameter <bf>x</bf>, subject to ridge regression with the
    value of lambda.

    \param    A the coefficient matrix
    \param    b the required output vector
    \param    lambda regularization parameter
    \returns  x

    \pre      A.shape()[0] == b.size()
 */
distribution<float>
ridge_regression(const boost::multi_array<float, 2> & A,
                 const distribution<float> & b,
                 float lambda);

distribution<double>
ridge_regression(const boost::multi_array<double, 2> & A,
                 const distribution<double> & b,
                 float lambda);

/** Solve a least squares linear problem using LASSO regression
    (LSE + L1 cost on the weigths) using coordinate descent.

    \param    A the coefficient matrix
    \param    b the required output vector
    \param    lambda regularization parameter
    \returns  x

    \pre      A.shape()[0] == b.size()
 */
distribution<float>
lasso_regression(const boost::multi_array<float, 2> & A,
                 const distribution<float> & b,
                 float lambda,
                 int maxIter = 1000,
                 float epsilon = 1e-4);

distribution<double>
lasso_regression(const boost::multi_array<double, 2> & A,
                 const distribution<double> & b,
                 float lambda,
                 int maxIter = 1000,
                 float epsilon = 1e-4);


/*****************************************************************************/
/* IRLS                                                                      */
/*****************************************************************************/

/** Multiply two matrices with a diagonal matrix.
    \f[
        X W X^T
    \f]

    \f[
        W = \left[
            \begin{array}{cccc}
              d_0  &    0   & \cdots &    0   \\
               0   &   d_1  & \cdots &    0   \\
            \vdots & \vdots & \ddots & \vdots \\
               0   &    0   &    0   &   d_n 
            \end{array}
            \right]
    \f]
 */

boost::multi_array<float, 2>
weighted_square(const boost::multi_array<float, 2> & XT,
                const distribution<float> & d);

boost::multi_array<double, 2>
weighted_square(const boost::multi_array<double, 2> & XT,
                const distribution<double> & d);


template<class Float>
boost::multi_array<Float, 2>
diag_mult2(const boost::multi_array<Float, 2> & X,
           const distribution<Float> & d)
{
    if (X.shape()[1] != d.size())
        throw Exception("Incompatible matrix sizes");

    size_t nx = X.shape()[1];
    size_t nv = X.shape()[0];

    boost::multi_array<Float, 2> Y(boost::extents[nv][nx]);

    for (unsigned i = 0;  i < nv;  ++i) {
        for (unsigned j = 0;  j < nx;  ++j) {
            Y[i][j] = X[i][j] * d[j];
        }
    }

    return Y;
}

/** Multiply a vector and matrix by a diagonal matrix.
    \f[
        X W \mathbf{y}
    \f]

    where

    \f[
        W = \left[
            \begin{array}{cccc}
              d_0  &    0   & \cdots &    0   \\
               0   &   d_1  & \cdots &    0   \\
            \vdots & \vdots & \ddots & \vdots \\
               0   &    0   &    0   &   d_n 
            \end{array}
            \right]
    \f]
*/
template<class Float>
distribution<Float>
diag_mult(const boost::multi_array<Float, 2> & X,
          const distribution<Float> & d,
          const distribution<Float> & y)
{
    size_t nx = X.shape()[1];
    if (nx != d.size() || nx != y.size())
        throw Exception("Incompatible matrix sizes");
    size_t nv = X.shape()[0];

    distribution<Float> result(nv, 0.0);
    for (unsigned v = 0;  v < nv;  ++v)
        for (unsigned x = 0;  x < nx;  ++x)
            result[v] += X[v][x] * d[x] * y[x];

    return result;
}

/** Iteratively reweighted least squares.  Allows a non-linear transformation
    (given by the link parameter) of a linear combination of features to be
    fitted in a least-squares fashion.  The dist parameter gives the
    distribution of the errors.
    
    \param y      the values to fit (target values)
    \param x      the matrix of values to fit with.  It should be nv x nx,
                  where nv is the number of variables to fit (and will be
                  the length of the output \b), and nx is the number of
                  examples (and is also the length of y).
    \param w      the relative weight (importance) of each example.  Is
                  normalized before use.  If unknown, pass a uniform
                  distribution.
    \param m      the number of observations for the binomial distribution.  If
                  the binomial distribution is not used, or the y values are
                  already proportions, then set all of the values to 1.
    \param link   the link function (see those above)
    \param dist   the error distribution function (see those above)

    \returns      the fitted parameters \p b, one for each column in x

    \pre          y.size() == w.size() == x.shape()[1]
    \post         b.size() == x.shape()[0]
*/

template<class Link, class Dist, class Float, class Regressor>
distribution<Float>
irls(const distribution<Float> & y, const boost::multi_array<Float, 2> & x,
     const distribution<Float> & w, 
     const Link & link, 
     const Dist & dist,
     const Regressor & regressor)
{
    using namespace std;

    bool debug = false;

    typedef distribution<Float> Vector;

    static const int max_iter = 20;           // from GLMlab
    static const float tolerence = 5e-5;      // from GLMlab
    
    size_t nv = x.shape()[0];                     // number of variables
    size_t nx = x.shape()[1];                     // number of examples

    if (y.size() != nx || w.size() != nx)
        throw Exception("incompatible data sizes");

    int iter = 0;
    Float rdev = std::sqrt((y * y).total());  // residual deviance
    Float rdev2 = 0;                          // last residual deviance
    Vector mu = (y + 0.5) / 2;                // link input
    Vector b(nv, 0.0);                        // ls fit parameters
    Vector b2;                                // last ls fit parameters
    Vector eta = link.forward(mu);            // link output
    Vector offset(nx, 0.0);                   // known values (??)
    distribution<Float> weights = w;          // sample weights

    for (unsigned i = 0;  i < mu.size();  ++i)
        if (!std::isfinite(mu[i]))
            throw Exception(format("mu[%d] = %f", i, mu[i]));

    Timer t(debug);
    
    auto doneStep = [&] (const std::string & step)
        {
            if (!debug)
                return;
            cerr << "  " << step << ": " << t.elapsed() << endl;
            t.restart();
        };
    
    /* Note: look in the irls.m function of GLMlab to see what we are trying
       to do here.  This is essentially a C++ reimplementation of that
       function.  I don't really know what it is doing. */
    while (fabs(rdev - rdev2) > tolerence && iter < max_iter) {
        Timer t(debug);

        /* Find the new weights for this iteration. */
        Vector deta_dmu    = link.diff(mu);
        for (unsigned i = 0;  i < deta_dmu.size();  ++i)
            if (!std::isfinite(deta_dmu[i]))
                throw Exception(format("deta_dmu[%d] = %f", i, deta_dmu[i]));

        //if (debug_irls)
        //    (*debug_irls) << "deta_demu: " << deta_dmu << endl;

        doneStep("diff");

        Vector var         = dist.variance(mu);
        for (unsigned i = 0;  i < var.size();  ++i)
            if (!std::isfinite(var[i]))
                throw Exception(format("var[%d] = %f", i, var[i]));

        //if (debug_irls)
        //    (*debug_irls) << "var: " << deta_dmu << endl;

        doneStep("variance");

        Vector fit_weights = weights / (deta_dmu * deta_dmu * var);
        for (unsigned i = 0;  i < fit_weights.size();  ++i) {
            if (!std::isfinite(fit_weights[i])) {
                cerr << "weigths = " << weights[i]
                     << "  deta_dmu = " << deta_dmu[i]
                     << "  var = " << var[i] << endl;
                throw Exception(format("fit_weights[%d] = %f", i,
                                       fit_weights[i]));
            }
        }

        //if (debug_irls)
        //    (*debug_irls) << "fit_weights: " << fit_weights << endl;
        
        doneStep("fit_weights");
        
        //cerr << "fit_weights = " << fit_weights << endl;

        /* Set up the reweighted least squares problem. */
        Vector z           = eta - offset + (y - mu) * deta_dmu;

        // Instead of explicitly scaling here, we calculate the fit weights
        //Matrix xTwx        = diag_mult(x, fit_weights);
        //doneStep("xTwx");
        Vector xTwz        = diag_mult(x, fit_weights, z);
        doneStep("xTwz");

        //if (debug_irls)
        //    (*debug_irls) << "z: " << z << endl
        //                  << "xTwx: " << xTwx << endl
        //                  << "xTwz: " << xTwz << endl;

        /* Solve the reweighted problem using a linear least squares. */
        b2                 = b;
        b                  = regressor.calc_scaled(x, fit_weights, xTwz);
        
        //if (debug_irls)
        //    (*debug_irls) << "b: " << b << endl;

        doneStep("least squares");

        /* Re-estimate eta and mu based on refined estimate. */
        //cerr << "b.size() = " << b.size() << endl;
        //cerr << "x.shape()[0] = " << x.shape()[0]
        //     << " x.shape()[1] = " << x.shape()[1]
        //     << endl;

        eta                = (b * x) + offset;
        for (unsigned i = 0;  i < eta.size();  ++i)
            if (!std::isfinite(eta[i]))
                throw Exception(format("eta[%d] = %f", i, eta[i]));

        //if (debug_irls)
        //    (*debug_irls) << "eta: " << eta << endl;

        doneStep("eta");

        mu                 = link.inverse(eta);
        for (unsigned i = 0;  i < mu.size();  ++i)
            if (!std::isfinite(mu[i]))
                throw Exception(format("mu[%d] = %f", i, mu[i]));

        //if (debug_irls)
        //    (*debug_irls) << "me: " << mu << endl;

        doneStep("mu");

        /* Recalculate the residual deviance, and save the last one to check
           for convergence. */
        rdev2              = rdev;
        rdev               = dist.deviance(y, mu, weights);

        doneStep("deviance");

        ++iter;

        if (debug)
            cerr << "iter " << iter << ": " << t.elapsed() << endl;

        //if (debug_irls) {
        //    *debug_irls << "iter " << iter << " rdev " << rdev
        //                << " rdev2 " << rdev2 << " diff " << abs(rdev - rdev2)
        //                << " tolerance " << tolerence << endl;
        //}
    }

    return b;
}


} // namespace ML
