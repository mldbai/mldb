// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* least_squares.cc
   Jeremy Barnes, 15 June 2003
   Copyright (c) 2003 Jeremy Barnes.  All rights reserved.

   Least squares solution of problems.  Various versions.
*/

#include "least_squares.h"


#include "mldb/ml/algebra/matrix_ops.h"
#include "mldb/base/exc_assert.h"
#include "svd.h"
#include <boost/timer.hpp>
#include "mldb/arch/timers.h"
#include "lapack.h"
#include <cmath>
#include "mldb/jml/utils/string_functions.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/base/parallel.h"


using namespace std;


namespace ML {

#if 0
template<class Float>
distribution<Float>
least_squares_impl(const boost::multi_array<Float, 2> & A,
                   const distribution<Float> & c,
                   const boost::multi_array<Float, 2> & B,
                   const distribution<Float> & d)
{
    using namespace LAPack;

    size_t m = A.shape()[0];
    size_t n = A.shape()[1];
    size_t p = B.shape()[0];

    //cerr << "A: (mxn) " << A.shape()[0] << " x " << A.shape()[1] << endl;
    //cerr << "B: (pxn) " << B.shape()[0] << " x " << B.shape()[1] << endl;
    //cerr << "c: m     " << c.size() << endl;
    //cerr << "d: p     " << d.size() << endl;

    if (c.size() != m || B.shape()[1] != n || d.size() != p)
        throw Exception("least_squares: sizes didn't match");

    if (p > n || n > (m + p))
        throw Exception("least_squares: overconstrained system");

    // todo: check that B has full row rank p and that the matrix (A B)' has
    // full column rank n.

    distribution<Float> result(n);

    /* We need to transpose them for Fortran, but since they are destroyed
       anyway it's no big deal since they would have been copied. */
    boost::multi_array<Float, 2> AF = fortran(A);
    boost::multi_array<Float, 2> BF = fortran(B);
    distribution<Float> c2 = c;
    distribution<Float> d2 = d;

    int res = gglse(m, n, p,
                    AF.data_begin(), AF.shape()[0],
                    BF.data_begin(), BF.shape()[1],
                    &c2[0], &d2[0], &result[0]);

    if (res != 0)
        throw Exception(format("least_squares(): gglse returned error in arg "
                               "%d", res));

    return result;
}

distribution<float>
least_squares(const boost::multi_array<float, 2> & A,
              const distribution<float> & c,
              const boost::multi_array<float, 2> & B,
              const distribution<float> & d)
{
    return least_squares_impl(A, c, B, d);
}

distribution<double>
least_squares(const boost::multi_array<double, 2> & A,
              const distribution<double> & c,
              const boost::multi_array<double, 2> & B,
              const distribution<double> & d)
{
    return least_squares_impl(A, c, B, d);
}
#endif

template<class Float>
distribution<Float>
least_squares_impl(const boost::multi_array<Float, 2> & A, const distribution<Float> & b)
{
    using namespace std;

    //boost::timer t;

    if (A.shape()[0] != b.size()) {
        cerr << "A.shape()[0] = " << A.shape()[0] << endl;
        cerr << "A.shape()[1] = " << A.shape()[1] << endl;
        cerr << "b.size() = " << b.size() << endl;
        throw Exception("incompatible dimensions for least_squares");
    }

    using namespace LAPack;
    
    int m = A.shape()[0];
    int n = A.shape()[1];

    distribution<Float> x = b;
    x.resize(std::max<size_t>(m, n));

    boost::multi_array<Float, 2> A2 = A;

#if 0
    using namespace std;
    cerr << "m = " << m << " n = " << n << " A2.shape()[0] = " << A2.shape()[0]
         << " A2.shape()[1] = " << A2.shape()[1] << endl;
    cerr << "A2 = " << endl << A2 << endl;
    cerr << "b = " << b << endl;
#endif
    int res = gels('T', n, m, 1, A2.data(), n, &x[0],
                   x.size());

    if (res < 0)
        throw Exception(format("least_squares(): gels returned error in arg "
                               "%d", -res));

    //if (debug_irls) {
    //    (*debug_irls) << "gels returned " << res << endl;
    //    (*debug_irls) << "x = " << x << endl;
    //}

    if (res > 0) {
        //if (debug_irls)
        //      (*debug_irls) << "retrying; " << res << " are too small" << endl;
    
        /* Rank-deficient matrix.  Use the more efficient routine. */
        int rank;
        Float rcond = -1.0;
        Float sv[std::min(m, n)];
        std::fill(sv, sv + std::min(m, n), 0.0);

        // Rebuild A2, transposed this time
        A2.resize(boost::extents[n][m]);
        A2 = transpose(A);

        // Rebuild x as it was previously overwritten
        x = b;
        x.resize(std::max<size_t>(m, n));

        res = gelsd(m, n, 1, A2.data(), m, &x[0], x.size(), sv, rcond, rank);

        //if (debug_irls)
        //    (*debug_irls) << "rcond: " << rcond << " rank: "
        //                  << rank << endl;
    }

    if (res < 0) {
        throw Exception(format("least_squares(): gelsy returned error in arg "
                               "%d", -res));
    }

    x.resize(n);
 
    //using namespace std;
    //cerr << "least_squares: took " << t.elapsed() << "s" << endl;
    
    return x;
    //cerr << "least squares: gels returned " << x2 << endl;
    //cerr << "least squares: A2 = " << endl << A2 << endl;

    //cerr << "least_squares: " << t.elapsed() << "s" << endl;
    //distribution<Float> x3
    //    = least_squares(A, b, boost::multi_array<Float, 2>(0, n), distribution<Float>());
    
    //cerr << "least squares: gglse returned " << x3 << endl;

}

distribution<float>
least_squares(const boost::multi_array<float, 2> & A,
              const distribution<float> & b)
{
    return least_squares_impl(A, b);
}

distribution<double>
least_squares(const boost::multi_array<double, 2> & A,
              const distribution<double> & b)
{
    return least_squares_impl(A, b);
}

template<typename Float>
void doDiagMultColumn(const boost::multi_array<Float, 2> & U,
                      const distribution<Float> & d,
                      const boost::multi_array<Float, 2> & V,
                      boost::multi_array<Float, 2> & result,
                      int j)
{
    size_t m = U.shape()[0], x = d.size();

    Float Vj_values[x];
    for (unsigned k = 0;  k < x;  ++k) {
        Vj_values[k] = V[k][j];
    }
    for (unsigned i = 0;  i < m;  ++i) {
        result[i][j] = ML::SIMD::vec_accum_prod3(&U[i][0], &d[0], Vj_values, x);
    }
}

template<class Float>
boost::multi_array<Float, 2>
diag_mult_impl(const boost::multi_array<Float, 2> & U,
               const distribution<Float> & d,
               const boost::multi_array<Float, 2> & V,
               bool parallel)
{
    size_t m = U.shape()[0], n = V.shape()[1], x = d.size();

    boost::multi_array<Float, 2> result(boost::extents[m][n]);
    
    if (U.shape()[1] != x || V.shape()[0] != x)
        throw Exception("diag_mult(): wrong shape");

    auto doColumn = std::bind(&doDiagMultColumn<Float>, 
                              std::cref(U),
                              std::cref(d),
                              std::cref(V),
                              std::ref(result),
                              std::placeholders::_1);

    if (parallel)
        MLDB::parallelMap(0, n, doColumn);
    else {
        for (unsigned j = 0;  j < n;  ++j)
            doColumn(j);
    }
    
    return result;
}

boost::multi_array<float, 2>
diag_mult(const boost::multi_array<float, 2> & U,
          const distribution<float> & d,
          const boost::multi_array<float, 2> & V,
          bool parallel)
{
    return diag_mult_impl<float>(U, d, V, parallel);
}

boost::multi_array<double, 2>
diag_mult(const boost::multi_array<double, 2> & U,
          const distribution<double> & d,
          const boost::multi_array<double, 2> & V,
          bool parallel)
{
    return diag_mult_impl<double>(U, d, V, parallel);
}

template<typename Float>
struct RidgeRegressionIteration {
    double lambda;
    double current_lambda;
    double total_mse_unbiased;
    distribution<Float> x;

    void run(const distribution<Float> & singular_values,
             const boost::multi_array<Float, 2> & A,
             const distribution<Float> & b,
             const boost::multi_array<Float, 2> & VT,
             const boost::multi_array<Float, 2> & U,
             const boost::multi_array<Float, 2> & GK,
             bool debug)
    {
        int m = A.shape()[0];
        int n = A.shape()[1];

        Timer t(debug);

        auto doneStep = [&] (const std::string & where)
        {
            if (!debug)
                return;
            cerr << "      " << where << ": " << t.elapsed() << endl;
            t.restart();
        };

        //cerr << "i = " << i << " current_lambda = " << current_lambda << endl;
        // Adjust the singular values for the new lambda
        distribution<Float> my_singular = singular_values;
        if (current_lambda != lambda)
            my_singular += (current_lambda - lambda);

        //boost::multi_array<Float, 2> GK_pinv
        //    = U * diag((Float)1.0 / my_singular) * VT;

        boost::multi_array<Float, 2> GK_pinv
            = diag_mult(U, (Float)1.0 / my_singular, VT, true /* parallel */);

        doneStep("diag_mult");

        // TODO: reduce GK by removing those basis vectors where the singular
        // values are too close to lambda
    
        if (debug && false) {
            cerr << "GK_pinv = " << endl << GK_pinv
                 << endl;
            cerr << "prod = " << endl << (GK * GK_pinv * GK) << endl;
            cerr << "prod2 = " << endl << (GK_pinv * GK * GK) << endl;
        }

        boost::multi_array<Float, 2> A_pinv
            = (m < n ? GK_pinv * A : A * GK_pinv);

        doneStep("A_pinv");

        if (debug && false)
            cerr << "A_pinv = " << endl << A_pinv << endl;

        x = b * A_pinv;
    
        if (debug)
            cerr << "x = " << x << endl;

        distribution<Float> predictions = A * x;

        //cerr << "A: " << A.shape()[0] << "x" << A.shape()[1] << endl;
        //cerr << "A_pinv: " << A_pinv.shape()[0] << "x" << A_pinv.shape()[1]
        //     << endl;

        //boost::multi_array<Float, 2> A_A_pinv
        //    = A * transpose(A_pinv);

        doneStep("predictions");

#if 0
        boost::multi_array<Float, 2> A_A_pinv
        = multiply_transposed(A, A_pinv);

        cerr << "A_A_pinv: " << A_A_pinv.shape()[0] << "x"
        << A_A_pinv.shape()[1] << " m = " << m << endl;

        if (debug && false)
            cerr << "A_A_pinv = " << endl << A_A_pinv << endl;
#else
        // We only need the diagonal of A * A_pinv

        distribution<Float> A_A_pinv_diag(m);
        for (unsigned j = 0;  j < m;  ++j)
            A_A_pinv_diag[j] = SIMD::vec_dotprod_dp(&A[j][0], &A_pinv[j][0], n);
#endif

        doneStep("A_A_pinv");

        // Now figure out the performance
        double total_mse_biased = 0.0;
        for (unsigned j = 0;  j < m;  ++j) {

            if (j < 10 && false)
                cerr << "j = " << j << " b[j] = " << b[j]
                     << " predictions[j] = " << predictions[j]
                     << endl;

            double resid = b[j] - predictions[j];

            // Adjust for the bias cause by training on this example.  This is
            // A * pinv(A), which is A * 

            double factor = 1.0 - A_A_pinv_diag[j];

            double resid_unbiased = resid / factor;

            total_mse_biased += (1.0 / m) * resid * resid;
            total_mse_unbiased += (1.0 / m) * resid_unbiased * resid_unbiased;
        }

        doneStep("mse");

        //cerr << "lambda " << current_lambda
        //     << " rmse_biased = " << sqrt(total_mse_biased)
        //     << " rmse_unbiased = " << sqrt(total_mse_unbiased)
        //     << endl;

        //if (sqrt(total_mse_biased) > 1.0) {
        //    cerr << "rmse_biased: x = " << x << endl;
        //}
    
#if 0
        cerr << "m = " << m << endl;
        cerr << "total_mse_biased   = " << total_mse_biased << endl;
        cerr << "total_mse_unbiased = " << total_mse_unbiased << endl;
        cerr << "best_error = " << best_error << endl;
        cerr << "x = " << x << endl;
#endif
    };

};

// NOTE: this cumbersome construction is to keep clang 3.4 from
// ICEing
template<typename Float>
struct RidgeRegressionIterations: public std::vector<RidgeRegressionIteration<Float> > {
    void run(const distribution<Float> & singular_values,
             const boost::multi_array<Float, 2> & A,
             const distribution<Float> & b,
             const boost::multi_array<Float, 2> & VT,
             const boost::multi_array<Float, 2> & U,
             const boost::multi_array<Float, 2> & GK,
             bool debug,
             int n)
    {
        this->at(n).run(singular_values, A, b, VT, U, GK, debug);
    }
};

template<class Float>
distribution<Float>
ridge_regression_impl(const boost::multi_array<Float, 2> & A,
                      const distribution<Float> & b,
                      float& lambda)
{
    using namespace std;
    float initialLambda = lambda < 0 ? 1e-5 : lambda;
    //cerr << "ridge_regression: A = " << A.shape()[0] << "x" << A.shape()[1]
    //     << " b = " << b.size() << endl;

    //cerr << "b = " << b << endl;
    //cerr << "A = " << A << endl;

    bool debug = false;
    //debug = true;

    Timer t(debug);

    auto doneStep = [&] (const std::string & where)
        {
            if (!debug)
                return;
            cerr << where << ": " << t.elapsed() << endl;
            t.restart();
        };

    // Step 1: SVD

    if (A.shape()[0] != b.size())
        throw Exception("incompatible dimensions for least_squares");

    using namespace LAPack;
    
    int m = A.shape()[0];
    int n = A.shape()[1];

    int minmn = std::min(m, n);

    // See http://www.clopinet.com/isabelle/Projects/ETH/KernelRidge.pdf

    // The matrix to decompose is square
    boost::multi_array<Float, 2> GK(boost::extents[minmn][minmn]);

    
    //cerr << "m = " << m << " n = " << n << endl;

    
    // Take either A * transpose(A) or (A transpose) * A, whichever is smaller
    if (m < n) {
        for (unsigned i1 = 0;  i1 < m;  ++i1)
            for (unsigned i2 = 0;  i2 < m;  ++i2)
                GK[i1][i2] = SIMD::vec_dotprod_dp(&A[i1][0], &A[i2][0], n);

        //for (unsigned i1 = 0;  i1 < m;  ++i1)
        //    for (unsigned i2 = 0;  i2 < m;  ++i2)
        //        for (unsigned j = 0;  j < n;  ++j)
        //            GK[i1][i2] += A[i1][j] * A[i2][j];
    } else {
        // TODO: vectorize and look at loop order
        for (unsigned i = 0;  i < m;  ++i)
            for (unsigned j1 = 0;  j1 < n;  ++j1)
                for (unsigned j2 = 0;  j2 < n;  ++j2)
                    GK[j1][j2] += A[i][j1] * A[i][j2];
    }

    doneStep("    square");

    if (debug)
        cerr << "GK = " << endl << GK << endl;

    //cerr << "GK.shape()[0] = " << GK.shape()[0] << endl;
    //cerr << "GK.shape()[1] = " << GK.shape()[1] << endl;

    // Add in the ridge
    for (unsigned i = 0;  i < minmn;  ++i)
        GK[i][i] += initialLambda;

    if (debug)
        cerr << "GK with ridge = " << endl << GK << endl;

    // Decompose to get the pseudoinverse
    distribution<Float> svalues(minmn);
    boost::multi_array<Float, 2> VT(boost::extents[minmn][minmn]);
    boost::multi_array<Float, 2> U(boost::extents[minmn][minmn]);
    
    svd_square(GK, VT, U, svalues);

    distribution<Float> singular_values
        (svalues.begin(), svalues.begin() + minmn);

    if (debug)
        cerr << "singular values = " << singular_values << endl;

    if (debug) {
        // Multiply decomposition back to make sure that we get the original
        // matrix
        boost::multi_array<Float, 2> D = diag(singular_values);

        boost::multi_array<Float, 2> GK_test
            = U * D * VT;

        cerr << "GK_test = " << endl << GK_test << endl;
        //cerr << "errors = " << endl << (GK_test - GK) << endl;
    }

    // Figure out the optimal value of lambda based upon leave-one-out cross
    // validation
    distribution<Float> x_best;
    Float best_lambda = initialLambda;
    typedef RidgeRegressionIteration<Float> Iteration;
    if (lambda < 0) {
        double current_lambda = 10.0;

        RidgeRegressionIterations<Float> iterations;

        for (; current_lambda >= 1e-14; current_lambda /= 10.0) {
            Iteration iter;
            iter.lambda = initialLambda;
            iter.current_lambda = current_lambda;
            iterations.push_back(iter);
        };

        MLDB::parallelMap(0, iterations.size(),
                        std::bind(std::mem_fn(&RidgeRegressionIterations<Float>::run),
                                  std::ref(iterations),
                                  std::cref(singular_values),
                                  std::cref(A), std::cref(b),
                                  std::cref(VT), std::cref(U),
                                  std::cref(GK), debug,
                                  std::placeholders::_1));

        //double best_lambda = -1000;
        double best_error = 1000000;

        for (unsigned i = 0;  i < iterations.size();  ++i) {

            if (iterations[i].total_mse_unbiased < best_error || i == 0) {

                //cerr << "best_lambda " << iterations[i].current_lambda << " error : " << iterations[i].total_mse_unbiased << endl;

                x_best = iterations[i].x;
                best_lambda = iterations[i].current_lambda;
                best_error = iterations[i].total_mse_unbiased;
            }
        } 
    }
    else {
        Iteration iter;
        iter.lambda = initialLambda;
        iter.current_lambda = initialLambda;
        iter.run(singular_values, A, b, VT, U, GK, debug);
        x_best = iter.x;
    }

    doneStep("    lambda");

    //cerr << "total: " << t.elapsed() << endl;

    lambda = best_lambda; //return the lambda we ended up using
    return x_best;
}

distribution<float>
ridge_regression(const boost::multi_array<float, 2> & A,
                 const distribution<float> & b,
                 float lambda)
{
    return ridge_regression_impl(A, b, lambda);
}

distribution<double>
ridge_regression(const boost::multi_array<double, 2> & A,
                 const distribution<double> & b,
                 float lambda)
{
    return ridge_regression_impl(A, b, lambda);
}

template<class Float>
distribution<Float>
lasso_regression_impl(const boost::multi_array<Float, 2> & A,
                      const distribution<Float> & b,
                      float lambda,
                      int maxIter,
                      float epsilon)
{ 
    // cerr << "lasso_regression: A = " << A.shape()[0] << "x" << A.shape()[1]
    //     << " b = " << b.size() << " lambda = " << lambda <<" maxIter = "
    //     << maxIter << " epsilon = " << epsilon << endl;

    //  ref: https://www.coursera.org/learn/ml-regression/lecture/AsCvQ/coordinate-descent-for-lasso-unnormalized-features
    //
    //  on why standardisation might be required before using LASSO:
    //  http://stats.stackexchange.com/q/86434/22296

    int n = A.shape()[0];   //Number of samples
    int p = A.shape()[1];   //Number of variables

     distribution<Float> x(p, 0.); //our solution vector

    if (lambda <= 0) {
        x = ridge_regression_impl(A, b, lambda); //Use the ridge regression to determine lambda and use the result as initialization
    }

    Float halflambda = lambda / 2.0f;
    distribution<Float> Atb(p, 0.);                        //Correlation of each variable with the target vector
    boost::multi_array<Float, 2> AtA(boost::extents[p][p]); //Correlation betwen each variables

    //Precompute Atb and AtA
    for (int j = 0; j < p; ++j) {
        for (int i = 0; i < n; ++i) {
            Atb[j] += A[i][j] * b[i];
        }
        //each column dot each column
        for (int i = 0; i < p; ++i) { //todo: optimise for triangular matrix
            Float dotprod = 0.;
            for (int r = 0; r < n; ++r) {
                dotprod += A[r][i] * A[r][j];
            }
            AtA[j][i] = dotprod;
        }
    }

    //Main lasso loop, which is really a coordinate descent where we optimize each variable independently.
    //We do this in a roundrobin fashion.
    int iter = 0;
    do {
        Float max_step = 0.;
        distribution<Float> oldX(p);
        oldX = x;

        for (int j = 0; j < p; ++j) { //for each column / variable

            Float rho = Atb[j];

            for (int i = 0; i < p; ++i){    //scales with the number of variables
                if (i != j)
                    rho -= AtA[j][i] * x[i];
            }

            if (rho > halflambda) {
                x[j] = (rho - halflambda) / (AtA[j][j]);
            }
            else if (rho < -halflambda) {
                x[j] = (rho + halflambda) / (AtA[j][j]);
            }
            else {
                x[j] = 0;
            }

	        //cerr << "x[j]: " << x[j] << endl; 
            Float step = fabs(x[j] - oldX[j]);
            if (step > max_step)
                max_step = step;
        }

        // cerr << "max_step: " << max_step << endl;

        // if the biggest step we took was smaller than our threshold, let's
        // stop there and assume we have converged
        if (max_step < epsilon) {
            break;
        }
        if (iter >= maxIter) {
            // if we stopped after max iteration and not because we have
            // converge, let's issue a warning
            cerr << MLDB::format("LASSO did not converge in %i iterations, last "
                               " max_step > eps (%f > %f)",
                               iter, max_step, epsilon);
            break;
        }
        iter++;
    } while (true); //until convergence

    return x;
}

distribution<float>
lasso_regression(const boost::multi_array<float, 2> & A,
                 const distribution<float> & b,
                 float lambda,
                 int maxIter,
                 float epsilon)
{
    return lasso_regression_impl(A, b, lambda, maxIter, epsilon);
}

distribution<double>
lasso_regression(const boost::multi_array<double, 2> & A,
                 const distribution<double> & b,
                 float lambda,
                 int maxIter,
                 float epsilon)
{
    return lasso_regression_impl(A, b, lambda, maxIter, epsilon);
}

//***********************************************

template<class Float>
void doWeightedSquareRow(const boost::multi_array<Float, 2> & XT,
                         const distribution<Float> & d,
                         boost::multi_array<Float, 2> & result,
                         int i)
{
    int chunk_size = 2048;  // ensure we fit in the cache

    size_t nx = XT.shape()[1];
    size_t nv = XT.shape()[0];

    int x = 0;
    while (x < nx) {
        int nxc = std::min<size_t>(chunk_size, nx - x);
        distribution<Float> Xid(chunk_size);
        SIMD::vec_prod(&XT[i][x], &d[x], &Xid[0], nxc);
            
        for (unsigned j = 0;  j < nv;  ++j) {
            result[i][j] += SIMD::vec_dotprod_dp(&XT[j][x], &Xid[0], nxc);
        }

        x += nxc;
    }

}

template<class Float>
boost::multi_array<Float, 2>
weighted_square_impl(const boost::multi_array<Float, 2> & XT,
                     const distribution<Float> & d)
{
    if (XT.shape()[1] != d.size())
        throw Exception("Incompatible matrix sizes for weighted_square");

    size_t nx = XT.shape()[1];
    size_t nv = XT.shape()[0];

    //cerr << "nx = " << nx << " nv = " << nv << endl;

    boost::multi_array<Float, 2> result(boost::extents[nv][nv]);

    if (false) {
        int chunk_size = 2048;  // ensure we fit in the cache
        distribution<Float> Xid(chunk_size);

        int x = 0;
        while (x < nx) {
            int nxc = std::min<size_t>(chunk_size, nx - x);

            for (unsigned i = 0;  i < nv;  ++i) {
                SIMD::vec_prod(&XT[i][x], &d[x], &Xid[0], nxc);
            
                for (unsigned j = 0;  j < nv;  ++j) {
                    result[i][j] += SIMD::vec_dotprod_dp(&XT[j][x], &Xid[0], nxc);
                }
            }

            x += nxc;
        }
    } else {
        MLDB::parallelMap(0, nv, std::bind(doWeightedSquareRow<Float>,
                                                 std::cref(XT),
                                                 std::cref(d),
                                                 std::ref(result),
                                                 std::placeholders::_1));
    }
    
    return result;
}

boost::multi_array<float, 2>
weighted_square(const boost::multi_array<float, 2> & XT,
                const distribution<float> & d)
{
    return weighted_square_impl(XT, d);
}

boost::multi_array<double, 2>
weighted_square(const boost::multi_array<double, 2> & XT,
                const distribution<double> & d)
{
    return weighted_square_impl(XT, d);
}

template<typename Float>
void svd_square_impl(boost::multi_array<Float, 2> & X,
                     boost::multi_array<Float, 2> & VT,
                     boost::multi_array<Float, 2> & U,
                     distribution<Float> & svalues)
{
    size_t minmn = X.shape()[0];
    ExcAssertEqual(minmn, X.shape()[1]);

    // Decompose to get the pseudoinverse
    svalues.clear();
    svalues.resize(minmn);

    VT.resize(boost::extents[minmn][minmn]);
    U.resize(boost::extents[minmn][minmn]);

    // SVD
    int result = LAPack::gesdd("S", minmn, minmn,
                               X.data(), minmn,
                               &svalues[0],
                               &VT[0][0], minmn,
                               &U[0][0], minmn);

    if (result != 0)
        throw Exception("gesdd returned non-zero");
}

void svd_square(boost::multi_array<float, 2> & X,
                boost::multi_array<float, 2> & VT,
                boost::multi_array<float, 2> & U,
                distribution<float> & svalues)
{
    return svd_square_impl(X, VT, U, svalues);
}

void svd_square(boost::multi_array<double, 2> & X,
                boost::multi_array<double, 2> & VT,
                boost::multi_array<double, 2> & U,
                distribution<double> & svalues)
{
    return svd_square_impl(X, VT, U, svalues);
}

} // namespace ML
