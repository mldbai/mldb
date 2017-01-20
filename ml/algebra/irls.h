/* irls.h                                                          -*- C++ -*-
   Jeremy Barnes, 19 March 2004
   Copyright (c) 2004 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Iteratively Reweighted Least Squares.
*/

#pragma once

#include <vector>
#include <boost/multi_array.hpp>
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/jml/utils/enum_info.h"

namespace ML {

using namespace MLDB;

/*****************************************************************************/
/* Regularization                                                            */
/*****************************************************************************/

enum Regularization{
    Regularization_none = 0,
    Regularization_l1,
    Regularization_l2,
};

COMPACT_PERSISTENT_ENUM_DECL(Regularization);

/** Print out a Regularization enum. */
std::string print(Regularization link);

/** Parse the name of a Regularization enum. */
Regularization parse_regularization_function(const std::string & name);

/** Put a Regularization enum in a stream. */
std::ostream & operator << (std::ostream & stream, Regularization link);



/** Implementation of the removal of the linearly dependent rows.
    In addition to the permutation vector, it also provides a vector
    for each column giving the linear combination of other columns that
    give that column.
*/
template<class FloatIn, class FloatCalc>
std::vector<int>
remove_dependent_impl(boost::multi_array<FloatIn, 2> & x,
                      std::vector<distribution<FloatCalc> > & y,
                      double tolerance = 1e-5);

/** Removes any linearly dependent rows from the matrix X.  Returns a
    vector with the same number of rows as x originally had, indicating
    where each row has moved to in the new matrix, or -1 if the column
    was removed.
*/
std::vector<int> remove_dependent(boost::multi_array<double, 2> & x);

/** Removes any linearly dependent rows from the matrix X.  Returns a
    vector with the same number of rows as x originally had, indicating
    where each row has moved to in the new matrix, or -1 if the column
    was removed.
*/
std::vector<int> remove_dependent(boost::multi_array<float, 2> & x);

/* Tell the compiler about the explicit instantiations */
extern template
std::vector<int> remove_dependent_impl(boost::multi_array<float, 2> & x,
                                       std::vector<distribution<float> > & y,
                                       double tolerance);
extern template
std::vector<int> remove_dependent_impl(boost::multi_array<float, 2> & x,
                                       std::vector<distribution<double> > & y,
                                       double tolerance);
extern template
std::vector<int> remove_dependent_impl(boost::multi_array<double, 2> & x,
                                       std::vector<distribution<double> > & y,
                                       double tolerance);


/** An enumeration which contains all of the link functions which we can
    perform a GLZ over.
*/
enum Link_Function {
    LOGIT,         ///< Logit, good generic link for probabilistic
    PROBIT,        ///< Probit, need to know response means
    COMP_LOG_LOG,  ///< Another good for probabilistic
    LINEAR,        ///< Linear; makes it solve linear least squares (identity)
    LOG            ///< Logarithm; good for transforming the output of boosting
};

COMPACT_PERSISTENT_ENUM_DECL(Link_Function);

/** Print out a link function. */
std::string print(Link_Function link);

/** Parse the name of a link function. */
Link_Function parse_link_function(const std::string & name);

/** Put a link function in a stream. */
std::ostream & operator << (std::ostream & stream, Link_Function link);


/*****************************************************************************/
/* REGRESSOR                                                                 */
/*****************************************************************************/

struct Regressor {
    virtual ~Regressor();

    /// Calculates the least squares of Ax = B
    virtual distribution<float>
    calc(const boost::multi_array<float, 2> & A,
         const distribution<float> & b) const = 0;

    /// Calculates the least squares of Ax = B
    virtual distribution<double>
    calc(const boost::multi_array<double, 2> & A,
         const distribution<double> & b) const = 0;

    /// Calculates the least squares of A * diag(aScale) * transpose(A) * x = b
    virtual distribution<float>
    calc_scaled(const boost::multi_array<float, 2> & A,
                const distribution<float> & aScale,
                const distribution<float> & b) const;

    /// Calculates the least squares of A * diag(aScale) * transpose(A) * x = b
    virtual distribution<double>
    calc_scaled(const boost::multi_array<double, 2> & A,
                const distribution<double> & aScale,
                const distribution<double> & b) const;
};

struct Least_Squares_Regressor : Regressor {
    Least_Squares_Regressor()
    {
    }

    virtual ~Least_Squares_Regressor();

    virtual distribution<float>
    calc(const boost::multi_array<float, 2> & A,
         const distribution<float> & b) const;

    virtual distribution<double>
    calc(const boost::multi_array<double, 2> & A,
         const distribution<double> & b) const;
};

struct Ridge_Regressor : Regressor {
    Ridge_Regressor(double lambda = 1e-10);
    virtual ~Ridge_Regressor();

    double lambda;

    virtual distribution<float>
    calc(const boost::multi_array<float, 2> & A,
         const distribution<float> & b) const;

    virtual distribution<double>
    calc(const boost::multi_array<double, 2> & A,
         const distribution<double> & b) const;

    virtual distribution<float>
    calc_scaled(const boost::multi_array<float, 2> & A,
                const distribution<float> & aScale,
                const distribution<float> & b) const;

    virtual distribution<double>
    calc_scaled(const boost::multi_array<double, 2> & A,
                const distribution<double> & aScale,
                const distribution<double> & b) const;
};

struct Lasso_Regressor : Regressor {

    Lasso_Regressor(double lambda = 1e-5, int maxIter = 1000, double epsilon = 1e-4);

    virtual ~Lasso_Regressor();

    virtual distribution<float>
    calc(const boost::multi_array<float, 2> & A,
         const distribution<float> & b) const;

    virtual distribution<double>
    calc(const boost::multi_array<double, 2> & A,
         const distribution<double> & b) const;

    double lambda;
    int maxIter;
    double epsilon;
};


const Regressor & default_regressor();


/*****************************************************************************/
/* IRLS                                                                      */
/*****************************************************************************/

/** Run the iteratively reweighted least squares algorithm with a logit link
    function and a binomial distribution.
    
    \param correct          The target vector, with nx entries.
    \param outputs          The response matrix, a nv x nx matrix.
    \param weights          The weights vector, with nx entries.

    \returns                A vector with nv entries, giving the trained
                            weights for the IRLS problem.
*/
distribution<double>
irls_logit(const distribution<double> & correct,
           const boost::multi_array<double, 2> & outputs,
           const distribution<double> & w,
           const Regressor & regressor);

distribution<float>
irls_logit(const distribution<float> & correct,
           const boost::multi_array<float, 2> & outputs,
           const distribution<float> & w,
           const Regressor & regressor);


/** Run the iteratively reweighted least squares algorithm with a log link
    function and a binomial distribution.

    \copydoc irls_logit
*/
distribution<double>
irls_log(const distribution<double> & correct,
         const boost::multi_array<double, 2> & outputs,
         const distribution<double> & w,
         const Regressor & regressor);

distribution<float>
irls_log(const distribution<float> & correct,
         const boost::multi_array<float, 2> & outputs,
         const distribution<float> & w,
         const Regressor & regressor);


/** Run the iteratively reweighted least squares algorithm with a linear link
    function and a normal distribution.

    \copydoc irls_logit
*/
distribution<double>
irls_linear(const distribution<double> & correct,
            const boost::multi_array<double, 2> & outputs,
            const distribution<double> & w,
            const Regressor & regressor);

distribution<float>
irls_linear(const distribution<float> & correct,
            const boost::multi_array<float, 2> & outputs,
            const distribution<float> & w,
            const Regressor & regressor);


/** Run the iteratively reweighted least squares algorithm with a probit link
    function and a binomial distribution.

    \copydoc irls_logit
*/
distribution<double>
irls_probit(const distribution<double> & correct,
            const boost::multi_array<double, 2> & outputs,
            const distribution<double> & w,
            const Regressor & regressor);

distribution<float>
irls_probit(const distribution<float> & correct,
            const boost::multi_array<float, 2> & outputs,
            const distribution<float> & w,
            const Regressor & regressor);


/** Run the iteratively reweighted least squares algorithm with a
    complementary log-log link function and a binomial distribution.
    
    \copydoc irls_logit
*/
distribution<double>
irls_complog(const distribution<double> & correct,
             const boost::multi_array<double, 2> & outputs,
             const distribution<double> & w,
             const Regressor & regressor);

distribution<float>
irls_complog(const distribution<float> & correct,
             const boost::multi_array<float, 2> & outputs,
             const distribution<float> & w,
             const Regressor & regressor);


/** Run the correct version of the IRLS algorithm depending upon the link
    function selected. 

    \param func         Link function to use.  Default is logit, which is
                        pretty good for general purpose use.
*/
distribution<double>
run_irls(const distribution<double> & correct,
         const boost::multi_array<double, 2> & outputs,
         const distribution<double> & w,
         Link_Function func = LOGIT,
         const Regressor & regressor = default_regressor());

distribution<float>
run_irls(const distribution<float> & correct,
         const boost::multi_array<float, 2> & outputs,
         const distribution<float> & w,
         Link_Function func = LOGIT,
         const Regressor & regressor = default_regressor());

/** Apply the inverse IRLS function to the given value. */
double apply_link_inverse(double val, Link_Function func);

/** Implementation of the error function. */
double erf(double);
double erfinv(double);


/** Perform an IRLS in single precision.

    \param condition  Tells us whether to run a slow pre-conditioning
                      step on the matrix first.  This can help avoid
                      numerical issues where ridge_regression is off
                      on under-determined problems, but is very slow
                      for large matrices.
*/
distribution<float>
perform_irls(const distribution<float> & correct,
             const boost::multi_array<float, 2> & outputs,
             const distribution<float> & w,
             Link_Function link_function,
             bool ridge_regression = true,
             float regularization_factor = 1e-5,
             int maxIter = 20,
             float epsilon = 1e-4,
             bool condition = true);

/** Perform an IRLS in double precision.

    \param condition  Tells us whether to run a slow pre-conditioning
                      step on the matrix first.  This can help avoid
                      numerical issues where ridge_regression is off
                      on under-determined problems, but is very slow
                      for large matrices.
*/
distribution<double>
perform_irls(const distribution<double> & correct,
             const boost::multi_array<double, 2> & outputs,
             const distribution<double> & w,
             Link_Function link_function,
             Regularization = Regularization_l2,
             double regularization_factor = 1e-5,
             int maxIter = 20,
             double epsilon = 1e-4,
             bool condition = true);


} // namespace ML


DECLARE_ENUM_INFO(ML::Link_Function, 5);
DECLARE_ENUM_INFO(ML::Regularization, 3);

