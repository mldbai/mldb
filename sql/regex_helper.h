/** regex_helper.h                                                 -*- C++ -*-
    Jeremy Barnes, 31 October 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

    Helper classes for regex.
*/

#include "mldb/types/regex.h"
#include "sql_expression.h"


namespace MLDB {


/*****************************************************************************/
/* REGEX HELPER                                                              */
/*****************************************************************************/

/** Helper class that takes care of regular expression application whether
    it's a constant value or not.
*/
struct RegexHelper {
    RegexHelper();

    /// Must be called by child constructor.  This performs the actual
    /// compilation, etc.  It can't be done in the real constructor as
    /// then the compile method would be bound to this one, not the one
    /// overriden in the sub-class.
    void init(BoundSqlExpression expr,
              int argNumber);

    /// Called to take an expression and turn it into a regex that will be
    /// applied.  Default simply compiles a standard regex.
    virtual Regex compile(const ExpressionValue & val) const;

    /// The expression that the regex came from, to help with error messages
    BoundSqlExpression expr;

    /// The pre-compiled version of that expression, when it's constant
    Regex precompiled;

    /// Is it actually constant (and precompiled), or computed on the fly?
    bool isPrecompiled;

    /// What argument contains the regex to be compiled if it's not
    /// constant?
    int argNumber;

    virtual ExpressionValue apply(const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & scope,
                                  const Regex & regex) const = 0;

    ExpressionValue operator () (const std::vector<ExpressionValue> & args,
                                 const SqlRowScope & scope);
};


/*****************************************************************************/
/* APPLY REGEX REPLACE                                                       */
/*****************************************************************************/

struct ApplyRegexReplace: public RegexHelper {
    ApplyRegexReplace(BoundSqlExpression e);

    virtual ExpressionValue apply(const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & scope,
                                  const Regex & regex) const;
};


/*****************************************************************************/
/* APPLY REGEX MATCH                                                         */
/*****************************************************************************/

struct ApplyRegexMatch: public RegexHelper {
    ApplyRegexMatch(BoundSqlExpression e);

    virtual ExpressionValue apply(const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & scope,
                                  const Regex & regex) const;
};


/*****************************************************************************/
/* APPLY REGEX SEARCH                                                        */
/*****************************************************************************/

struct ApplyRegexSearch: public RegexHelper {
    ApplyRegexSearch(BoundSqlExpression e);

    virtual ExpressionValue apply(const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & scope,
                                  const Regex & regex) const;
};


/*****************************************************************************/
/* APPLY LIKE                                                                */
/*****************************************************************************/

/** Apply a like expression. */

struct ApplyLike: public RegexHelper {
    ApplyLike(BoundSqlExpression e, bool isNegative);

    /// For the LIKE expression, there is a different syntax to standard
    /// regular expressions, which we deal with here.
    virtual Regex compile(const ExpressionValue & val) const;

    virtual ExpressionValue apply(const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & scope,
                                  const Regex & regex) const;

    /// This inverts it, ie turns LIKE into NOT LIKE
    bool isNegative;
};



} // namespace MLDB
