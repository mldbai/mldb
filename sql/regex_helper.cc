/** regex_helper.cc
    Jeremy Barnes, 31 October 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

*/

#include "regex_helper.h"
#include "mldb/http/http_exception.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;

namespace MLDB {

/*****************************************************************************/
/* REGEX HELPER                                                              */
/*****************************************************************************/

RegexHelper::
RegexHelper()
{
}

void
RegexHelper::
init(BoundSqlExpression expr_, int argNumber)
{
    expr = std::move(expr_);
    this->argNumber = argNumber;

    if (expr.metadata.isConstant) {
        isPrecompiled = true;
        precompiled = compile(expr.constantValue());
    }
    else isPrecompiled = false;
}

Regex
RegexHelper::
compile(const ExpressionValue & val) const
{
    Utf8String regexStr;
    try {
        regexStr = val.toUtf8String();
    } MLDB_CATCH_ALL {
        rethrowHttpException
            (400, "Error when extracting regex from argument '"
             + expr.expr->surface + "': " + getExceptionString()
             + ".  Regular expressions need to be strings.",
             "expr", expr,
             "value", val);
    }
    try {
        return Regex(std::move(regexStr));
    } MLDB_CATCH_ALL {
        rethrowHttpException
            (400, "Error when compiling regex '"
             + regexStr + "' from expression " + expr.expr->surface + "': "
             + getExceptionString()
             + ".  Regular expressions must adhere to Perl-style "
             + "regular expression syntax.",
             "expr", expr,
             "value", val);
    }
}

ExpressionValue
RegexHelper::
operator () (const std::vector<ExpressionValue> & args,
             const SqlRowScope & scope)
{
    if (isPrecompiled) {
        return apply(args, scope, precompiled);
    }
    else {
        return apply(args, scope, compile(args.at(argNumber)));
    }
}


/*****************************************************************************/
/* APPLY REGEX REPLACE                                                       */
/*****************************************************************************/

ApplyRegexReplace::
ApplyRegexReplace(BoundSqlExpression e)
{
    init(std::move(e), 1 /* argNumber */);
}

ExpressionValue
ApplyRegexReplace::
apply(const std::vector<ExpressionValue> & args,
      const SqlRowScope & scope,
      const Regex & regex) const
{
    checkArgsSize(args.size(), 3);

    if (args[0].empty() || args[1].empty() || args[2].empty())
        return ExpressionValue::null(calcTs(args[0], args[1], args[2]));

    auto result = regex_replace(args[0].toUtf8String(),
                                regex,
                                args[2].toUtf8String());
    
    return ExpressionValue(std::move(result), calcTs(args[0], args[1], args[2]));
}


/*****************************************************************************/
/* APPLY REGEX MATCH                                                         */
/*****************************************************************************/

ApplyRegexMatch::
ApplyRegexMatch(BoundSqlExpression e)
{
    init(std::move(e), 1 /* argNumber */);
}

ExpressionValue
ApplyRegexMatch::
apply(const std::vector<ExpressionValue> & args,
      const SqlRowScope & scope,
      const Regex & regex) const
{
    // TODO: should be able to pass utf-8 string directly in

    checkArgsSize(args.size(), 2);

    if (args[0].empty() || args[1].empty())
        return ExpressionValue::null(calcTs(args[0], args[1]));

    bool result = regex_match(args[0].toUtf8String(), regex);
    return ExpressionValue(result, calcTs(args[0], args[1]));
}


/*****************************************************************************/
/* APPLY REGEX SEARCH                                                        */
/*****************************************************************************/

ApplyRegexSearch::
ApplyRegexSearch(BoundSqlExpression e)
{
    init(std::move(e), 1 /* argNumber */);
}

ExpressionValue
ApplyRegexSearch::
apply(const std::vector<ExpressionValue> & args,
      const SqlRowScope & scope,
      const Regex & regex) const
{
    // TODO: should be able to pass utf-8 string directly in

    checkArgsSize(args.size(), 2);

    if (args[0].empty() || args[1].empty())
        return ExpressionValue::null(calcTs(args[0], args[1]));

    bool result = regex_search(args[0].toUtf8String(), regex);
    return ExpressionValue(result, calcTs(args[0], args[1]));
}


/*****************************************************************************/
/* APPLY LIKE                                                                */
/*****************************************************************************/

ApplyLike::
ApplyLike(BoundSqlExpression e, bool isNegative)
    : isNegative(isNegative)
{
    init(std::move(e), 1 /* argNumber */);
}

/// Return the regex string that matches the same values as the given LIKE
/// filter.
Utf8String likeToRegex(const Utf8String& filterString)
{
    Utf8String regExFilter;

    for (const auto& filterChar : filterString) {

        switch (filterChar) {
            case ('%'): {
                regExFilter += ".*";
                break;
            }
            case ('_'): {
                regExFilter += ".";
                break;
            }
            case ('*'): {
                regExFilter += "[*]";
                break;
            }
            case ('['): {
                regExFilter += "\\[";
                break;
            }
            case (']'): {
                regExFilter += "\\]";
                break;
            }
            case ('.'): {
                regExFilter += "[.]";
                break;
            }
            case ('|'): {
                regExFilter += "[|]";
                break;
            }
            case ('('): {
                regExFilter += "[(]";
                break;
            }
            case (')'): {
                regExFilter += "[)]";
                break;
            }
            case ('^'): {
                regExFilter += "\\^";
                break;
            }
            case ('$'): {
                regExFilter += "[$]";
                break;
            }
            default: {
                regExFilter += filterChar;
            }
        }
    }

    return regExFilter;
}

Regex
ApplyLike::
compile(const ExpressionValue & val) const
{
    if (val.empty()) {
        return Regex();
    }
    Utf8String regexStr = likeToRegex(val.toUtf8String());
    ExpressionValue regexVal(std::move(regexStr), val.getEffectiveTimestamp());
    return RegexHelper::compile(regexVal);
}

ExpressionValue
ApplyLike::
apply(const std::vector<ExpressionValue> & args,
      const SqlRowScope & scope,
      const Regex & regex) const
{
    checkArgsSize(args.size(), 2);

    if (!regex.initialized()) {
        return ExpressionValue::null(Date::negativeInfinity());
    }

    if (args[0].empty())
        return args[0];

    if (!args[0].isString()) {
        throw HttpReturnException
            (400, "LIKE expression must have string on left side");
    }
    
    bool result = regex_match(args[0].toUtf8String(), regex);

    if (isNegative)
        result = !result;

    return ExpressionValue(result, args[0].getEffectiveTimestamp());
}

} // namespace MLDB
