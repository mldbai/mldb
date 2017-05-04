// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* boolean_expression_parser.h                                     -*- C++ -*-
   Jeremy Barnes, 23 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "boolean_expression.h"
#include "mldb/base/parse_context.h"

namespace MLDB {

struct BooleanExpressionParser;


/*****************************************************************************/
/* UNARY OPERATOR PARSER                                                     */
/*****************************************************************************/

struct UnaryOperatorParser {

    virtual std::string getOperator() const = 0;

    virtual BoolExprPtr
    parse(ParseContext & context,
          const BooleanExpressionParser & parser) const = 0;
};


/*****************************************************************************/
/* BINARY OPERATOR PARSER                                                    */
/*****************************************************************************/

struct BinaryOperatorParser {
    virtual std::string getOperator() const = 0;

    virtual BoolExprPtr parse(BoolExprPtr lhs,
                              ParseContext & context,
                              const BooleanExpressionParser & parser) const = 0;
};

/*****************************************************************************/
/* SEGMENT EXPRESSION PARSER                                                 */
/*****************************************************************************/

struct SegmentExpressionParser {
    virtual BoolExprPtr parse(ParseContext & context,
                              const BooleanExpressionParser & parser) const = 0;
};


/*****************************************************************************/
/* FUNCTIONAL EXPRESSION PARSER                                              */
/*****************************************************************************/

struct FunctionalExpressionParser {
    virtual std::string getFunctionName() const = 0;
    virtual BoolExprPtr parse(ParseContext & context,
                              const BooleanExpressionParser & parser) const = 0;
};


/*****************************************************************************/
/* BOOLEAN EXPRESSION PARSER                                                 */
/*****************************************************************************/

/** Parser for a boolean expression.  This allows each of the productions
    to be extended.
*/

struct BooleanExpressionParser {

    BooleanExpressionParser();


    /*************************************************************************/
    /* PARSING API                                                           */
    /*************************************************************************/

    /** Parse from a ParseContext. */
    virtual BoolExprPtr parse(ParseContext & context,
                              bool greedy = true) const;

    /** Parse from a string. */
    BoolExprPtr parse(const std::string & expr) const;


    /*************************************************************************/
    /* EXTENSION API                                                         */
    /*************************************************************************/

    /// Add a binary operator such as AND, OR, ...
    void addBinaryOperator(BinaryOperatorParser * parser);

    /// Add a unary operator such as NOT
    void addUnaryOperator(UnaryOperatorParser * parser);

    /// Add a functional expression such as CHILDREN(node)
    void addFunctionalExpression(FunctionalExpressionParser * parser);

    /// Add a free segment expression such as 123, "hello", etc
    void addFreeSegmentExpression(SegmentExpressionParser * parser);

    /// Clear all free segment expressions
    void clearFreeSegmentExpressions();

protected:
    std::map<std::string, std::unique_ptr<FunctionalExpressionParser> >
        functions;
    std::map<std::string, std::unique_ptr<BinaryOperatorParser> >
        binaryOperators;
    std::map<std::string, std::unique_ptr<UnaryOperatorParser> >
        unaryOperators;
    std::vector<std::unique_ptr<SegmentExpressionParser> >
        freeSegments;

    /** Parse a free segment expression.  Returns the segment if it was
        matched, or a null pointer otherwise.
    */
    BoolExprPtr parseFreeSegment(ParseContext & context) const;

    /** Try to find a unary expression at the current position, and return
        a parser for the found expression if there is one.
    */
    virtual const UnaryOperatorParser *
    getUnaryParser(ParseContext & context) const;

    virtual const BinaryOperatorParser *
    getBinaryParser(ParseContext & context) const;

    virtual const FunctionalExpressionParser *
    getFunctionParser(ParseContext & context) const;


};

/*****************************************************************************/
/* BINARY OPERATOR PARSER TEMPLATE                                           */
/*****************************************************************************/

/** Constructs a parser for a binary operator with a normal single-token
    keyword.
*/

template<class Result>
struct BinaryOperatorParserT: public BinaryOperatorParser {
    BinaryOperatorParserT(const std::string & op)
        : op(op)
    {
    }

    std::string op;

    virtual std::string getOperator() const
    {
        return op;
    }
    
    virtual BoolExprPtr parse(BoolExprPtr lhs,
                              ParseContext & context,
                              const BooleanExpressionParser & parser) const
    {
        std::vector<BoolExprPtr> exprs(1, lhs);
        context.skip_whitespace();
        for (;;) {
            BoolExprPtr expr = parser.parse(context, false);
            if (!expr)
                context.exception("expected expression after " + op + " operator");
            exprs.push_back(expr);
            context.match_whitespace();
            if (!context.match_literal(op)) {
                if (context && *context != ')')
                    context.exception("expected " + op);
                break;
            }
        }

        return std::make_shared<Result>(exprs);
    }
};

/*****************************************************************************/
/* STANDARD PARSERS                                                          */
/*****************************************************************************/

/** Parse a free string or integer that can be used as an ID. */

struct IdSegmentExpressionParser: public SegmentExpressionParser {

    virtual BoolExprPtr
    parse(ParseContext & context,
          const BooleanExpressionParser & parser) const;
};

struct SegNameContainsSegmentExpressionParser: public SegmentExpressionParser {

    virtual BoolExprPtr
    parse(ParseContext & context,
          const BooleanExpressionParser & parser) const;
};

struct RegexSegmentExpressionParser: public SegmentExpressionParser {

    virtual BoolExprPtr
    parse(ParseContext & context,
          const BooleanExpressionParser & parser) const;
};


/** Parse a NOT expression. */

struct NotExpressionParser : public UnaryOperatorParser {
    
    virtual std::string getOperator() const;

    virtual BoolExprPtr
    parse(ParseContext & context,
          const BooleanExpressionParser & parser) const;
};

/** Parsers for AND, OR, THEN based upon the standard pattern. */

typedef BinaryOperatorParserT<AndExpression> AndExpressionParser;
typedef BinaryOperatorParserT<OrExpression> OrExpressionParser;
typedef BinaryOperatorParserT<ThenExpression> ThenExpressionParser;

/** Parser for WITHIN(time) */

struct WithinExpressionParser: public BinaryOperatorParser {
    WithinExpressionParser(const std::string & op = "WITHIN");
    std::string op;

    virtual std::string getOperator() const;
    
    virtual BoolExprPtr parse(BoolExprPtr lhs,
                              ParseContext & context,
                              const BooleanExpressionParser & parser) const;
};

struct TimesFunctionExpressionParser : public FunctionalExpressionParser
{
    virtual std::string getFunctionName() const;
    virtual BoolExprPtr parse(ParseContext & context,
                              const BooleanExpressionParser & parser) const;
};

} // namespace MLDB
