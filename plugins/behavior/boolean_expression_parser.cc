// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* boolean_expression_parser.cc
   Jeremy Banres, 23 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Expression parser for boolean expressions.
*/

#include "boolean_expression_parser.h"
#include "mldb/types/periodic_utils.h"
#include "mldb/types/json_parsing.h"

using namespace std;


namespace MLDB {


// Simple parser rules:
// Operators are binary or unary
// Operators can take arguments in ()
// Segments are either strings, integers or FUNCTION(args)
//
// Grammar:
// SEGMENT  -> integer | " string " | FREESEGMENT (exensible)
// BINARYOP -> AND | OR | THEN | WITHIN(args) | BINARYFUNCTION(args)
// UNARYOP  -> NOT | UNARYFUNCTION(args)
// EXPRESSION -> SEGMENT | ( EXPRESSION ) | UNARYOP EXPRESSION | EXPRESSION BINARYOP EXPRESSION BINARYOP ... | FUNCTION(args)


/******************************************************************************/
/* MATCH ID                                                                   */
/******************************************************************************/

namespace {

bool matchId(ParseContext & context, Id& id)
{
    uint64_t i;
    if (context.match_unsigned_long(i)) {
        id = Id(i);
        return true;
    }

    if (context.match_literal('\"')) {
        string name = context.expect_text('\"');
        context.expect_literal('\"');

        id = Id(name);
        return true;
    }

    return false;
}

} // namespace anonymous


/*****************************************************************************/
/* NOT EXPRESSION PARSER                                                     */
/*****************************************************************************/

std::string
NotExpressionParser::
getOperator() const
{
    return "NOT";
}

BoolExprPtr
NotExpressionParser::
parse(ParseContext & context,
      const BooleanExpressionParser & parser) const
{
    context.skip_whitespace();
    //cerr << "*** NOT ***" << endl;
    BoolExprPtr internal = parser.parse(context, false /* greedy */);
    if (!internal)
        context.exception("Expected expression after NOT");
    return std::make_shared<NotExpression>(internal);
}


/*****************************************************************************/
/* WITHIN EXPRESSION PARSER                                                  */
/*****************************************************************************/

WithinExpressionParser::
WithinExpressionParser(const std::string & op)
    : op(op)
{
}

std::string
WithinExpressionParser::
getOperator() const
{
    return op;
}
    
BoolExprPtr
WithinExpressionParser::
parse(BoolExprPtr lhs,
      ParseContext & context,
      const BooleanExpressionParser & parser) const
{
    //cerr << "Within parsing" << endl;
    // First, extract the parameter (days)

    auto expectTime = [&] ()
        {
            try {
                context.skip_whitespace();
                context.expect_literal("(");
                context.skip_whitespace();
                std::string str = expectJsonStringAscii(context);
                context.skip_whitespace();
                context.expect_literal(")");
            
                TimeGranularity granularity;
                double number;
                std::tie(granularity, number)
                = parsePeriod(str);
                switch (granularity) {
                case MILLISECONDS:     return number * 0.001;
                case SECONDS:          return number;
                case MINUTES:          return number * 60;
                case HOURS:            return number * 3600;
                case DAYS:             return number * 86400;
                case WEEKS:            return number * 7 * 86400;
                case MONTHS:           return number * 30 * 86400;
                case YEARS:            return number * 365 * 86400;
                default:
                    throw MLDB::Exception("bad time spec");
                }
            } catch (const std::exception & exc) {
                context.exception(string("bad within time specification: ")
                                  + exc.what());
            }
        };

    std::vector<BoolExprPtr> exprs(1, lhs);
    std::vector<double> seconds;
    context.skip_whitespace();
    for (;;) {
        seconds.push_back(expectTime());
        BoolExprPtr expr = parser.parse(context, false);
        if (!expr)
            context.exception("expected expression after " + op + " operator");
        exprs.push_back(expr);
        context.match_whitespace();
        if (!context.match_literal(op))
            break;
    }

    return std::make_shared<WithinExpression>(exprs, seconds);
}


/*****************************************************************************/
/* ID SEGMENT EXPRESSION PARSER                                              */
/*****************************************************************************/

BoolExprPtr
IdSegmentExpressionParser::
parse(ParseContext & context, const BooleanExpressionParser & parser) const
{
    Id id;
    if (!matchId(context, id)) return nullptr;

    return std::make_shared<SegExpression>(id);
}

/*****************************************************************************/
/* CONTAINS SEGMENT EXPRESSION PARSER                                        */
/*****************************************************************************/
BoolExprPtr
SegNameContainsSegmentExpressionParser::
parse(ParseContext & context, const BooleanExpressionParser & parser) const
{
    // cerr << "parsing CONTAINS ID segment" << endl;
    context.skip_whitespace();
    string op = "SEG_NAME_CONTAINS";
    if(context.match_literal_str(op.c_str(), op.size())) {
        context.expect_literal("(");
        context.expect_literal("\"");
        string name = context.expect_text('\"');
        context.expect_literal("\"");
        context.expect_literal(")");
        // cerr << "got string " << name << endl;
        return std::make_shared<SegNameContainsExpression>(name);
    }
    else return nullptr;
}

/*****************************************************************************/
/* REGEX EXPRESSION PARSER                                        */
/*****************************************************************************/
BoolExprPtr
RegexSegmentExpressionParser::
parse(ParseContext & context, const BooleanExpressionParser & parser) const
{
    // cerr << "parsing REGEX segment" << endl;
    context.skip_whitespace();
    string op = "REGEX";
    if(context.match_literal_str(op.c_str(), op.size())) {
        context.expect_literal("(");
        context.expect_literal("\"");
        string name = context.expect_text('\"');
        context.expect_literal("\"");
        context.expect_literal(")");
        // cerr << "got string " << name << endl;
        return std::make_shared<RegexExpression>(name);
    }
    else return nullptr;
}


/******************************************************************************/
/* TIMES FUNCTION EXPRESSION PARSER                                           */
/******************************************************************************/

std::string
TimesFunctionExpressionParser::
getFunctionName() const
{
    return "TIMES";
}

BoolExprPtr
TimesFunctionExpressionParser::
parse(  ParseContext & context,
        const BooleanExpressionParser & parser) const
{
    context.skip_whitespace();
    //cerr << "*** NOT ***" << endl;
    BoolExprPtr subexpr = parser.parse(context, false /* greedy */);
    if (!subexpr)
        context.exception("Expected expression after TIMES(");
    
    context.skip_whitespace();
    context.match_literal(',');
    context.skip_whitespace();

    unsigned count;
    if (!context.match_unsigned(count))
        context.exception("argument 2 of TIMES should be an unsigned int");

    return std::make_shared<TimesFunctionExpression>(subexpr, count);
}


/*****************************************************************************/
/* BOOLEAN EXPRESSION PARSER                                                 */
/*****************************************************************************/

BooleanExpressionParser::
BooleanExpressionParser()
{
    addBinaryOperator(new AndExpressionParser("AND"));
    addBinaryOperator(new OrExpressionParser("OR"));
    addBinaryOperator(new ThenExpressionParser("THEN"));
    addBinaryOperator(new WithinExpressionParser("WITHIN"));
        
    addUnaryOperator(new NotExpressionParser());

    addFreeSegmentExpression(new IdSegmentExpressionParser());
    addFreeSegmentExpression(new SegNameContainsSegmentExpressionParser());
    addFreeSegmentExpression(new RegexSegmentExpressionParser());

    addFunctionalExpression(new TimesFunctionExpressionParser());
}

void
BooleanExpressionParser::
addBinaryOperator(BinaryOperatorParser * parser)
{
    ExcAssert(parser);
    string op = parser->getOperator();
    binaryOperators[op].reset(parser);
}

void
BooleanExpressionParser::
addUnaryOperator(UnaryOperatorParser * parser)
{
    ExcAssert(parser);
    string op = parser->getOperator();
    unaryOperators[op].reset(parser);
}

void
BooleanExpressionParser::
addFunctionalExpression(FunctionalExpressionParser * parser)
{
    ExcAssert(parser);
    string fn = parser->getFunctionName();
    functions[fn].reset(parser);
}

void
BooleanExpressionParser::
addFreeSegmentExpression(SegmentExpressionParser * parser)
{
    ExcAssert(parser);
    freeSegments.push_back(std::unique_ptr<SegmentExpressionParser>(parser));
}

void
BooleanExpressionParser::
clearFreeSegmentExpressions()
{
    freeSegments.clear();
}

BoolExprPtr
BooleanExpressionParser::
parseFreeSegment(ParseContext & context) const
{
    for (auto & p: freeSegments) {
        auto r = p->parse(context, *this);
        if (r) return r;
    }
    return nullptr;
}

const UnaryOperatorParser *
BooleanExpressionParser::
getUnaryParser(ParseContext & context) const
{
    ParseContext::Revert_Token token(context);
    string op;
    if (!context.match_text(op, " "))
        return nullptr;
    auto it = unaryOperators.find(op);
    if (it != unaryOperators.end()) {
        token.ignore();
        return it->second.get();
    }
    return nullptr;
}

const BinaryOperatorParser *
BooleanExpressionParser::
getBinaryParser(ParseContext & context) const
{
    ParseContext::Revert_Token token(context);
    string op;
    if (!context.match_text(op, " ("))
        return nullptr;

    auto it = binaryOperators.find(op);
    if (it != binaryOperators.end()) {
        token.ignore();
        return it->second.get();
    }
    return nullptr;
}

const FunctionalExpressionParser *
BooleanExpressionParser::
getFunctionParser(ParseContext & context) const
{
    ParseContext::Revert_Token token(context);
    string op;
    if (!context.match_text(op, " ("))
        return nullptr;
    context.skip_whitespace();
    if (!context.match_literal('('))
        return nullptr;
    auto it = functions.find(op);
    if (it != functions.end()) {
        token.ignore();
        return it->second.get();
    }
    return nullptr;
}

BoolExprPtr
BooleanExpressionParser::
parse(ParseContext & context, bool greedy) const
{
    ParseContext::Revert_Token token(context);

    context.skip_whitespace();

    // Possibilities from here:
    // - UNARYOP EXPRESSION
    // - SEGMENT
    // - EXPRESSION BINARYOP EXPRESSION ....
    // - EXPRESSION
    
    // Try a unary operation
    {
        auto unary = getUnaryParser(context);
        if (unary) {
            //cerr << "*** UNARY ***" << endl;
            BoolExprPtr result = unary->parse(context, *this);
            if (result) {
                token.ignore();
                //cerr << "returning unary result " << result->print() << endl;
                return result;
            }
        }
    }

    // An expression could either start with a (, or be a segment or an expression or a function
    BoolExprPtr expr;
    if (context.match_literal('(')) {
        //cerr << "*** PARENTHESES ***" << endl;
        // The only possible production is an expression
        expr = parse(context);
        if (!expr)
            context.exception("expected expression");
        context.skip_whitespace();
        context.expect_literal(')');
    }
    if (!expr) {
        expr = parseFreeSegment(context);
    }
    if (!expr) {
        auto fn = getFunctionParser(context);
        //cerr << "after function parser " << *context << endl;

        if (fn) {
            BoolExprPtr result = fn->parse(context, *this);
            context.match_whitespace();
            context.expect_literal(')');
            context.match_whitespace();
            if (result) {
                token.ignore();
                //cerr << "after unary parsing " << *context << endl;
                //cerr << "returning unary result " << result->print() << endl;
                expr = result;
            }
        }
    }
    if (!expr) {
        context.exception("expected expression");
    }
    if (!greedy) {
        token.ignore();
        return expr;
    }
        
    context.skip_whitespace();

    //cerr << "*** BINARY ***" << endl;

    // Anything that comes next must be a binary operator
    if (!context || *context == ')') {
        token.ignore();
        return expr;
    }

    auto binParser = getBinaryParser(context);
    if (!binParser) {
        string op;
        context.match_text(op, " ()");
        context.exception("unknown operator " + op);
    }
    auto res = binParser->parse(expr, context, *this);
    token.ignore();
    return res;
}

BoolExprPtr
BooleanExpressionParser::
parse(const std::string & expression) const
{
    ParseContext context(expression, expression.c_str(),
                              expression.length());
    BoolExprPtr result = parse(context, true);
    context.skip_whitespace();
    if (!context.eof()) {
        context.exception("didn't match the whole expression: at " + string(1, *context));
    }
    return result;
}

} // namespace MLDB
