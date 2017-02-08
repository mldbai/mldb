/** MLDB-1559-transform-method.cc
    Mathieu Marquis Bolduc, 12 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of sql expression transform method
*/

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/arch/exception_handler.h"
#include "server/dataset_context.h"
#include "mldb/types/value_description.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <tuple>


using namespace std;

using namespace MLDB;

#define CHECK_EQUAL_EXPR(val, expected) \
BOOST_CHECK_EQUAL(val, ExpressionValue(expected, Date()))

template <class T>
void TestExpression(const T& expression, int expected)
{
    int count = 0;

    auto printBefore = expression.print();

    cerr << "testing " << printBefore << endl;

    TransformArgs onChild = [&] (std::vector<std::shared_ptr<SqlExpression> > args)
    -> std::vector<std::shared_ptr<SqlExpression> >
        {     
            count += args.size();

            for (auto& a : args) {

                auto var = std::dynamic_pointer_cast<ReadColumnExpression>(a);
                if (var) {

                    a = std::make_shared<ReadColumnExpression>(PathElement("replaced"));
                }

                a = a->transform(onChild);
            }

            return args;
        };

    expression.transform(onChild);
    BOOST_CHECK_EQUAL(count, expected);

    auto printAfter = expression.print();

    BOOST_CHECK_EQUAL(printBefore, printAfter);
}

template <class T>
void TestExpression(const std::shared_ptr<T> expression, int expected)
{
    return TestExpression(*expression, expected);
}

BOOST_AUTO_TEST_CASE(test_transform)
{
    {
        OrderByExpression orderBy = OrderByExpression::parse("a, b");
        TestExpression(orderBy, 2);
    }

    //note: The total should be the number of edges in the evaluation tree
    {
        OrderByExpression orderBy = OrderByExpression::parse("a, b+c");
        TestExpression(orderBy, 4);
    }

    {
        TupleExpression tuple = TupleExpression::parse("a, b");
        TestExpression(tuple, 2);
    }

    {
        TupleExpression tuple = TupleExpression::parse("a, b+c");
        TestExpression(tuple, 4);
    }

    {
        auto comparisonExpr = SqlExpression::parse("a > c");
        TestExpression(comparisonExpr, 2);
    }

    {
        auto comparisonExpr = SqlExpression::parse("a > (b+c)");
        TestExpression(comparisonExpr, 4);
    }

    {
        auto arithExpr = SqlExpression::parse("a + b");
        TestExpression(arithExpr, 2);
    }

    {
        auto arithExpr = SqlExpression::parse("a + b + c");
        TestExpression(arithExpr, 4);
    }

    {
        auto bitwiseExpr = SqlExpression::parse("a & b");
        TestExpression(bitwiseExpr, 2);
    }

    {
        auto bitwiseExpr = SqlExpression::parse("a & b | c");
        TestExpression(bitwiseExpr, 4);
    }

    // SelectExpression::transform() not implemented
  /*  {
        auto withinExpr = SqlExpression::parse("{a,b}");
        TestExpression(withinExpr, 2);
    }

    {
        auto withinExpr = SqlExpression::parse("{a,b+c}");
        TestExpression(withinExpr, 4);
    }*/


    {
        auto embeddingExpr = SqlExpression::parse("[a,b]");
        TestExpression(embeddingExpr, 2);
    }

    {
        auto embeddingExpr = SqlExpression::parse("[a,b+c]");
        TestExpression(embeddingExpr, 4);
    }


    {
        auto booleanExpr = SqlExpression::parse("a OR b");
        TestExpression(booleanExpr, 2);
    }

    {
        auto booleanExpr = SqlExpression::parse("a OR b AND c");
        TestExpression(booleanExpr, 4);
    }

    {
        auto functionExpr = SqlExpression::parse("f(a)[c]");
        TestExpression(functionExpr, 3);
    }

    {
        auto functionExpr = SqlExpression::parse("f(a+b)[c+e]");
        TestExpression(functionExpr, 7);
    }

    {
        auto functionExpr = SqlExpression::parse("f(a,b)");
        TestExpression(functionExpr, 2);
    }

    {
        auto functionExpr = SqlExpression::parse("f(a,b+c)");
        TestExpression(functionExpr, 4);
    }

    {
        auto caseExpr = SqlExpression::parse("CASE WHEN a THEN b ELSE c END");
        TestExpression(caseExpr, 3);
    }

    {
        auto caseExpr = SqlExpression::parse("CASE WHEN a+b THEN b+c ELSE c+d END");
        TestExpression(caseExpr, 9);
    }

    {
        auto caseExpr = SqlExpression::parse("c BETWEEN a AND b");
        TestExpression(caseExpr, 3);
    }

    {
        auto caseExpr = SqlExpression::parse("c+d BETWEEN a+b AND b+c");
        TestExpression(caseExpr, 9);
    }

    {
        auto inExpr = SqlExpression::parse("a IN (b,c)");
        TestExpression(inExpr, 3);
    }

    {
        auto inExpr = SqlExpression::parse("(a + b) IN (b+c,c+d)");
        TestExpression(inExpr, 9);
    }

    //Table Expressions have no transform

   /* {
        auto inExpr = SqlExpression::parse("a IN (SELECT x,y)");
        TestExpression(inExpr, 3);
    }

    {
        auto inExpr = SqlExpression::parse("(a + b) IN (SELECT x+b,y+c)");
        TestExpression(inExpr, 9);
    }*/

    {
        auto inExpr = SqlExpression::parse("a IN (KEYS OF b)");
        TestExpression(inExpr, 2);
    }

    {
        auto inExpr = SqlExpression::parse("a + b IN (KEYS OF b)");
        TestExpression(inExpr, 4);
    }

    {
        auto likeExpr = SqlExpression::parse("a LIKE b");
        TestExpression(likeExpr, 2);
    }

    {
        auto likeExpr = SqlExpression::parse("(a + b) LIKE (c + d)");
        TestExpression(likeExpr, 6);
    }

    {
        auto castExpr = SqlExpression::parse("CAST (a AS int)");
        TestExpression(castExpr, 1);
    }

    {
        auto castExpr = SqlExpression::parse("CAST ((a + b) AS int)");
        TestExpression(castExpr, 3);
    }
}
