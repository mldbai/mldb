/** sql_expression_test.cc
    Jeremy Barnes, 25 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Test of row expressions.
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


struct TestContext: public SqlRowScope {
    std::map<ColumnPath, ExpressionValue> vars;

    virtual ExpressionValue getVariable(const ColumnPath & columnName) const
    {
        auto it = vars.find(columnName);
        if (it == vars.end())
            return ExpressionValue();
        return it->second;
    }
};

TestContext createRow(const std::vector<std::pair<std::string, CellValue> > & vars)
{
    TestContext result;
    for (auto & v: vars) {
        result.vars[PathElement(v.first)] = ExpressionValue(v.second, Date());
    }
    return result;
}

struct TestBindingContext: public SqlBindingScope {
    TestBindingContext()
    {
    }

    ColumnGetter doGetColumn(const Utf8String & tableName,
                               const ColumnPath & columnName)
    {
        return {[=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    return storage = static_cast<const TestContext &>(context)
                        .getVariable(columnName);
                },
                std::make_shared<AtomValueInfo>()};
    }

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep)
    {
        GetAllColumnsOutput result;

        result.exec = [=] (const SqlRowScope & context, const VariableFilter & filter)
            -> ExpressionValue
            {
                const TestContext & testContext
                    = static_cast<const TestContext &>(context);
                std::vector<std::tuple<PathElement, ExpressionValue> > result;

                for (auto & v: testContext.vars) {
                    ColumnPath name = keep(v.first);
                    if (!name.empty())
                        result.emplace_back(name.toSimpleName(), v.second);
                }
                
                return std::move(result);
            };
        
        result.info = std::make_shared<UnknownRowValueInfo>();

        return result;
    }
};


BOOST_AUTO_TEST_CASE(test_column_expression)
{
    {
        // MLDB-483
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR (ORDER BY count(values()) DESC LIMIT 1000)");
    
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR (WHERE (rowCount() > 100) ORDER BY rowCount() DESC LIMIT 1000)");
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR (WHERE rowCount() > 100 ORDER BY rowCount() DESC LIMIT 1000)");
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse
            ("columnCount() AS numUsers");
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-1590
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR(ORDER BY count(values()) DESC LIMIT 1000)");
    
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR(WHERE (rowCount() > 100) ORDER BY rowCount()DESC LIMIT 1000)");
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse
            ("COLUMN EXPR(WHERE rowCount() > 100 ORDER BY rowCount() DESC LIMIT 1000)");
        cerr << parsed->print() << endl;
    }
}


BOOST_AUTO_TEST_CASE(test_simple_comparison)
{
    TestBindingContext context;

    {
        auto expr = SqlExpression::parse("1 = 1", "example1")->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x = 1", "example2");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x = y", "example3");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 1}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}, {"y", 1}}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x = y - 1", "example4");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 1}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}, {"y", 1}}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x = y + z + 1", "example5");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 1}, {"z", 2}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 4}, {"y", 1}, {"z", 2}}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("3 + 10 * 2", "example6");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 23);
    }

    {
        auto parsed = SqlExpression::parse("10 * 2 + 3", "example6b");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 23);
    }

    {
        auto parsed = SqlExpression::parse("10 * (2 + 3)", "example6c");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 50);
    }

    {
        auto parsed = SqlExpression::parse("x = 10 AND y = 3 AND NOT z = 4", "example7");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 10}, {"y", 3}, {"z", 2}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 10}, {"y", 4}, {"z", 2}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 10}, {"y", 3}, {"z", 4}}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x & y AND z", "example8");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 3}, {"z", 2}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 4}, {"z", 2}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 3}, {"z", 0}}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("pow(x, 2)", "example9");
        auto expr = parsed->bind(context);
    }

    {
        auto parsed = SqlExpression::parse("'hello' + ' ' + 'world'", "example10");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), "hello world");
    }

    {
        auto parsed = SqlExpression::parse("true");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), true);
    }

    {
        auto parsed = SqlExpression::parse("x >= y", "example11");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 3}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 1}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}, {"y", 0}}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("\"variable with space and !\" + 'my name is Bob'", "example12");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"variable with space and !", "Hello world, "}}), GET_LATEST), "Hello world, my name is Bob");
    }

    {
        auto parsed = SqlExpression::parse("{x + 1}", "example12");
        cerr << parsed->print() << endl;
        auto expr = parsed->bind(context);
        cerr << jsonEncode(expr) << endl;
        vector<tuple<PathElement, ExpressionValue> > expected;
        expected.emplace_back(PathElement("x + 1"), ExpressionValue(11, Date()));
        BOOST_CHECK_EQUAL(expr(createRow({{"x", 10}, {"y", 3}, {"z", 2}}), GET_LATEST),
                          ExpressionValue(expected));
    }

    {
        // MLDB-157
        auto parsed = SqlExpression::parse("x % y", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}, {"y", 2}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1.75}, {"y", 0.5}}), GET_LATEST), 0.25);
    }

    {
        auto parsed = SqlExpression::parse("x % y", "example14");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 9233899587298179420ULL}, {"y", 2}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 9233899587298179421ULL}, {"y", 2}}), GET_LATEST), 1);
    }

    {
        // MLDB-313
        auto parsed = SqlExpression::parse("pow(x,y)", "example15");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}, {"y", 2}}), GET_LATEST), 9);
    }

    {
        // MLDB-195
        auto parsed = SqlExpression::parse("2.2*\"Weight\"");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"Weight", 1.0}}), GET_LATEST), 2.2);
    }

    {
        // MLDB-195
        auto parsed = SqlExpression::parse("col1 IS NOT NULL OR true");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"col1", 1.0}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), true);
    }

    {
        // MLDB-195
        auto parsed = SqlExpression::parse("(col1 IS NOT NULL) OR true");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"col1", 1.0}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), true);
    }

    {
        // MLDB-195
        auto parsed = SqlExpression::parse("- - -(-x)");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1.0}}), GET_LATEST), 1.0);
    }

    {
        // MLDB-195
        auto parsed = SqlExpression::parse("x IS NOT NULL IS NOT NULL IS NOT NULL");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1.0}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), true);
    }

    {
        auto parsed = SqlExpression::parse("x-1");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x-(-1)");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 2);
    }

    {
        auto parsed = SqlExpression::parse("-(-1)");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse(Utf8String("'yé'"));
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), Utf8String("yé"));
    }

    {
        auto parsed = SqlExpression::parse(Utf8String("'y' + 'é'"));
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), Utf8String("yé"));
    }

    {
        auto parsed = SqlExpression::parse(Utf8String("'yée'"));
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), Utf8String("yée"));
    }

    {
        auto parsed = SqlExpression::parse(Utf8String("'yééééééée'"));
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), Utf8String("yééééééée"));
    }

    {
        auto parsed = SqlExpression::parse("\"é\"-1");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"é", 1}}), GET_LATEST), 0);
    }

    {
        // MLDB-495
        auto parsed = SqlExpression::parse("(col1 is not null) or (true and true)");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"col1", 1.0}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), true);
    }

    {
        // MLDB-503
        auto parsed = SqlExpression::parse("CASE x WHEN 1 THEN 'hello' ELSE 'world' END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "hello");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), "world");
    }

    {
        // MLDB-503
        auto parsed = SqlExpression::parse("CASE x WHEN 1 THEN 'hello' END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "hello");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), nullptr);
    }

    {
        // MLDB-503
        auto parsed = SqlExpression::parse("CASE x WHEN NULL THEN 'yes' ELSE 'no' END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "no");
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), "no");
    }

    {
        // MLDB-503
        auto parsed = SqlExpression::parse("CASE WHEN x = 1 THEN 'hello' ELSE 'world' END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "hello");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), "world");
    }

    {
        // MLDB-503
        auto parsed = SqlExpression::parse("CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "odd");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), "even");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}}), GET_LATEST), "odd");
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), "odd");
    }

    {
        // MLDB-503, with a nested case
        auto parsed = SqlExpression::parse("CASE CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END WHEN 'even' THEN 'good' ELSE CASE WHEN x = 3 THEN 'three' ELSE 'bad' END END");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), "bad");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), "good");
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}}), GET_LATEST), "three");
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), "bad");
    }

    {
        // MLDB-504
        auto parsed = SqlExpression::parse("x BETWEEN 1 AND 2");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), false);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}}), GET_LATEST), false);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), nullptr);
    }

    {
        // MLDB-504
        auto parsed = SqlExpression::parse("x NOT BETWEEN 1 AND 2");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), false);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 0}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 3}}), GET_LATEST), true);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), nullptr);
    }

    {
        // MLDB-514
        auto parsed = SqlExpression::parse("NULL = NULL");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), nullptr);
        BOOST_CHECK_EQUAL(expr(createRow({}), GET_LATEST).isTrue(), false);
    }

    {
        // MLDB-514
        auto parsed = SqlExpression::parse("NULL != NULL");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), nullptr);
        BOOST_CHECK_EQUAL(expr(createRow({}), GET_LATEST).isTrue(), false);
    }

    {
        // MLDB-514
        auto parsed = SqlExpression::parse("NULL < NULL");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), nullptr);
        BOOST_CHECK_EQUAL(expr(createRow({}), GET_LATEST).isTrue(), false);
    }

    // Not sure if it should be an error or not... currently not accepted
    //{
    //    auto parsed = SqlExpression::parse("x---1");
    //    auto expr = parsed->bind(context);
    //    CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 0);
    //}
    //
    //{
    //    auto parsed = SqlExpression::parse("x----1");
    //    auto expr = parsed->bind(context);
    //    CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 2);
    //}
}

BOOST_AUTO_TEST_CASE(test_is_null)
{
    TestBindingContext context;

    {
        auto parsed = SqlExpression::parse("1 IS NOT NULL", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("1 IS NULL", "example14");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT NULL", "example15");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS NULL", "example16");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"x", 1}}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }
}

BOOST_AUTO_TEST_CASE(test_is_true_false)
{
    TestBindingContext context;

    {
        auto parsed = SqlExpression::parse("1 IS NOT TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("0 IS NOT TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("NULL IS NOT TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("1 IS NOT FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("0 IS NOT FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("NULL IS NOT FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT TRUE", "example14");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 0 } }), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT FALSE", "example14");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 0 } }), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("1 IS TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("0 IS TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("NULL IS TRUE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("1 IS FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("0 IS FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("NULL IS FALSE", "example13");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS TRUE", "example14");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 0 } }), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS FALSE", "example15");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 0 } }), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS NUMBER", "example16");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT NUMBER", "example17");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS INTEGER", "example18");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT INTEGER", "example19");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS STRING", "example20");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.1 } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "" } }), GET_LATEST), 1);
    }

    {
        auto parsed = SqlExpression::parse("x IS NOT STRING", "example21");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "" } }), GET_LATEST), 0);
    }

    {
        auto parsed = SqlExpression::parse("x iS NoT StRiNg", "example22");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.0 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", 1.1 } }), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "1.0" } }), GET_LATEST), 0);
        CHECK_EQUAL_EXPR(expr(createRow({ { "x", "" } }), GET_LATEST), 0);
    }

    {
        // MLDB-759
        auto parsed = SqlExpression::parse("count % 5");
        auto expr = parsed->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({ { "count", 3 }}), GET_LATEST), 3);
    }
}

// MLDB-599
BOOST_AUTO_TEST_CASE(test_implicit_cast)
{

    auto run = [] (const std::string & str) -> CellValue
        {
            TestBindingContext context;
            auto parsed = SqlExpression::parse(str);
            auto expr = parsed->bind(context);
            return expr(createRow({}), GET_LATEST).getAtom();
        };
    
    BOOST_CHECK_EQUAL(run("implicit_cast('1')"), 1);
    BOOST_CHECK_EQUAL(run("implicit_cast(1)"), 1);
    BOOST_CHECK_EQUAL(run("implicit_cast(null) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("implicit_cast('1.0')"), 1);
    BOOST_CHECK_EQUAL(run("implicit_cast('1e15')"), 1e15);
    BOOST_CHECK_EQUAL(run("implicit_cast('hello')"), "hello");
    BOOST_CHECK_EQUAL(run("implicit_cast('') IS NULL"), true);
    BOOST_CHECK_EQUAL(run("implicit_cast('Inf') IS NUMBER"), true);
    BOOST_CHECK_EQUAL(run("implicit_cast('NaN') IS NUMBER"), true);
}

// CAST expression
BOOST_AUTO_TEST_CASE(test_explicit_cast)
{

    auto run = [] (const std::string & str) -> CellValue
        {
            TestBindingContext context;
            auto parsed = SqlExpression::parse(str);
            auto expr = parsed->bind(context);
            return expr(createRow({}), GET_LATEST).getAtom();
        };
    
    BOOST_CHECK_EQUAL(run("CAST (1 AS string)"), "1");
    BOOST_CHECK_EQUAL(run("CAST (NULL AS string) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("CAST ('hello' AS string)"), "hello");
    BOOST_CHECK_EQUAL(run("CAST (1.0 AS string)"), "1");
    BOOST_CHECK_EQUAL(run("CAST (1.1 AS string)"), "1.1");
    BOOST_CHECK_EQUAL(run("CAST (1.1000 AS string)"), "1.1");
    BOOST_CHECK_EQUAL(run("CAST (Inf AS string)"), "Infinity");
    BOOST_CHECK_EQUAL(run("CAST (NaN AS string)"), "NaN");
    BOOST_CHECK_EQUAL(run("CAST (NULL AS integer) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("CAST ('1' AS integer)"), 1);
    BOOST_CHECK_EQUAL(run("CAST ('1.1' AS integer)"), 1);
    BOOST_CHECK_EQUAL(run("CAST ('NaN' AS integer) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("CAST ('-1' AS integer)"), -1);
    BOOST_CHECK_EQUAL(run("CAST ('-1' AS INTEGER)"), -1);  // MLDB-722
    BOOST_CHECK_EQUAL(run("CAST ('hello' AS integer) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("CAST ('' AS integer) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("CAST (1+2AS integer)"), 3);
    BOOST_CHECK_EQUAL(run("CAST ('true' AS boolean)"), true);
    BOOST_CHECK_EQUAL(run("CAST ('false' AS boolean)"), false);
    BOOST_CHECK_EQUAL(run("CAST ('1' AS boolean)"), true);
    BOOST_CHECK_EQUAL(run("CAST (1 AS boolean)"), true);
    BOOST_CHECK_EQUAL(run("CAST (true AS boolean)"), true);
    BOOST_CHECK_EQUAL(run("CAST (false AS boolean)"), false);
    BOOST_CHECK_EQUAL(run("CAST ('0' AS boolean)"), false);
    BOOST_CHECK_EQUAL(run("CAST (0 AS timestamp)"), Date());
    BOOST_CHECK_EQUAL(run("CAST ('1971-01-01T01:03:03' AS timestamp)"),
                      Date(1971, 1, 1, 1, 3, 3));
    //MLDB_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(run("CAST (123 AS \"\")"), std::exception);
}

// MLDB-635 truth tables
BOOST_AUTO_TEST_CASE(test_truth_tables)
{

    auto run = [] (const std::string & str) -> CellValue
        {
            TestBindingContext context;
            auto parsed = SqlExpression::parse(str);
            auto expr = parsed->bind(context);
            return expr(createRow({}), GET_LATEST).getAtom();
        };
    
    BOOST_CHECK_EQUAL(run("(TRUE AND TRUE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(TRUE AND FALSE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(TRUE AND NULL) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("(NULL AND TRUE) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("(NULL AND FALSE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(NULL AND NULL) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("(FALSE AND TRUE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(FALSE AND FALSE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(FALSE AND NULL) IS FALSE"), true);

    BOOST_CHECK_EQUAL(run("(TRUE OR TRUE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(TRUE OR FALSE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(TRUE OR NULL) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(NULL OR TRUE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(NULL OR FALSE) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("(NULL OR NULL) IS NULL"), true);
    BOOST_CHECK_EQUAL(run("(FALSE OR TRUE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(FALSE OR FALSE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(FALSE OR NULL) IS NULL"), true);

    BOOST_CHECK_EQUAL(run("(NOT TRUE) IS FALSE"), true);
    BOOST_CHECK_EQUAL(run("(NOT FALSE) IS TRUE"), true);
    BOOST_CHECK_EQUAL(run("(NOT NULL) IS NULL"), true);
}

// MLDB-649
BOOST_AUTO_TEST_CASE(test_regex)
{

    auto run = [] (const Utf8String & str)
        {
            TestBindingContext context;
            auto parsed = SqlExpression::parse(str);
            auto expr = parsed->bind(context);
            return expr(createRow({}), GET_LATEST).getAtom();
        };
    
    BOOST_CHECK_EQUAL(run(Utf8String("regex_replace('hello world', 'world', 'bob')")),
                      "hello bob");
    BOOST_CHECK_EQUAL(run(Utf8String("regex_replace('héllo world', 'world', 'évé')")),
                      Utf8String("héllo évé"));
    BOOST_CHECK_EQUAL(run(Utf8String("regex_replace('héllo évé', 'évé', 'bob')")),
                      Utf8String("héllo bob"));
    BOOST_CHECK_EQUAL(run(Utf8String("regex_match('héllo évé', '.*évé')")),
                      true);
    BOOST_CHECK_EQUAL(run(Utf8String("regex_match('héllo évé', 'évé')")),
                      false);
    BOOST_CHECK_EQUAL(run(Utf8String("regex_search('héllo évé', 'évé')")),
                      true);
    BOOST_CHECK_EQUAL(run(Utf8String("regex_search('héllo évé', 'éve')")),
                      false);
    BOOST_CHECK_EQUAL(run(Utf8String("regex_search(NULL, 'évé')")),
                      CellValue());
}

// MLDB-686
BOOST_AUTO_TEST_CASE(test_timestamps)
{

    auto run = [] (const std::string & str)
        {
            TestBindingContext context;
            auto parsed = SqlExpression::parse(str);
            auto expr = parsed->bind(context);
            return expr(createRow({}), GET_LATEST).getAtom();
        };
    
    BOOST_CHECK_EQUAL(run("latest_timestamp(1 @ '2015-01-01T00:00:00Z')"),
                      Date(2015,1,1,0,0,0));
}

BOOST_AUTO_TEST_CASE(test_result_variable_expressions)
{
    {
        auto parsed = SqlRowExpression::parse("*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (bonus)");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* eXcluDing (bonus)");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (bonus*)");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("prefix* EXCLUDING (prefix*)");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

#if 0
    {
        // prefix of expression and exclusions doesn't match
        BOOST_CHECK_THROW(SqlRowExpression::parse("prefix* EXCLUDING bonus"),
                          std::exception);
    }

    {
        // prefix of expression and exclusions doesn't match
        BOOST_CHECK_THROW(SqlRowExpression::parse("prefix* EXCLUDING bonus*"),
                          std::exception);
    }
#endif

    {
        auto parsed = SqlRowExpression::parse("prefix* EXCLUDING (bonus) AS newprefix*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (bonus) AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (bonus*) AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (bonus) AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING(bonus) AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(SqlRowExpression::parse("* EXCLUDING bonus AS myvar*"),
                          MLDB::Exception);
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING () AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        auto parsed = SqlRowExpression::parse("* EXCLUDING (foo, bar*) AS myvar*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // ambiguity between * operator and wildcard
        auto parsed = SqlRowExpression::parse("o*2 AS p");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-195
        auto parsed = SqlRowExpression::parse("2.2*\"Weight\"");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-200
        auto parsed = SqlRowExpression::parse("2.2");
        ExcAssert(parsed);
        BOOST_CHECK_EQUAL(parsed->print(), "computed(\"\"2.2\"\",constant([2.2,\"-Inf\"]))");
    }

    {
        // MLDB-200
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(SqlRowExpression::parse("2.2*"),
                          MLDB::Exception);
    }

    {
        // MLDB-200
        auto parsed = SqlRowExpression::parse("\"2\".\"2\"");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-200
        auto parsed = SqlRowExpression::parse("\"2\".\"2\"*");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-201
        auto parsed = SqlRowExpression::parse
            ("\"bonus\"({*})");
        ExcAssert(parsed);
        cerr << parsed->print() << endl;
    }

    {
        // MLDB-826   
        auto parsed = SqlRowExpression::parse
            ("\"bonus\" ()");

        ExcAssert(parsed);

        parsed = SqlRowExpression::parse
            ("\"bonus\" ()[{*}]");

        ExcAssert(parsed);   

        parsed = SqlRowExpression::parse
            ("\"bonus\" ()[out]");

        ExcAssert(parsed);        

         parsed = SqlRowExpression::parse
            ("\"bonus\" ()[{out as alias}]");

        ExcAssert(parsed);          

        auto parsed2 = SqlExpression::parse
            ("classifier({{* EXCLUDING (adventuretime)} AS features})[score]");
        ExcAssert(parsed);

        parsed2 = SqlExpression::parse
            ("classifier({features: {* EXCLUDING (a)}, label: x AND y})[score]");
        ExcAssert(parsed);

        parsed2 = SqlExpression::parse
            ("classifyFunctionglz({{ * EXCLUDING (label) } AS features})[score]");

        ExcAssert(parsed2);


    }
}

BOOST_AUTO_TEST_CASE(test_select_statement_parse)
{
    TestBindingContext context;
    {
        auto statement = SelectStatement::parse("SELECT * FROM ds");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
    }

    {
        auto statement = SelectStatement::parse("SELECT i WHERE i = 0");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);

        auto expr = statement.where->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"i", 0}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"i", 1}}), GET_LATEST), 0);
    }

    {
        auto statement = SelectStatement::parse(
                "SELECT a, b ORDER BY b, a LIMIT 10   OFFSET 20");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 2);
        BOOST_CHECK_EQUAL(statement.orderBy.clauses.size(), 2);
        BOOST_CHECK_EQUAL(statement.limit, size_t(10));
        BOOST_CHECK_EQUAL(statement.offset, size_t(20));
    }

    {
        auto statement = SelectStatement::parse(
                "sElEcT a WhErE a = 'select' lImIt 10");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_CHECK_EQUAL(statement.limit, size_t(10));

        auto expr = statement.where->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "select"}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "where"}}), GET_LATEST), 0);

    }

    {
        auto statement = SelectStatement::parse(
                "select a where a = 'SELECT '' \"WHERE\"' limit 10");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_CHECK_EQUAL(statement.limit, size_t(10));

        auto expr = statement.where->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "SELECT ' \"WHERE\""}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "WHERE"}}), GET_LATEST), 0);
    }

    {
        auto statement = SelectStatement::parse(
                "select a where \"a\" = 'boo' limit 10");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_CHECK_EQUAL(statement.limit, size_t(10));

        auto expr = statement.where->bind(context);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "boo"}}), GET_LATEST), 1);
        CHECK_EQUAL_EXPR(expr(createRow({{"a", "goo"}}), GET_LATEST), 0);
    }

    // MLDB-314 - from clause
    {
        auto statement = SelectStatement::parse("select * from   abc  limit 10");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_REQUIRE(statement.from);
        BOOST_CHECK_EQUAL(statement.from->surface, "abc");
        BOOST_CHECK_EQUAL(statement.limit, 10);
    }

    // MLDB-646
    {
        auto statement = SelectStatement::parse("select * from bob_the_select");
        BOOST_REQUIRE(statement.from);
        BOOST_CHECK_EQUAL(statement.from->surface, "bob_the_select");
    }

    // MLDB-646
    {
        auto statement = SelectStatement::parse("select * from select_all_the_things");
        BOOST_REQUIRE(statement.from);
        BOOST_CHECK_EQUAL(statement.from->surface, "select_all_the_things");
    }

    {
        Set_Trace_Exceptions trace(false);

        BOOST_CHECK_THROW(SelectStatement::parse("select * SELECT *"), MLDB::Exception);

        BOOST_CHECK_THROW(SelectStatement::parse("where a = 0 where b = 0"), MLDB::Exception);
        BOOST_CHECK_THROW(SelectStatement::parse("having a = 0 having b = 0"), MLDB::Exception);

        BOOST_CHECK_THROW(SelectStatement::parse("group by a group by b"), MLDB::Exception);
        BOOST_CHECK_THROW(SelectStatement::parse("order by a order by b"), MLDB::Exception);

        BOOST_CHECK_THROW(SelectStatement::parse("limit 10 limit 20"), MLDB::Exception);
        BOOST_CHECK_THROW(SelectStatement::parse("offset 10 offset 20"), MLDB::Exception);
    }

    {
        // MLDB-637
        auto statement = SelectStatement::parse("select 1 from table");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_CHECK_EQUAL(MLDB::type_name(*statement.select.clauses[0].get()),
                          "MLDB::NamedColumnExpression");
        auto cast = dynamic_cast<NamedColumnExpression *>(statement.select.clauses[0].get());
        BOOST_CHECK_EQUAL(cast->alias, ColumnPath("1"));
    }

    {
        // MLDB-759
        auto statement = SelectStatement::parse("select * from table group by count % 5");
        BOOST_CHECK_EQUAL(statement.select.clauses.size(), 1);
        BOOST_CHECK_EQUAL(statement.groupBy.clauses.size(), 1);
        BOOST_CHECK_EQUAL(statement.groupBy.clauses[0]->surface, "count % 5");
    }

    {
        // MLDB-829
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from (table)"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from table1 join (table2)"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from (table1) join table2"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from (table1 join table2)"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from (table1 join (table2))"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select * from (table1 join (table3 join table2))"));
    }

    {
        // MLDB-832
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select {a,b} as o from table"));
        BOOST_CHECK_NO_THROW(SelectStatement::parse("select {a,b} as o from table"));
    }

    {
         // MLDB-868
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(SelectStatement::parse("select * from tableç"),
                          std::exception);
        auto statement = SelectStatement::parse("select * from \"tableç\"");
        BOOST_CHECK_EQUAL(statement.from->getAs(), Utf8String("tableç"));
    }

    {
        // MLDB-909
        auto statement = SelectStatement::parse("select * from table when value_timestamp() between '2014-01-02T00:00:01' and '2015-01-02'");
        BOOST_CHECK_EQUAL(statement.when.getType(), "when");
        BOOST_CHECK_EQUAL(statement.when.when->getType(), "between");
        BOOST_CHECK_EQUAL(statement.when.surface, "value_timestamp() between '2014-01-02T00:00:01' and '2015-01-02'");
        BOOST_CHECK_EQUAL(statement.when.getChildren().size(), 1); // inner self
        BOOST_CHECK_EQUAL(statement.when.when->getChildren().size(), 3); // lhs, rhs, value

        // can't be in two non-overlapping ranges
        statement = SelectStatement::parse("select * from table when "
                                           "value_timestamp() between '2014-01-02' and '2014-02-02' AND "
                                           "value_timestamp() between '2015-01-02' and '2015-02-02'");

        // overlapping ranges
        statement = SelectStatement::parse("select * from table when "
                                           "value_timestamp() between '2014-01-02' and '2014-03-02' AND "
                                           "value_timestamp() between '2014-02-02' and '2014-04-02'");

        statement = SelectStatement::parse("select * from table when value_timestamp() > '2014-01-02'");
        BOOST_CHECK_EQUAL(statement.when.getType(), "when");
        BOOST_CHECK_EQUAL(statement.when.when->getType(), "compare");
        BOOST_CHECK_EQUAL(statement.when.surface, "value_timestamp() > '2014-01-02'");
        BOOST_CHECK_EQUAL(statement.when.when->getChildren().size(), 2); // lhs and rhs

        statement = SelectStatement::parse("select * from table when value_timestamp() > '2014-01-02' AND value_timestamp() < '2015-01-02'");
        BOOST_CHECK_EQUAL(statement.when.getType(), "when");
        BOOST_CHECK_EQUAL(statement.when.when->getType(), "boolean");
        BOOST_CHECK_EQUAL(statement.when.surface, "value_timestamp() > '2014-01-02' AND value_timestamp() < '2015-01-02'");
        BOOST_CHECK_EQUAL(statement.when.when->getChildren().size(), 2); // lhs and rhs
    }   

    {
        // MLDB-963
        auto statement = SelectStatement::parse("select * from table when true");
        BOOST_CHECK_EQUAL(statement.when.getType(), "when");
        BOOST_CHECK_EQUAL(statement.when.when->getType(), "constant");
        BOOST_CHECK_EQUAL(statement.when.surface, "true");
    }

    {
        // MLDB-1022
        auto isTupleDependent = [&](const std::string & expression) {
            SqlExpressionWhenScope whenContext(context);
            auto when = WhenExpression::parse(expression);
            when.bind(whenContext);
            return whenContext.isTupleDependent;
        };
        
        BOOST_CHECK(!isTupleDependent("true"));
        BOOST_CHECK(isTupleDependent("value_timestamp() < TIMESTAMP '2014-01-01'"));
        BOOST_CHECK(!isTupleDependent("latest_timestamp(a) BETWEEN TIMESTAMP '2015-01-10' AND TIMESTAMP '2016-10-01'"));
        BOOST_CHECK(isTupleDependent("value_timestamp() < TIMESTAMP '2014-01-09' AND latest_timestamp(a) BETWEEN TIMESTAMP '2015-01-10' AND TIMESTAMP '2016-10-01'"));
        BOOST_CHECK(!isTupleDependent("latest_timestamp(a) < TIMESTAMP '2015-01-09'"));
    }

    {
        // MLDB-1434
        auto statement = SelectStatement::parse("select rowName(),a.b,a.c from table");
    }
}

// MLDB-1002: x AS y <--> y: x, x* AS y* <--> y*: x*

BOOST_AUTO_TEST_CASE(test_colon_as)
{
    auto parsed = SelectExpression::parse("x: 10, y: 20");
    cerr << parsed.print() << endl;
    BOOST_CHECK_EQUAL(parsed.print(),
                      "[computed(\"x\",constant([10,\"-Inf\"])), computed(\"y\",constant([20,\"-Inf\"]))]");

    // Check for operator precedence working as expected
    parsed = SelectExpression::parse("x: 10 + 1, y: 20 * 2");
    cerr << parsed.print() << endl;
    BOOST_CHECK_EQUAL(parsed.print(), 
                      "[computed(\"x\",arith(\"+\",constant([10,\"-Inf\"]),constant([1,\"-Inf\"]))), computed(\"y\",arith(\"*\",constant([20,\"-Inf\"]),constant([2,\"-Inf\"])))]");

    parsed = SelectExpression::parse("x*: y*");
    cerr << parsed.print() << endl;
    BOOST_CHECK_EQUAL(parsed.print(),
                      "[columns(\"x\",\"y\",[])]");
    parsed = SelectExpression::parse("x : 10, y : 20");
    cerr << parsed.print() << endl;

    parsed = SelectExpression::parse("x:10,y:20");
    cerr << parsed.print() << endl;
}

BOOST_AUTO_TEST_CASE(test_alignment)
{
    // Not really a test, but helpful for developers...
    cerr << "sizeof(CellValue) = " << sizeof(CellValue) << endl;
    cerr << "alignof(CellValue) = " << alignof(CellValue) << endl;
    cerr << "sizeof(ExpressionValue) = " << sizeof(ExpressionValue) << endl;
    cerr << "alignof(ExpressionValue) = " << alignof(ExpressionValue) << endl;
    cerr << "sizeof(PathElement) = " << sizeof(PathElement) << endl;
    cerr << "alignof(PathElement) = " << alignof(PathElement) << endl;
    cerr << "sizeof(Path) = " << sizeof(Path) << endl;
    cerr << "alignof(Path) = " << alignof(Path) << endl;
    cerr << "sizeof(Utf8String) = " << sizeof(Utf8String) << endl;
    cerr << "alignof(Utf8String) = " << alignof(Utf8String) << endl;
    cerr << "sizeof(std::string) = " << sizeof(std::string) << endl;
    cerr << "alignof(std::string) = " << alignof(std::string) << endl;

    BOOST_CHECK_EQUAL(sizeof(ExpressionValue), 32);
}
