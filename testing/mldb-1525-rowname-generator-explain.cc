/** mldb-1225-rowname-generator-explain.cc
    Mathieu Marquis Bolduc, 25 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of sql expression transform method
*/

#include "mldb/sql/sql_expression.h"
#include "mldb/core/dataset.h"
#include "mldb/builtin/sub_dataset.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <tuple>


using namespace std;

using namespace MLDB;

#define CHECK_EQUAL_EXPR(val, expected) \
BOOST_CHECK_EQUAL(val, ExpressionValue(expected, Date()))

static MldbServer *notNull = (MldbServer *)0x000001;

BOOST_AUTO_TEST_CASE(test_explain)
{
    {
        SubDataset dataset(notNull, {});

        SqlBindingScope scope;

        {
            auto where = SqlExpression::parse("x != 2");
            auto generator = dataset.generateRowsWhere(scope, "", *where, 0, -1);
            BOOST_CHECK_EQUAL(generator.explain, "scan table filtering by where expression");
            BOOST_CHECK_EQUAL(generator.complexity, GenerateRowsWhereFunction::TABLESCAN);
        }

        {
            auto where = SqlExpression::parse("FALSE");
            auto generator = dataset.generateRowsWhere(scope, "", *where, 0, -1);
            BOOST_CHECK_EQUAL(generator.explain, "Return nothing as constant where expression doesn't evaluate true");
            BOOST_CHECK_EQUAL(generator.complexity, GenerateRowsWhereFunction::CONSTANT);
        }

        {
            auto where = SqlExpression::parse("x IS TRUE");
            auto generator = dataset.generateRowsWhere(scope, "", *where, 0, -1);
            BOOST_CHECK_EQUAL(generator.explain, "generate rows where var 'x' is true");
            BOOST_CHECK_EQUAL(generator.complexity, GenerateRowsWhereFunction::BETTER_THAN_TABLESCAN);
        }

        {
            auto where = SqlExpression::parse("x IS TRUE AND y IS NOT NULL");
            auto generator = dataset.generateRowsWhere(scope, "", *where, 0, -1);
            BOOST_CHECK_EQUAL(generator.explain, "set intersection for AND boolean(\"AND\",istype(column(\"x\"),\"true\",1),istype(column(\"y\"),\"null\",0))");
            BOOST_CHECK_EQUAL(generator.complexity, GenerateRowsWhereFunction::BETTER_THAN_TABLESCAN);
        }

    }

}
