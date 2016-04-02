/** expression_value_test.cc
    Jeremy Barnes, 1 April 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Tests for the ExpressionValue class.
*/

#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/http/http_exception.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE( test_size )
{
    BOOST_CHECK_EQUAL(sizeof(ExpressionValue), 32);
}

BOOST_AUTO_TEST_CASE( test_get_embedding_atom )
{
    CellValue atom("hello");
    Date ts;

    ExpressionValue val(atom, ts);

    {
        /// Test that extracting from an atom throws as it's not an embedding
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(val.getEmbedding(nullptr, 0), HttpReturnException);
    }
}

BOOST_AUTO_TEST_CASE( test_get_embedding_row )
{
    Date ts;

    RowValue row;
    row.emplace_back("a", 1, ts);
    row.emplace_back("b", 2, ts);
        
    ExpressionValue val2(row);

    ColumnName cols[2] = { "a", "b" };

    auto dist = val2.getEmbedding(cols, 2);

    ML::distribution<double> expected{1, 2};

    BOOST_CHECK_EQUAL(dist, expected);

    ColumnName cols2[2] = { "b", "a" };

    dist = val2.getEmbedding(cols2, 2);

    ML::distribution<double> expected2{2, 1};

    BOOST_CHECK_EQUAL(dist, expected2);
}
