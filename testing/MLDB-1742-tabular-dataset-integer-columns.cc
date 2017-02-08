/* MLDB-1360-sparse-mutable-multithreaded-insert.cc
   Jeremy Barnes, 20 March 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include "mldb/plugins/frozen_column.h"
#include "mldb/plugins/tabular_dataset_column.h"
#include "mldb/server/mldb_server.h"
#include "mldb/arch/timers.h"

using namespace std;

using namespace MLDB;

std::shared_ptr<FrozenColumn>
freezeAndTest(const std::vector<CellValue> & cells)
{
    TabularDatasetColumn col;

    BOOST_CHECK_EQUAL(col.columnTypes.numStrings, 0);

    for (size_t i = 0;  i < cells.size();  ++i) {
        col.add(i, cells[i]);
    }

    ColumnFreezeParameters params;
    std::shared_ptr<FrozenColumn> frozen = col.freeze(params);

    ExcAssertEqual(frozen->size(), cells.size());

    for (size_t i = 0;  i < cells.size();  ++i) {
        BOOST_REQUIRE_EQUAL(frozen->get(i), cells[i]);
    }
    
    return frozen;
}

// Simple positive and zero integers
BOOST_AUTO_TEST_CASE( test_frozen_ints_only )
{
    std::vector<CellValue> vals;
    for (unsigned i = 0;  i < 1000;  ++i) {
        vals.push_back(i);
    }

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::IntegerFrozenColumn");
}

// Simple positive and null integers
BOOST_AUTO_TEST_CASE( test_frozen_ints_and_nulls_only )
{
    std::vector<CellValue> vals;
    for (int i = 0;  i < 1000;  ++i) {
        vals.push_back(i);
    }
    vals.emplace_back();  // add a null

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::IntegerFrozenColumn");
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_frozen_neg_ints )
{
    std::vector<CellValue> vals;
    for (int i = 0;  i < 1000;  ++i) {
        vals.push_back(i);
        vals.push_back(-i);
    }

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                   "MLDB::IntegerFrozenColumn");
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_frozen_neg_ints_and_nulls_only )
{
    std::vector<CellValue> vals;
    for (int i = 0;  i < 1000;  ++i) {
        vals.push_back(i);
        vals.push_back(-i);
    }
    vals.emplace_back();  // add a null

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                   "MLDB::IntegerFrozenColumn");
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_frozen_neg_only_ints_and_nulls )
{
    std::vector<CellValue> vals;
    for (int i = 0;  i < 1000;  ++i) {
        vals.push_back(-i-1);
    }
    vals.emplace_back();  // add a null

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                   "MLDB::IntegerFrozenColumn");
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_large_positive_ints )
{
    std::vector<CellValue> vals;

    // Check for large integers (we have uint64_t so we end up with overflow
    for (uint64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(-i);
    }

    freezeAndTest(vals);
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_large_positive_ints_and_nulls )
{
    std::vector<CellValue> vals;

    // Check for large integers (we have uint64_t so we end up with overflow
    for (uint64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(-i);
    }
    vals.emplace_back();  // add a null

    freezeAndTest(vals);
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_big_range )
{
    std::vector<CellValue> vals;

    // Check for large integers (we have uint64_t so we end up with overflow
    for (uint64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(-i);
    }
    vals.emplace_back();  // add a null

    freezeAndTest(vals);
}

// Simple negative, positive and null integers
BOOST_AUTO_TEST_CASE( test_big_pos_neg_range )
{
    std::vector<CellValue> vals;

    // Check for large integers (we have uint64_t so we end up with overflow
    for (int64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(std::numeric_limits<int64_t>::min() + i);
        vals.push_back(std::numeric_limits<int64_t>::max() - i);
    }

    auto frozen = freezeAndTest(vals);

    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                   "MLDB::IntegerFrozenColumn");
}

// Full range and nulls.  Not enough 64 bit integers to represent all values.
BOOST_AUTO_TEST_CASE( test_big_pos_neg_range_and_nulls )
{
    std::vector<CellValue> vals;

    // Check for large integers (we have uint64_t so we end up with overflow
    for (int64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(std::numeric_limits<int64_t>::min() + i);
        vals.push_back(std::numeric_limits<int64_t>::max() - i);
    }
    vals.emplace_back();

    freezeAndTest(vals);
}
