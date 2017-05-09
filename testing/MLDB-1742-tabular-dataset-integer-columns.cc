/* MLDB-1742-tabular-dataset-integer-columns.cc
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
    MemorySerializer serializer;

    BOOST_CHECK_EQUAL(col.columnTypes.numStrings, 0);

    for (size_t i = 0;  i < cells.size();  ++i) {
        col.add(i, cells[i]);
    }

    ColumnFreezeParameters params;
    std::shared_ptr<FrozenColumn> frozen = col.freeze(serializer, params);

    cerr << "testing " << MLDB::type_name(*frozen) << endl;

    ExcAssertEqual(frozen->size(), cells.size());

    for (size_t i = 0;  i < cells.size();  ++i) {
        BOOST_REQUIRE_EQUAL(frozen->get(i), cells[i]);
    }

    {
        std::vector<CellValue> outVals(cells.size());
        std::vector<int8_t> done(cells.size());
        size_t numDone = 0;

        auto onEntry = [&] (int64_t rowNum, CellValue val)
            {
                BOOST_CHECK_GE(rowNum, 0);
                BOOST_CHECK_LT(rowNum, cells.size());
                BOOST_CHECK_EQUAL(done[rowNum], false);
                done[rowNum] = true;
                outVals[rowNum] = std::move(val);
                ++numDone;
                return true;
            };
        
        frozen->forEachDense(onEntry);

        BOOST_CHECK_EQUAL(numDone, cells.size());

        for (size_t i = 0;  i < outVals.size();  ++i) {
            BOOST_CHECK_EQUAL(cells[i], outVals[i]);
        }
    }

    {
        std::vector<CellValue> outVals(cells.size());
        std::vector<int8_t> done(cells.size());
        
        auto onEntry = [&] (int64_t rowNum, CellValue val)
            {
                if (val.empty()) {
                    cerr << "rowNum " <<rowNum << " has null" << endl;
                }
                BOOST_CHECK_GE(rowNum, 0);
                BOOST_CHECK_LT(rowNum, cells.size());
                BOOST_CHECK_EQUAL(done[rowNum], false);
                BOOST_CHECK(!val.empty());
                done[rowNum] = true;
                outVals[rowNum] = std::move(val);
                return true;
            };
        
        frozen->forEach(onEntry);

        for (size_t i = 0;  i < outVals.size();  ++i) {
            BOOST_CHECK_EQUAL(cells[i], outVals[i]);
        }
    }

    {
        std::set<CellValue> vals, cellVals;

        auto onDistinct = [&] (CellValue val)
            {
                bool inserted = vals.insert(val).second;
                if (!inserted) {
                    cerr << "error on " << jsonEncodeStr(val) << endl;
                }
                BOOST_CHECK(inserted);
                return true;
            };

        frozen->forEachDistinctValue(onDistinct);

        for (auto & c: cells) {
            if (!vals.count(c)) {
                cerr << "error on " << jsonEncodeStr(c) << endl;
            }
            BOOST_CHECK_EQUAL(vals.count(c), 1);
            cellVals.insert(c);
        }

        BOOST_CHECK(vals == cellVals);
    }
    
    BOOST_REQUIRE_EQUAL(frozen->size(), cells.size());

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

BOOST_AUTO_TEST_CASE( test_double_basics )
{
    std::vector<CellValue> vals;

    for (int64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(i * 0.5);
    }
        
    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::DoubleFrozenColumn");
    
}

BOOST_AUTO_TEST_CASE( test_double_special_values )
{
    std::vector<CellValue> vals;
    vals.emplace_back();
    vals.emplace_back(INFINITY);
    vals.emplace_back(-INFINITY);
    vals.emplace_back(std::numeric_limits<double>::quiet_NaN());
    vals.emplace_back(0.0);
    vals.emplace_back(-0.0);
    vals.emplace_back(1);
    vals.emplace_back(std::numeric_limits<double>::max());
    vals.emplace_back(std::numeric_limits<double>::min());
    vals.emplace_back(std::numeric_limits<double>::lowest());
    
    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::DoubleFrozenColumn");
}

BOOST_AUTO_TEST_CASE( test_double_basics_null )
{
    std::vector<CellValue> vals;
    vals.emplace_back();

    for (int64_t i = 0;  i < 1000;  ++i) {
        vals.push_back(i * 0.5);
        vals.emplace_back();
        vals.push_back(-(i * 0.5));
    }
    vals.emplace_back();

    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::DoubleFrozenColumn");
}

BOOST_AUTO_TEST_CASE( test_timestamp_basics )
{
    std::vector<CellValue> vals;

    for (int64_t i = 0;  i < 10;  ++i) {
        vals.push_back(Date::now().plusSeconds(i * 0.5));
    }
        
    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::TimestampFrozenColumn");
    
}

BOOST_AUTO_TEST_CASE( test_timestamp_special_values )
{
    std::vector<CellValue> vals;
    vals.emplace_back();
    vals.emplace_back(Date::positiveInfinity());
    vals.emplace_back(Date::negativeInfinity());
    vals.emplace_back(Date::notADate());
    vals.emplace_back(Date::fromSecondsSinceEpoch(0.0));
    vals.emplace_back(Date::fromSecondsSinceEpoch(1));
    vals.emplace_back(Date::fromSecondsSinceEpoch(std::numeric_limits<double>::max()));
    vals.emplace_back(Date::fromSecondsSinceEpoch(std::numeric_limits<double>::min()));
    vals.emplace_back(Date::fromSecondsSinceEpoch(std::numeric_limits<double>::lowest()));
    
    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::TimestampFrozenColumn");
}

BOOST_AUTO_TEST_CASE( test_timestamp_basics_null )
{
    std::vector<CellValue> vals;
    vals.emplace_back();

    for (int64_t i = 0;  i < 10;  ++i) {
        vals.push_back(Date::now().plusSeconds(i * 0.5));
        vals.emplace_back();
        vals.push_back(Date::now().plusSeconds(-i * 0.5));
    }
    vals.emplace_back();

    auto frozen = freezeAndTest(vals);
    BOOST_CHECK_EQUAL(MLDB::type_name(*frozen),
                      "MLDB::TimestampFrozenColumn");
}
