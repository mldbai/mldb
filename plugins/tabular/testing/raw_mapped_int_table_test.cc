/** mapped_selector_table_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/plugins/tabular/raw_mapped_int_table.h"
#include "int_table_test_utils.h"

using namespace std;
using namespace MLDB;

TEST_CASE("raw mapped int table size")
{
    CHECK(sizeof(RawMappedIntTable) == 16);
}

static_assert(std::is_default_constructible_v<RawMappedIntTable>);

TEST_CASE("raw mapped int table basics")
{
    doIntTableBasicsTest<RawMappedIntTable>();
}

TEST_CASE("raw mapped int table stress")
{
    for (auto numBits: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 23, 24, 25, 31, 32}) {
        for (unsigned i = 0;  i < 16;  ++i) {
            size_t len = rand() % (1 << (rand() % 18));
            std::vector<uint32_t> vals(len);
            for (size_t i = 0;  i < len;  ++i) {
                vals[i] = rand() % (1ULL << numBits);
            }
            
            auto [context, table] = freeze_table<RawMappedIntTable>(vals);
        
            ExcAssert(table->size() == vals.size());
        }
    }
}

TEST_CASE("raw mapped size wrong")
{
    std::vector<uint32_t> vals = {2,0,1,0,1,0,2,1,1,1,0,1,1,0,0,0,1,0,0,2,1,0,0,0,1,0,0,0,0,1,0,0,0,1,0,0,0,0,2,0,1,0,0,1,1,0,0,1,0,0,1,2,0,2,0,0,0,0,2,1,0,0,1,0,0,1,1,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,1,0,2,1,1,1,0,0,0,1,0,2,0,1,0,0,0,0,1,0,2,1,1,0,0,2,1,0,0,2,0,1,0,0,0,0,0,0,0,0,2,1,0,0,0,1,1,2,3,0,1,0,0,2,0,1,1,1,0,0,0,0,1,1,1,1,0,0,0,1,0,2,0,0,0,1,2,0,0,0,0,1,1,0,0,0,1,0,0,0,0,1,0,1,2,0,0,1,0,1,0,0,1,0,0,2,0,0,1,1,0,1,0,0,0,0,1,1,1,2,0,1,0,0,0,0,0,2,2,0,0,0,1,0,0,0,1,0,0,0,0,0,0,2,0,2,1,0,0,1,0,1,0,0,1,0,0,1,0,1,0,0,0,2,0,0,1,0,2,0,0,1,1,0,2,2,3,0,1,0,5,1,1,1,0,2,1,0,0,1,1,0,0,2,1,0,0,0,1,1,2,2,0,1,0,1,1,1,0,0,0,0,3,1,1,1,1,1,4,1,0,2,2,0,1,2,0,3,0,2,1,0,2,2,0,1,1,4,0,1,1,0,1,1,0,0,0,1,2,0,1,2,1,1,1,0,3,1,3,1,2,1,0,0,0,1,0,2,0,2,0,0,1,1,2,0,2,1,1,1,0,4,2,1,0,2,2,1,1,2,1,1,0,0,1,0,2,3,1,0,0,3,2,3,3,0,1,1,1,2,1,2,1,3,1,1,0,1,2,1,2,1,0,0,0,1,1,2,1,1,1,2,5,0,0,1,0,2,3,0,0,1,1,0,1,0,0,1,0,1,3,0,2,1,0,1,1,0,1,0,0,2,1,0,1,1,0,1,0,0,0,0,2,1,2,2,0,2,1,1,0,0,0,2,3,0,1,0,1,0,1,0,2,0,1,1,0,1,0,2,1,2,1,1,0,2,0,1,1,0,1,0,2,1,0,1,0,3,0,2,0,1,1,1,2,0,0,1,1,0,2,2,3,0,3,1,5,2,1,1,1,3,2,0,0,2,2,0,0,2,3,0,2,0,3,1,4,2,0,1,0,1,1,3,0,0,2,1,4,1,2,2,1,1,5,1,1,2,2,1,1,3,1,3,1,3,2,1,3,3,0,1,1,4,0,1,3,0,1,1,2,0,1,2,2,0,1,2,1,2,1,1,4,1,3,1,2,1,0,1,0,2,1,2,0,2,0,0,2,1,2,0,3,1,2,1,0,4,3,1,0,2,4,1,1,3,1,1,1,1,2,1,2,3,2,1,0,3,2,3,3,0,1,1,2,3,1,2,1,3,2,1,2,1,2,3,3,2,0,0,0,1,3,2,2,2,1,2,5,0,0,2,0,4,4,0,0,1,1,1,1,0,0,1,0,3,3,0,2,2,1,1,1,0,1,2,1,2,1,0,1,2,0,1,3,1,0,0,2,2,2,3,0,2,1,2,1,0,2,4,4,0,5,0,2,0,1,0,2,1,1,1,2,1,0,3,2,4,1,1,2,3,1,2,1,1,1,0,2,3,0,2,2,3,0,2,0,2,2,2,4,0,0,1,1,0,2,3,5,0,3,2,5,2,3,1,1,3,2,0,0,2,3,1,0,4,3,1,2,0,3,2,4,2,1,1,0,1,1,3,1,1,3,1,4,2,3,2,1,1,6,1,2,2,2,1,1,3,2,3,2,3,2,1,5,5,0,2,1,5,1,2,4,0,1,1,2,1,2,2,3,1,1,3,1,2,2,1,5,1,3,2,4,2,0,1,0,3,1,2,0,3,0,3,2,4,3,1,3,2,2,1,0,5,3,1,1,2,4,2,1,5,3,2,1,1,2,2,3,4,2,1,0,4,3,3,4,0,1,1,3,3,1,2,2,3,3,1,2,2,2,3,3,2,1,0,1,2,3,2,3,2,2,3,5,1,0,3,0,5,4,1,0,1,1,2,1,2,2,1,1,4,4,1,3,3,1,1,1,1,2,2,1,2,1,0,1,2,0,2,3,1,0,0,3,2,3,3,1,2,2,3,1,0,2,4,5,0,6,0,2,1,1,2,3,4,2,1,2,2,0,3,4,4,1,1,2,4,2,3,1,2,2,0,4,3,0,5,3,3,1,2,0,2,3,2,4,1,0,1,1,1,3,4,5,0,4,3,5,2,3,1,1,3,2,1,0,2,3,1,0,4,4,2,2,0,6,3,4,4,1,1,0,3,1,3,1,1,3,1,5,2,5,4,1,1,7,1,2,2,3,1,1,4,2,4,4,3,2,3,5,5,2,4,2,5,2,3,4,0,1,1,2,2,2,4,3,2,1,4,1,2,2,2,5,1,3,2,6,2,0,1,0,4,2,2,1,4,0,3,2,4,3,1,3,2,2,3,1,5,3,4,1,3,5,3,1,5,3,2,1,1,3,2,3,6,2,1,0,4,4,4,5,0,2,1,3,4,1,2,2,4,3,2,3,3,3,4,3,4,1,1,1,3,3,3,3,2,3,4,6,2,1,3,0,6,5,1,0,1,2,2,1,2,2,2,1,4,5,2,3,4,2,1,2,2,2,2,1,3,1,1,1,3,2,2,3,2,1,0,4,4,4,3,1,2,4,4,2,2,2,4,5,1,6,0,2,1,1,2,3,5,2,1,2,2,0,4,6,5,1,2,3,4,3,4,1,2,2,1,5,4,2,5,4,3,1,2,0,2,4,2,4,3,0,2,1,2,3,5,5,0,5,4,6,2,3,1,1,3,2,2,0,2,3,2,1,5,4,5,3,1,7,3,4,4,2,1,1,4,1,3,1,1,3,1,6,2,6,6,1,1,7,1,3,5,4,1,1,4,2,5,6,3,2,3,5,8,2,4,2,5,2,4,5,0,2,2,2,2,2,4,3,3,1,4,3,2,2,3,5,4,3,2,6,2,0,1,1,4,3,2,1,4,1,3,2,5,3,1,3,2,3,4,1,6,4,7,5,4,6,4,1,5,3,2,1,1,4,3,3,6,2,2,0,5,4,4,5,0,2,2,3,4,1,2,3,5,4,2,4,4,3,7,3,4,2,1,2,3,3,5,5,2,3,4,7,2,1,3,2,6,6,1,0,2,2,4,2,3,2,2,2,4,5,2,4,4,3,2,2,3,2,2,2,3,1,1,2,5,2,2,3,2,1,0,4,4,5,3,1,2,4,4,2,3,2,4,5,1,7,0,4,1,4,3,3,6,2,1,2,2,1,4,7,6,1,2,3,4,4,7,2,2,3,2,5,4,3,5,4,6,1,3,0,3,5,2,4,4,0,3,1,3,3,5,5,0,7,4,6,2,3,2,1,3,2,4,0,3,3,2,1,5,4,7,3,1,7,4,5,4,2,1,1,4,4,3,1,2,3,2,7,2,8,6,1,1,7,1,4,5,5,1,1,7,2,5,6,4,3,3,5,8,2,4,2,7,2,4,6,0,3,2,2,3,2,5,6,4,1,4,5,3,3,3,5,5,3,3,6,2,0,3,2,4,4,2,2,7,2,4,2,6,4,2,3,2,5,4,2,6,5,7,5,4,6,4,2,5,3,2,1,1,4,3,3,6,4,3,0,6,4,4,6,0,3,2,3,4,1,3,3,7,5,2,4,4,3,7,3,4,2,1,2,3,3,6,6,2,4,5,8,2,1,3,3,7,6,3,0,2,4,4,2,4,2,2,2,4,5,4,4,5,5,3,2,3,2,3,3,6,1,3,4,6,2,2,4,4,2,0,5,4,5,3,2,2,5,6,2,4,3,4,5,1,7,1,4,1,5,3,4,6,2,2,2,2,2,4,7,6,4,2,3,4,4,7,5,2,3,3,6,4,4,6,4,7,2,3,0,3,6,3,4,4,0,3,2,3,5,6,5,0,7,4,7,2,3,2,1,4,2,6,0,3,5,2,1,5,4,8,3,1,8,4,5,4,2,2,1,4,4,3,1,2,4,4,7,5,8,6,1,1,7,1,4,5,6,1,1,8,3,7,7,4,3,3,6,8,4,5,3,7,2,4,6,1,3,2,2,5,3,5,7,5,2,4,6,3,4,3,5,5,3,3,7,2,0,3,2,5,4,2,2,8,3,5,2,7,4,2,3,3,5,4,2,6,6,7,6,4,8,4,2,5,4,3,2,1,5,3,3,7,4,4,0,6,4,4,7,0,4,3,3,5,2,4,4,8,5,2,4,4,5,7,3,5,3,1,2,3,3,7,7,3,4,5,9,2,2,3,3,7,7,4,0,3,5,5,2,4,3,3,2,4,6,6,4,5,6,3,2,3,4,4,3,6,1,4,4,7,3,2,4,6,3,0,6,5,5,4,2,2,5,6,3,5,4,4,5,3,9,2,4,4,5,5,4,6,3,2,3,2,2,4,8,6,5,2,4,5,4,7,5,3,3,3,6,4,4,7,5,8,3,3,1,3,8,4,6,5,1,4,2,3};

    auto [type, size] = useTableOfType(vals);
    cerr << "type = " << type << " size = " << size << endl;

    freeze_table<RawMappedIntTable>(vals);
}

TEST_CASE("raw mapped size wrong 2")
{
    std::vector<uint32_t> vals = {128,256,384,512,640,706,706,706,706,706,706,706,706,706,706,706,706,706,706,706,706,706};

    auto [type, size] = useTableOfType(vals);
    cerr << "type = " << type << " size = " << size << endl;

    freeze_table<RawMappedIntTable>(vals);
}