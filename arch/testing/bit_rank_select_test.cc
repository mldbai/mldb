/* bit_rank_select_test.cc                                                   -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/arch/bit_rank.h"
#include "mldb/arch/bit_select.h"

using namespace std;
using namespace MLDB;

TEST_CASE("test rank and select")
{
    CHECK(bit_rank(0, {1U}) == 0);
    CHECK(bit_rank(1, {3U}) == 1); 
}

