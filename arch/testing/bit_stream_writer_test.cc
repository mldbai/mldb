/* bit_stream_test.cc                                                   -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/arch/bit_stream_writer.h"

using namespace std;
using namespace MLDB;

TEST_CASE("test bit stream writer")
{
   BitStreamWriter<std::vector<uint64_t>, uint64_t, uint32_t> writer;
}
