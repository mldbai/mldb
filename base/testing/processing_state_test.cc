/* processing_state_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test processing state tracker.
*/


#include "mldb/base/processing_state.h"
#include "mldb/utils/testing/mldb_catch2.h"

using namespace std;
using namespace MLDB;

TEST_CASE("basics")
{
    ProcessingState state(0 /* max parallelism */);
    state.waitForAll();
}

