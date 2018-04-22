/* filter_streams_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string.h>

#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/utils/testing/watchdog.h"

#include <boost/test/unit_test.hpp>


using namespace MLDB;
using namespace std;


BOOST_AUTO_TEST_CASE( test_mmap_file )
{
    ML::Watchdog watchdog(5.0);

    auto mapping = mapUri("s3://jeremytest/tsx-100k.csv");
    cerr << "info is " << jsonEncodeStr(mapping.info) << endl;

    std::string contents(mapping.data, mapping.data + mapping.length);

    cerr << "got contents" << endl;
}
