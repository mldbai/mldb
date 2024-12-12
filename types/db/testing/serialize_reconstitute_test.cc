/* compact_size_type_test.cc
   Jeremy Barnes, 12 August 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Testing for the compact size type.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/base/parse_context.h"
#include "mldb/arch/file_functions.h"
#include "mldb/base/scope.h"
#include "mldb/types/db/persistent.h"
#include "mldb/types/db/compact_size_types.h"
#include "mldb/plugins/jml/algebra/matrix.h"
#include "mldb/plugins/jml/algebra/matrix_ops.h"
#include <boost/test/unit_test.hpp>

#include <sstream>
#include "mldb/utils/distribution.h"

#define CHECK_EQUAL(x, y) BOOST_CHECK_EQUAL(x, y)
#include "test_serialize_reconstitute.h"


using namespace MLDB;
using namespace MLDB::DB;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test_char )
{
    test_serialize_reconstitute('a');
}

BOOST_AUTO_TEST_CASE( test_bool )
{
    ostringstream stream_out;
    {
        DB::Store_Writer writer(stream_out);
        writer << true;
    }

    BOOST_CHECK_EQUAL(stream_out.str().size(), 1);
    BOOST_CHECK_EQUAL(stream_out.str().at(0), 1);

    test_serialize_reconstitute(true);

    test_serialize_reconstitute(false);
}

BOOST_AUTO_TEST_CASE( test_distribution )
{
    distribution<float> dist;
    test_serialize_reconstitute(dist);

    dist.push_back(1.0);
    test_serialize_reconstitute(dist);

    dist.push_back(2.0);
    test_serialize_reconstitute(dist);
}
