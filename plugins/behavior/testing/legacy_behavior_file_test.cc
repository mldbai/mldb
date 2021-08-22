/* behavior_domain_test.cc
   Jeremy Barnes, 9 June 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of the behavior domain classes.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/plugins/behavior/mapped_behavior_domain.h"
#include "mldb/utils/smart_ptr_utils.h"
#include "mldb/utils/string_functions.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/pair_utils.h"
#include "mldb/arch/demangle.h"

#include "mldb/utils/testing/fixtures.h"

using namespace std;
using namespace MLDB;


MLDB_FIXTURE(behavior_domain_test);

BOOST_AUTO_TEST_CASE( test_subject_info_sort1 )
{
    MappedBehaviorDomain beh("mldb/mldb_test_data/rcp.beh");

    BOOST_REQUIRE_EQUAL(sizeof(MappedBehaviorDomain::Metadata), 4224);
    BOOST_REQUIRE_EQUAL(offsetof(MappedBehaviorDomain::Metadata, fileMetadataOffset), 176);
    BOOST_CHECK_EQUAL(beh.md->version, 5);
    BOOST_CHECK_EQUAL(beh.md->fileMetadataOffset, 47127713);
}