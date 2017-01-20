/** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

extern "C" {
void validate_pffft_simd();
} // extern "C"


BOOST_AUTO_TEST_CASE( test_pffft_vectorization )
{
    validate_pffft_simd();
}

