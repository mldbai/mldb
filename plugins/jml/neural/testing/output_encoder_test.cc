// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* output_encoder_test.cc
   Jeremy Barnes, 18 May 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Test of the output encoder.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#define MLDB_TESTING_PERCEPTRON

#include <boost/test/unit_test.hpp>
#include <thread>
#include "mldb/utils/thread_barrier.h"
#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/plugins/jml/neural/output_encoder.h"
#include "mldb/plugins/jml/neural/dense_layer.h"
#include "mldb/utils/smart_ptr_utils.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/arch/exception_handler.h"

using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test_output_encoder )
{
    Output_Encoder encoder;
}
