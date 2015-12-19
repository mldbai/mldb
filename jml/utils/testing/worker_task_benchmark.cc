// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* worker_task_test.cc
   Jeremy Barnes, 5 May 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   Test for the worker task code.
*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/jml/utils/worker_task.h"
#include "mldb/ml/jml/training_data.h"
#include "mldb/ml/jml/dense_features.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/arch/atomic_ops.h"
#include "mldb/arch/demangle.h"

using namespace ML;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE(worker_task_test)
{
    std::atomic<uint64_t> jobsDone(0);

    auto work = [&] ()
        {
            uint64_t doneBefore = jobsDone.fetch_add(1);
            if (doneBefore && doneBefore % 100000 == 0)
                cerr << "done " << doneBefore << endl;
        };
    
    ML::Worker_Task task(10);

    uint64_t numJobs = 1000000;

    ML::Timer timer;

    auto group = task.get_group([] () {},
                                "group");

    for (uint64_t i = 0;  i < numJobs;  ++i)
        task.add(work, "job");

    task.run_until_finished(group, true /* unlock */);

    cerr << "elapsed " << timer.elapsed() << endl;

    BOOST_CHECK_EQUAL(jobsDone.load(), numJobs);
}

