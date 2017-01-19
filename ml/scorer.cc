// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* scorer.cc
   Jeremy Barnes, 5 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Scorer class.
*/

#include <mutex>
#include "scorer.h"
#include "mldb/types/date.h"
#include "mldb/arch/spinlock.h"
#include <thread>

using namespace ML;
using namespace std;


namespace MLDB {


/*****************************************************************************/
/* SCORER                                                                    */
/*****************************************************************************/

ScoredStats
Scorer::
test(const DataPartition & partition) const
{
    // 1.  Run the model on everything

    ScoredStats result;

    Date before = Date::now();

    typedef Spinlock Lock;
    Lock lock;

    auto onExample = [&] (bool label, const boost::any & user, double weight,
                          size_t exampleNum)
        {
            double score = this->scoreGeneric(user);
            std::unique_lock<Lock> guard(lock);
            result.update(label, score, weight, user);
        };

    partition.forEachExample(onExample,
                             isCallableFromMultipleThreads() /* parallel */);

    Date after = Date::now();

    cerr << "calculated " << partition.exampleCount()
         << " scores in " << after.secondsSince(before)
         << " at " << partition.exampleCount() / after.secondsSince(before)
         << " per second" << endl;

    result.calculate();

    return result;
}



} // namespace MLDB
