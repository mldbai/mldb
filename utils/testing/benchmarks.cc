// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#include "mldb/utils/testing/benchmarks.h"

using namespace std;
using namespace MLDB;


/* BENCHMARKS */

void
Benchmarks::
collectBenchmark(const vector<string> & tags, double delta)
    noexcept
{
    Guard lock(dataLock_);
    // fprintf(stderr, "benchmark: %s took %f s.\n",
    //         label.c_str(), delta);
    for (const string & tag: tags) {
        data_[tag] += delta;
    }
}

void
Benchmarks::
dumpTotals(ostream & out)
{
    Guard lock(dataLock_);

    string result("Benchmark totals:\n");
    for (const auto & entry: data_) {
        result += ("  " + entry.first
                   + ": " + to_string(entry.second)
                   + " s.\n");
    }

    out << result;
}

void
Benchmarks::
clear()
{
    Guard lock(dataLock_);
    data_.clear();
}
