// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "mldb/jml/utils/rng.h"
#include "mldb/utils/testing/benchmarks.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/plugins/behavior/mutable_behavior_domain.h"

using namespace std;
using namespace MLDB;

vector<Id>
genIds(int numIds)
{
    vector<Id> ids;

    ids.reserve(numIds);
    for (int i = 0; i < numIds; i++) {
        ids.push_back(Id(randomString(20)));
    }

    return ids;
}

int main(int argc, char * argv[])
{
    int nThreads(1);
    int numSubjects(1);
    int numBehs(1);

    {
        using namespace boost::program_options;

        options_description all_opt;
        all_opt.add_options()
            ("num-threads,t", value(&nThreads),
             "default: 1")
            ("num-subjects,s", value(&numSubjects),
             "default: 1")
            ("num-behaviors,b", value(&numBehs),
             "default: 1")
            ("help,H", "show help");

        if (argc == 1) {
            return 0;
        }

        variables_map vm;
        store(command_line_parser(argc, argv)
              .options(all_opt)
              .run(),
              vm);
        notify(vm);

        if (vm.count("help")) {
            cerr << all_opt << endl;
            return 1;
        }
    }

    cerr << "subjects: " << numSubjects
         << "; behaviors: " << numBehs
         << "; threads: " << nThreads
         << endl;

    vector<Id> subjects = genIds(numSubjects);
    vector<Id> behaviors = genIds(numBehs);

    MutableBehaviorDomain behDom;
    int sliceSize = numSubjects / nThreads;
    if (sliceSize * nThreads < numSubjects) {
        sliceSize++;
    }

    vector<MutableBehaviorDomain::ManyEntryId> behEntries(numBehs);
    Date behDate = Date::now();
    for (int i = 0; i < numBehs; i++) {
        auto & entry = behEntries[i];
        entry.behavior = behaviors[i];
        entry.timestamp = behDate;
        behDate.addSeconds(-123.321);
    }
    
    Benchmarks bms;
    
    auto populate = [&] (int threadNum) {
        Benchmark bm(bms, "record-" + to_string(threadNum));
        int start = threadNum * sliceSize;
        int end = min(start + sliceSize, numSubjects);
        for (int i = start; i < end; i++) {
            const Id & subject = subjects[i];
            behDom.recordMany(subject, &behEntries[0], numBehs);
        }
    };

    {
        Benchmark bm(bms, "record");

        vector<thread> threads;
        for (int i = 1; i < nThreads; i++) {
            threads.emplace_back(populate, i);
        }

        std::atomic_thread_fence(std::memory_order_release);

        populate(0);

        for (auto & th: threads) {
            th.join();
        }
    }

    bms.dumpTotals();

    return 0;
}
