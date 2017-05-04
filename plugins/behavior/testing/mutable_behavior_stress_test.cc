// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mutable_stress_test.cc                                 -*- C++ -*-
    Rémi Attab, 06 Nov 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Description

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/plugins/behavior/mutable_behavior_domain.cc"

#include <boost/test/unit_test.hpp>
#include <vector>
#include <thread>

using namespace std;
using namespace ML;
using namespace MLDB;

// This test is meant to run forever and it's important that it runs more
// threads than the number of available core to flush out race conditions.
BOOST_AUTO_TEST_CASE( record_stress_test )
{
    enum { NumThreads = 64, Rounds = 98, Entries = 4 };

    while (true) {
        MutableBehaviorDomain beh;

        std::atomic<size_t> bi{1};
        Date ts = Date::now();

        std::atomic<bool> done{false};

        auto writeFn = [&] (size_t id) {
            std::array<MutableBehaviorDomain::ManyEntryId, Entries> entries;

            for (size_t i = id; !done; i += NumThreads) {
                for (size_t j = 0; j < entries.size(); ++j) {
                    entries[j].timestamp = ts;
                    entries[j].behavior = Id(bi + j);
                    entries[j].count = (i * 62311) % (1ULL << 24);
                }

                beh.recordMany(Id(i), entries.data(), entries.size());
            }
        };

        std::vector<std::thread> threads;
        for (size_t i = 0; i < NumThreads; ++i)
            threads.emplace_back(std::thread([=] { writeFn(i + 1); }));

        for (size_t i = 0; i < Rounds; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            bi++;
            fprintf(stderr, "\r%lu", bi.load());
        }

        done = true;
        for (auto& th : threads) th.join();
        fprintf(stderr, "\n");
    }
}
