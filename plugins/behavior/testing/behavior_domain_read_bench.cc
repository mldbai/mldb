/* behavior_read_bench.cc
   Wolfgang Sourdeau, June 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Benchmark various read operations on behavior domains.
*/

#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/make_shared.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include "soa/service/s3.h"
#include "mldb/utils/testing/benchmarks.h"

#include "mldb/plugins/behavior/behavior_manager.h"
#include "mldb/plugins/behavior/behavior_domain.h"
#include "mldb/plugins/behavior/mapped_behavior_domain.h"

using namespace std;
namespace po = boost::program_options;
using namespace MLDB;

void
testGetSubjectIndex(Benchmarks & bms,
                    const shared_ptr<MappedBehaviorDomain> & beh,
                    const vector<SH> & allSH,
                    int limit)
{
    {
        Benchmark bm(bms, {"getSubjectIndex-" + to_string(limit) + "-from-start"});
        for (int i = 0; i < limit; i++) {
            beh->getSubjectIndex(allSH[i]);
        }
    }
    {
        Benchmark bm(bms,
                     {"getSubjectIndex-" + to_string(limit) + "-from-end"});
        int i;
        size_t pos;
        for (i = 0, pos = allSH.size() - 1;
             i < limit;
             i++, pos--) {
            beh->getSubjectIndex(allSH[pos]);
        }
    }
}

void
testGetSubjectId(Benchmarks & bms,
                 const shared_ptr<BehaviorDomain> & beh,
                 const vector<SH> & allSH,
                 int limit)
{
    {
        Benchmark bm(bms,
                     {"getSubjectId-" + to_string(limit) + "-from-start"});
        for (int i = 0; i < limit; i++) {
            beh->getSubjectId(allSH[i]);
        }
    }
    {
        Benchmark bm(bms, {"getSubjectId-" + to_string(limit) +
        "-from-end"});
        int i;
        size_t pos;
        for (i = 0, pos = allSH.size() - 1;
             i < limit;
             i++, pos--) {
            beh->getSubjectId(allSH[pos]);
        }
    }
}

void
testGetBehaviorId(Benchmarks & bms,
                   const shared_ptr<BehaviorDomain> & beh,
                   const vector<BH> & allBH,
                   int limit)
{
    {
        Benchmark bm(bms, {"getBehaviorId-" + to_string(limit) + "-from-start"});
        for (int i = 0; i < limit; i++) {
            beh->getBehaviorId(allBH[i]);
        }
    }
    {
        Benchmark bm(bms, {"getBehaviorId-" + to_string(limit) + "-from-end"});
        for (int i = 0; i < limit; i++) {
            size_t pos = allBH.size() - 1 - i;
            beh->getBehaviorId(allBH[pos]);
        }
    }
}

inline void increaseLimit(int & limit)
{
    // limit *= 10;
    limit += 10000;
}

int
main(int argc, char ** argv)
{
    string s3Bucket;
    string s3CacheDir("/mnt/s3cache");
    string s3KeyId;
    string s3Key;
    vector<string> inputFiles;
    int upperLimit(0);

    po::options_description all_opt;
    all_opt.add_options()
        ("s3-key-id,I", po::value<string>(&s3KeyId), "S3 access id")
        ("s3-key,K", po::value<string>(&s3Key), "S3 access id key")
        ("s3-bucket,B", po::value<string>(&s3Bucket), "S3 bucket")
        ("s3-cache-directory,C", po::value<string>(&s3CacheDir), "S3 cache directory")
        ("input-file,i", po::value(&inputFiles),
         "File to read")
        ("upper-limit,L", po::value(&upperLimit),
         "Maximum number of elements to test")
        ("help,h", "print this message");

    po::positional_options_description pos ;
    pos.add("input-file", -1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv)
              .options(all_opt)
              .positional(pos)
              .run(),
              vm);
    po::notify(vm);

    if (vm.count("help")) {
        cerr << all_opt << endl;
        return 1;
    }

    BehaviorManager behManager;

    if (!s3KeyId.empty()) {
        if (s3Bucket.empty())
            registerS3Buckets(s3KeyId, s3Key);
        else
            registerS3Bucket(s3Bucket, s3KeyId, s3Key);
    }

    if (!s3CacheDir.empty()) {
        behManager.setS3CacheDir(s3CacheDir);
    }

    auto beh = behManager.get(inputFiles);
    beh->stats();

    Benchmarks bms;

    vector<SH> allSH;
    {
        Benchmark bm(bms, {"allSubjectHashes"});
        allSH = beh->allSubjectHashes();
    }

    bool ordered(true);
    SH previousSH = allSH[0];
    uint64_t minDelta(-1), maxDelta(0);
    for (size_t i = 1; i < allSH.size(); i++) {
        SH sh = allSH[i];
        if (sh == previousSH) {
            ::fprintf(stderr, "SH %lu is same as previous\n", i);
        }
        else {
            uint64_t delta;
            if (sh < previousSH) {
                ordered = false;
                delta = previousSH - sh;
            }
            else {
                delta = sh - previousSH;
            }
            if (minDelta > delta) {
                minDelta = delta;
            }
            if (maxDelta < delta) {
                maxDelta = delta;
            }
        }
        previousSH = sh;
    }
    ::fprintf(stderr,
              "SH are ordered: %d\nmin delta = %lu (%lx);"
              " max delta = %lu (%lx)\n",
              ordered, minDelta, minDelta, maxDelta, maxDelta);

    /* testing the distribution of SH on 100 slots */
    size_t shPerSlice = allSH.size() / 100;
    uint64_t meanDelta = SH(-1) / 100;
    vector<size_t> shSlots(100, 0);
    for (size_t i = 0; i < allSH.size(); i++) {
        int slotNumber = allSH[i] / meanDelta;
        shSlots[slotNumber]++;
    }

    ::fprintf(stderr,
              "* testing SH distribution\nsh per slice: %lu\n", shPerSlice);
    for (int i = 0; i < shSlots.size(); i++) {
        double pcOfMean = (double(shSlots[i]) / shPerSlice) * 100;
        ::fprintf(stderr, "slot %d: %lu; %f%%\n", i, shSlots[i], pcOfMean);
    }

    // exit(0);
    vector<BH> allBH;
    {
        Benchmark bm(bms, {"allBehaviorHashes"});
        allBH = beh->allBehaviorHashes();
    }

    ordered = true;
    BH previousBH(0);
    for (BH bh: allBH) {
        if (bh < previousBH) {
            ordered = false;
            break;
        }
        previousBH = bh;
    }
    ::fprintf(stderr, "* BH ordered: %d\n", ordered);

    auto mappedBeh = dynamic_pointer_cast<MappedBehaviorDomain>(beh);
    if (mappedBeh) {
        ::fprintf(stderr, "* testing getSubjectIndex\n");
        {
            int limit = 1;
            int max = allSH.size();
            if (upperLimit > 0 && max > upperLimit) {
                max = upperLimit;
            }
            while (limit < max) {
                testGetSubjectIndex(bms, mappedBeh, allSH, limit);
                increaseLimit(limit);
            }
        }
    }

    ::fprintf(stderr, "* testing getSubjectId\n");
    {
        int limit = 1;
        int max = allSH.size();
        if (upperLimit > 0 && max > upperLimit) {
            max = upperLimit;
        }
        while (limit < max) {
            testGetSubjectId(bms, beh, allSH, limit);
            increaseLimit(limit);
        }
    }

    ::fprintf(stderr, "* testing getBehaviorId\n");
    {
        int limit = 1;
        int max = allBH.size();
        if (upperLimit > 0 && max > upperLimit) {
            max = upperLimit;
        }
        while (limit < max) {
            testGetBehaviorId(bms, beh, allBH, limit);
            increaseLimit(limit);
        }
    }
    bms.dumpTotals();

    return 0;
}
