// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// 
// behavior_manager_test.cc
// Simon Lemieux - 12 Jul 2013
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// 

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <thread>
#include "mldb/arch/timers.h"
#include "mldb/utils/testing/fixtures.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/plugins/behavior/behavior_manager.h"

using namespace MLDB;
using namespace std;

template <typename T>
void checkVectorsEqual(const vector<T> & v,
                  const vector<T> & w) {
    BOOST_CHECK_EQUAL(v.size(), w.size());
    for (int i=0; i < v.size(); ++i)
        BOOST_CHECK_EQUAL(v[i], w[i]);
};

BOOST_AUTO_TEST_CASE( test_beh_manager_parse_specs )
{
    BehaviorManager bm;
    map<string, string> parsedSpecs = bm.parseSpecs("key1:value1 key2:value2 key3:value3");
    BOOST_CHECK_EQUAL( parsedSpecs.size(), 3);
    BOOST_CHECK_EQUAL( parsedSpecs.count("key1"), 1);
    BOOST_CHECK_EQUAL( parsedSpecs.count("key2"), 1);
    BOOST_CHECK_EQUAL( parsedSpecs.count("key3"), 1);
    BOOST_CHECK_EQUAL( parsedSpecs["key1"], "value1");
    BOOST_CHECK_EQUAL( parsedSpecs["key2"], "value2");
    BOOST_CHECK_EQUAL( parsedSpecs["key3"], "value3");


    vector<string> nbs = bm.splitSpec("1,3,6-8");
    checkVectorsEqual(nbs, {"1", "3", "6", "7"});

    vector<int> nbs2 = bm.splitSpec<int>("1,3,6-8");
    checkVectorsEqual(nbs2, {1, 3, 6, 7});

    nbs = bm.splitSpec("1st,3rd");
    checkVectorsEqual(nbs, {"1st", "3rd"});

    nbs = bm.splitSpec("patate");
    checkVectorsEqual(nbs, {"patate"});

}

BOOST_AUTO_TEST_CASE( test_beh_mgr_getS3CacheEntries )
{
    TestFolderFixture fixture("beh_mgr_getS3CacheEntries");
    char buffer[1024];
    char * cwd = ::getcwd(buffer, sizeof(buffer));
    if (!cwd) {
        throw MLDB::Exception(errno, "getcwd");
    }
    string cacheDir = cwd;

    Date now = Date::now();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    /* create the random files */
    vector<string> files{cacheDir + "/file1.txt",
            cacheDir + "/dir1/file1.txt",
            cacheDir + "/dir2/file1.txt",
            cacheDir + "/dir2/subdir1/file1.txt",
            cacheDir + "/dir2/subdir1/file2.txt"};
    for (const string & filename: files) {
        makeUriDirectory(filename);
        filter_ostream stream(filename);
        stream << "42\n";
        stream.close();
    }
    sort(files.begin(), files.end());

    BehaviorManager bMgr;
    bMgr.setRemoteCacheDir(cacheDir);
    auto results = bMgr.getRemoteCacheEntries();
    BOOST_CHECK_EQUAL(files.size(), results.size());

    /* ensure all returned entries match the above:
       - same filenames
       - file size = 3
       - access time > "now"
    */
    auto compareEntries = [&] (const BehaviorManager::RemoteCacheEntry & file1,
                               const BehaviorManager::RemoteCacheEntry & file2) {
        return file1.name.compare(file2.name) < 0;
    };
    sort(results.begin(), results.end(), compareEntries);
    for (const auto & entry: results) {
        BOOST_CHECK_GT(entry.accessTime, now);
        BOOST_CHECK_EQUAL(entry.size, 3);
    }

    /* ensure access time changes when file is open */
    auto oldEntry = results[0];
    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        filter_istream stream(oldEntry.name);
        cerr << "contents is " + stream.readAll() + "\n";
        stream.close();
        ::utimes(oldEntry.name.c_str(), nullptr);
    }
    results = bMgr.getRemoteCacheEntries();
    sort(results.begin(), results.end(), compareEntries);
    BOOST_CHECK_EQUAL(oldEntry.name, results[0].name);
    BOOST_CHECK_GT(results[0].accessTime, oldEntry.accessTime);
}
