/* MLDB-1360-sparse-mutable-multithreaded-insert.cc
   Jeremy Barnes, 20 March 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include "mldb/plugins/sparse_matrix_dataset.h"
#include "mldb/server/mldb_server.h"
#include "mldb/arch/timers.h"

using namespace std;

using namespace MLDB;

void testMtInsert(MutableSparseMatrixDatasetConfig config)
{
    MldbServer server;
    
    server.init();

    PolyConfig pconfig;
    pconfig.params = config;
    MutableSparseMatrixDataset dataset(&server, pconfig, nullptr);

    std::atomic<size_t> done(0);

    constexpr int niter = 2000;
    constexpr int nthreads = 16;

    Date start = Date::now();

    auto insertThread = [&] ()
        {
            int base = random();
            for (unsigned i = 0;  i < niter;  ++i) {
                std::vector<std::tuple<ColumnPath, CellValue, Date> > vals;
                dataset.recordRow(PathElement(base + i), vals);

                if (done.fetch_add(1) % 1000 == 0)
                    cerr << "done " << done << " insertions" << endl;

                if (i % 10 == 0 && Date::now().secondsSince(start) > 5)
                    break;
            }
        };

    cerr << "testing " << jsonEncode(config) << endl;

    Timer timer;

    std::vector<std::thread> threads;
    for (unsigned i = 0;  i < nthreads;  ++i)
        threads.emplace_back(insertThread);

    for (auto & t: threads)
        t.join();

    cerr << "did " << done << " commits in " << timer.elapsed()
         << " at " << done / timer.elapsed_wall() << " commits/second"
         << endl;
}

BOOST_AUTO_TEST_CASE( test_multithreaded_insert_rr )
{
    MutableSparseMatrixDatasetConfig config;
    config.consistencyLevel = WT_READ_AFTER_WRITE;
    config.favor = TF_FAVOR_READS;
    testMtInsert(config);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_insert_rw )
{
    MutableSparseMatrixDatasetConfig config;
    config.consistencyLevel = WT_READ_AFTER_WRITE;
    config.favor = TF_FAVOR_WRITES;
    testMtInsert(config);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_insert_wr )
{
    MutableSparseMatrixDatasetConfig config;
    config.consistencyLevel = WT_READ_AFTER_COMMIT;
    config.favor = TF_FAVOR_READS;
    testMtInsert(config);
}

BOOST_AUTO_TEST_CASE( test_multithreaded_insert_ww )
{
    MutableSparseMatrixDatasetConfig config;
    config.consistencyLevel = WT_READ_AFTER_COMMIT;
    config.favor = TF_FAVOR_WRITES;
    testMtInsert(config);
}
