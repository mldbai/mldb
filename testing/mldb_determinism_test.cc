// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* mldb_sql_test.cc                                                -*- C++ -*-
   Mathieu Marquis Bolduc, November 18th
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/plugins/svd.h"
#include "mldb/http/http_rest_proxy.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;

namespace MLDB {

namespace Builtins {

typedef BoundAggregator (&BuiltinAggregator) ();

struct TestRegisterAggregator {
    template<typename... Names>
    TestRegisterAggregator(BuiltinAggregator aggregator, Names&&... names)
    {
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    void doRegister(BuiltinAggregator aggregator)
    {
    }

    template<typename... Names>
    void doRegister(BuiltinAggregator aggregator, std::string name,
                    Names&&... names)
    {
        auto fn = [&] (const Utf8String & str,
                       const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & context)
            -> BoundAggregator
            {
                return aggregator();
            };
        handles.push_back(registerAggregator(Utf8String(name), fn));
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    std::vector<std::shared_ptr<void> > handles;
};
}
}

struct PassAccum {
    PassAccum()
    {
    }

    ExpressionValue val;
};

BoundAggregator pass()
{
    auto init = [] () -> std::shared_ptr<void>
        {
            return std::make_shared<PassAccum>();
        };

    auto process = [] (const ExpressionValue * args,
                       size_t nargs,
                       void * data)
        {
            PassAccum & accum = *(PassAccum *)data;

            ExcAssertEqual(nargs, 1);
            const ExpressionValue & val = args[0];

            accum.val = val;
        };

    auto extract = [] (void * data) -> ExpressionValue
        {
            PassAccum & accum = *(PassAccum *)data;

            return ExpressionValue(std::move(accum.val));

        };

     auto merge = [] (void * data, void* src)
        {
            PassAccum & accum = *(PassAccum *)data;
            PassAccum & srcAccum = *(PassAccum *)src;

            accum.val = srcAccum.val;
        };

    return { init, process, extract, merge };
}

static Builtins::TestRegisterAggregator registerPass(pass, "pass");

//This will check that aggregators do not return a result based on multithread execution
//by using a (fake) aggregator that only keeps the latest result
BOOST_AUTO_TEST_CASE( test_determinism_agggregator )
{
    MldbServer server;
    
    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    // Create a dataset to hold our embedding
    PolyConfig datasetConfig;
    datasetConfig.type = "embedding";
    
    cerr << proxy.put("/v1/datasets/test1",
                      jsonEncode(datasetConfig));

    // Now we have a dataset, put some rows into it

    auto addRow = [&] (const std::string & rowName, float x)
        {
            MatrixNamedRow row;
            row.rowName = RowPath(rowName);
            row.columns.emplace_back(ColumnPath("x"), x, Date());
            cerr << proxy.post("/v1/datasets/test1/rows", jsonEncode(row));
        };

    for (int i = 0; i < 1001; ++i)
    {
       addRow(MLDB::format("row_%d", i), i);
    }

     // Commit it.  This will also create our distance index
    cerr << proxy.post("/v1/datasets/test1/commit");

    auto result = proxy.get("/v1/query",
                      { { "q", "SELECT pass(x) from test1" } }).jsonBody();

    int expectedValue = result[0]["columns"][0][1].asInt();

    for (int i = 0; i < 10; ++i)
    {
       result = proxy.get("/v1/query",
                      { { "q", "SELECT pass(x) from test1" } }).jsonBody();

       Json::Value expected = { "pass(x)", expectedValue, "1970-01-01T00:00:00Z" };
       BOOST_CHECK_EQUAL(result[0]["columns"][0], expected);
    }    

}
