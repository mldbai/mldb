// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_dataset_test.cc                                           -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Test for datasets.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/plugins/svd.h"
#include "mldb/plugins/sql_functions.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_rest_proxy.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    MldbServer server;
    
    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    PolyConfig datasetConfig;
    datasetConfig.type = "sparse.mutable";
    
    cerr << proxy.put("/v1/datasets/test1",
                      jsonEncode(datasetConfig));

    // Now we have a dataset, put some rows into it
    
    MatrixNamedRow row;
    row.rowName = RowPath("row1");
    row.columns.emplace_back(ColumnPath("techno"), "yes", Date());
    row.columns.emplace_back(ColumnPath("dance"), "yes", Date());

    cerr << proxy.post("/v1/datasets/test1/rows", jsonEncode(row));
    row.columns.clear();

    row.rowName = RowPath("row2");
    row.columns.emplace_back(ColumnPath("dance"), "yes", Date());
    row.columns.emplace_back(ColumnPath("country"), "yes", Date());

    cerr << proxy.post("/v1/datasets/test1/rows", jsonEncode(row));
    row.columns.clear();

    row.rowName = RowPath("row3");
    row.columns.emplace_back(ColumnPath("tag with spaces"), "yes", Date());
    row.columns.emplace_back(ColumnPath("tag:with spaces"), "yes", Date());

    cerr << proxy.post("/v1/datasets/test1/rows", jsonEncode(row));
    

    cerr << proxy.post("/v1/datasets/test1/commit");


    auto checkRow = [&] (const std::string & select, const std::string & where ,const std::string & answer)
        {
            std::string query = "/v1/query?q=select "+select + " from test1 where " + where;
            auto selectResult = proxy.get(query);

            BOOST_CHECK_EQUAL(selectResult.code(), 200);

            auto selectJson = jsonDecodeStr<std::vector<MatrixNamedRow> >(selectResult.body());

            BOOST_REQUIRE_EQUAL(selectJson.size(), 1);
            BOOST_CHECK_EQUAL(selectJson[0].rowName, RowPath(answer));
        };

    checkRow("*", "techno='yes'", "row1");
    checkRow("*", "\"tag%20with%20spaces\"='yes'", "row3");
    checkRow("*", "\"tag:with%20spaces\"='yes'", "row3");
}
