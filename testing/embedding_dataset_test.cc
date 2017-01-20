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

    // Create a dataset to hold our embedding
    PolyConfig datasetConfig;
    datasetConfig.type = "embedding";
    
    cerr << proxy.put("/v1/datasets/test1?sync=true",
                      jsonEncode(datasetConfig));

    // Now we have a dataset, put some rows into it

    auto addRow = [&] (const std::string & rowPath, float x, float y)
        {
            MatrixNamedRow row;
            row.rowName = RowPath(rowPath);
            row.columns.emplace_back(ColumnPath("x"), x, Date());
            row.columns.emplace_back(ColumnPath("y"), y, Date());
            cerr << proxy.post("/v1/datasets/test1/rows", jsonEncode(row));
        };

    addRow("row1", 0, 0);
    addRow("row1a", 0, 0);
    addRow("row2", 1, 0);
    addRow("row3", 0.5, 0.5);
    addRow("row4", -1, 0.5);
    addRow("row5", -0.5, 0.1);
    
    // Commit it.  This will also create our distance index
    cerr << proxy.post("/v1/datasets/test1/commit");

    cerr << proxy.get("/v1/datasets/test1/query");

    auto distances = proxy.get("/v1/datasets/test1/query",
                               { { "select", "distance(\"pythag\",{*},\"row1\") AS dist, *" },
                                 { "where", "rowName() != \"row1\" " },
                                 { "orderBy", "dist" },
                                 { "limit", "2" } });


    /*
    select dist( ...,
                 array(select * where rowname() = curname()), 
                 array(select * where rowname() = "row1")
                 )
    */
    cerr << distances.jsonBody() << endl;
}
