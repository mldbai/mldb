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
    
    cerr << proxy.put("/v1/datasets/test1",
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


    // Now, let's train an SVD
    SvdConfig svdParams;
    svdParams.trainingData.stm = make_shared<SelectStatement>();
    svdParams.trainingData.stm->from = TableExpression::parse(Utf8String("test1"));

    PolyConfig applyProcedureConfig;
    applyProcedureConfig.type = "svd.train";
    applyProcedureConfig.params = svdParams;
    auto payload = jsonEncode(applyProcedureConfig);
    payload["params"]["runOnCreation"] = 0;

    auto createProcedureOutput = proxy.put("/v1/procedures/test1",
                                           payload);
    
    cerr << createProcedureOutput << endl;

    BOOST_CHECK_EQUAL(createProcedureOutput.code(), 201);

    ProcedureRunConfig trainProcedureConfig;

    auto trainProcedureOutput = proxy.put("/v1/procedures/test1/runs/1",
                                         jsonEncode(trainProcedureConfig));
    
    cerr << trainProcedureOutput << endl;

    // Check it was created in the right place
    BOOST_CHECK_EQUAL(trainProcedureOutput.code(), 201);
    BOOST_CHECK_EQUAL(trainProcedureOutput.getHeader("location"),
                      "/v1/procedures/test1/runs/1");
    

    auto listTrainings = proxy.get("/v1/procedures/test1/runs");



    BOOST_CHECK_EQUAL(listTrainings.code(), 200);
    
    auto list = listTrainings.jsonBody();

    BOOST_CHECK_EQUAL(list.type(), Json::arrayValue);
    BOOST_CHECK_EQUAL(list.size(), 1);
    BOOST_CHECK_EQUAL(list[0].asString(), "1");
}
