// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** mldb_dataset_test.cc                                           -*- C++ -*-
    JF Connolly, April 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Test for nuance.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/plugins/classifier.h"
#include "mldb/http/http_rest_proxy.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    /* Connection ************************************************************/
    cerr << endl << "##### Connect" << endl;
    MldbServer server;

    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");

    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    /* Dataset ***************************************************************/
    cerr << endl << "##### Put the data set config" << endl;

    PersistentDatasetConfig params;
    params.dataFileUrl = Url("file:///home/jfconnolly/workspace/platform/mldb/testing/static/MLDB-429.beh.gz");

    PolyConfig datasetConfig;
    datasetConfig.type = "beh";
    datasetConfig.id = "nuance_hangup";
    datasetConfig.params = params;

    auto createResult = proxy.put("/v1/datasets/nuance_hangup",
                                  jsonEncode(datasetConfig));

    cerr << createResult << endl;

    // If we don't have the dataset, we can't do anything
    if (createResult.code() == 400)
        return;

    /* Procedure config *******************************************************/
    cerr << endl << "##### Put the procedure config" << endl;

    // Classifier config
    ClassifierConfig clsConfig;
    clsConfig.dataset = TableExpression::parse(Utf8String("nuance_hangup"));
    clsConfig.algorithm = "bbdt";
    clsConfig.modelFileUrl = Url("file://nuance_hangup_bbdt.cls");
    clsConfig.where = SqlExpression::parse("rowHash() % 10 != 1");
    clsConfig.label = SqlExpression::parse("event_hangup='XA' OR event_hangup='XH'");
    clsConfig.select = SelectExpression("* EXCLUDING (event_hangup)");
    clsConfig.weight = SqlExpression::parse("1.0");

    // Procedure config
    PolyConfig procedureConfig;
    procedureConfig.type = "classifier.train";
    procedureConfig.id = "trained_cls_nuance_hangup_bbdt";
    procedureConfig.params = clsConfig;

    cerr << proxy.put("/v1/procedures/trained_cls_nuance_hangup_bbdt",
                      jsonEncode(procedureConfig));

    /* Train the classifier **************************************************/
    cerr << endl << "##### Train the classifier" << endl;

    // Empty config
    ProcedureRunConfig trainingConfig;
    cerr << proxy.put("/v1/procedures/trained_cls_nuance_hangup_bbdt/runs/one",
                      jsonEncode(trainingConfig));
   }
