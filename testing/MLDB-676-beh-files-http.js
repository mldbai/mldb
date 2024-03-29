// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var datasetConfig = {
    type: "beh",
    id: "recipes",
    params: {
        dataFileUrl: "file://mldb/mldb_test_data/rcp.beh"
    }
};

var resp = mldb.post("/v1/datasets", datasetConfig);

plugin.log(resp);

unittest.assertEqual(resp.responseCode, 201);
unittest.assertEqual(resp.json.status.columnCount, 9546);
unittest.assertEqual(resp.json.status.rowCount, 115307);
unittest.assertEqual(resp.json.status.valueCount, 9546);

// Now check that it fails properly

datasetConfig.params.dataFileUrl = "https://s3.amazonaws.com/public-mldb-ai/i-dont-exist.beh"
datasetConfig.id = "recipes2";

var resp2 = mldb.post("/v1/datasets", datasetConfig);

plugin.log(resp2);

unittest.assertEqual(resp2.responseCode, 400);

datasetConfig.params.dataFileUrl = "https://host.that.doesnt.exist.datacratic.com/public-mldb-ai/i-dont-exist.beh"
datasetConfig.id = "recipes3";

var resp3 = mldb.post("/v1/datasets", datasetConfig);

plugin.log(resp3);

unittest.assertEqual(resp3.responseCode, 400);

datasetConfig.params.dataFileUrl = "http://host.that.doesnt.exist.asdisahdkuehr38yujhadkhsajk.com/public-mldb-ai/i-dont-exist.beh"
datasetConfig.id = "recipes4";

var resp4 = mldb.post("/v1/datasets", datasetConfig);

plugin.log(resp4);

unittest.assertEqual(resp4.responseCode, 400);


"success"
