// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// See MLDB-6
// Check that we can get a meaningful error out of a dataset

var datasetConfig = { "type": "mutableasdsdadsasddasdsdasda" };

var response1 = mldb.put("/v1/datasets/test1", datasetConfig);

if (response1.responseCode != 400)
    throw "failure to return a good response code";

"success"
