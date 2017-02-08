// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// See MLDB-3
// Check that we can POST and get auto-IDs

plugin.log("datasets", mldb.get('/v1/datasets').json, "\n\n\n");

var datasetConfig = { "type": "sparse.mutable" };

var response1 = mldb.post("/v1/datasets", datasetConfig);

plugin.log("response", response1);

datasetConfig = { "type": "sparse.mutable" };

var dataset = mldb.createDataset(datasetConfig);

plugin.log("dataset", dataset);

plugin.log("datasets", mldb.get('/v1/datasets').json, "\n\n\n");

if (mldb.get('/v1/datasets').json.length != 2)
    throw "Expected two datasets to have been created";

// MLDB-203: can we create a second anonymous dataset?

datasetConfig = { "type": "sparse.mutable" };

var response2 = mldb.post("/v1/datasets", datasetConfig);

plugin.log("response", response2);

if (response2.responseCode != 201)
    throw "Expected a second dataset to have been created";

datasetConfig = { "type": "sparse.mutable" };

var dataset2 = mldb.createDataset(datasetConfig);

plugin.log("dataset", dataset);

var datasets = mldb.get("/v1/datasets");

plugin.log("datasets", mldb.get('/v1/datasets').json, "\n\n\n");

if (datasets.json.length != 4)
    throw "Expected four datasets to have been created";

"success"
