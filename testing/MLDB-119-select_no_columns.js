// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// See MLDB-119
// Selecting no columns should cause a failure

var datasetConfig = { "id": "ds1", "type": "sparse.mutable" };

var dataset = mldb.createDataset(datasetConfig);

var ts = new Date();

dataset.recordRow("row1", [ [ "col1", 1, ts ], ["col2", 2, ts] ]);

dataset.commit();

var kmeansConfig = {
    type: "kmeans.train",
    params: {
        trainingData: "select bonus* from ds1",
    }
};

var kmeansOutput = mldb.put("/v1/procedures/kmeans", kmeansConfig);

plugin.log("kmeans output", kmeansOutput);


var trainingOutput = mldb.put("/v1/procedures/kmeans/runs/1", {});

plugin.log("training output", trainingOutput);

var result = "failure";

// Check that it does fail
if (trainingOutput.response.match("matched no columns"))
    result = "success";

result;
