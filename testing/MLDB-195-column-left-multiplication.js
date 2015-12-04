// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// See MLDB-195
// Check we can left multiply by a columns

var datasetConfig = { "id": "ds1", "type": "sparse.mutable" };

var dataset = mldb.createDataset(datasetConfig);

var ts = new Date();

dataset.recordRow("row1", [ [ "Weight", 1, ts ], ["col2", 2, ts] ]);

dataset.commit();

var output = mldb.perform("GET", "/v1/datasets/ds1/query",
                          [["select", "2.2*\"Weight\""]]);

plugin.log(output);

var result = output.responseCode == 200 && JSON.parse(output.response)[0].columns[0][1] == 2.2 
    ? "success": "failure";

result;
