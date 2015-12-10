// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Check that we can transpose a dataset and it returns the identity
   function.
*/

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndTrainProcedure(config, name)
{
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);
}


var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y, label, test)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts]]);
}

recordExample("ex00", 0, 0, 0);
recordExample("ex10", 1, 0, 1);
recordExample("ex01", 0, 1, 1);
recordExample("ex111", 1, 1, 1);
recordExample("ex110", 1, 1, 0);
recordExample("ex22", 2, 2, 0);
recordExample("ex31", 3, 1, 1);

dataset.commit()

//var dataset = createDataset();

// Double transposition should be the identity function

var dataset2_config = {
    type: 'transposed',
    id:   'test2',
    params: {
        dataset: {
            type: 'transposed',
            params: {
                dataset: { id: 'test' }
            }
        }
    }
};

var dataset2 = mldb.createDataset(dataset2_config);

assertEqual(mldb.get('/v1/datasets/test/query').json,
            mldb.get('/v1/datasets/test2/query').json);

// Should be able to run an SVD on both

// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: { "from" : {id: "test" }},
        columnOutputDataset: { id: "test_embedding", type: "embedding" },
        rowOutputDataset: { id: "test_row_embedding", type: "embedding" },
        numSingularValues: 10
    }
};

createAndTrainProcedure(svdConfig, 'svd1');

svdConfig.params.trainingData.from.id = "test2";
svdConfig.params.columnOutputDataset.id = "test2_embedding";
svdConfig.params.rowOutputDataset.id = "test2_row_embedding";

createAndTrainProcedure(svdConfig, 'svd2');

assertEqual(mldb.get('/v1/datasets/test_embedding/query').json,
            mldb.get('/v1/datasets/test2_embedding/query').json);

assertEqual(mldb.get('/v1/datasets/test_row_embedding/query').json,
            mldb.get('/v1/datasets/test2_row_embedding/query').json);

"success"
