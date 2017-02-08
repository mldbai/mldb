// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

assertEqual(mldb.get('/v1/query', {q : 'select * from test order by rowHash()'}).json,
            mldb.get('/v1/query', {q : 'select * from test2 order by rowHash()'}).json,
           "query diff");

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

//mldb.log(mldb.get('/v1/datasets/test_embedding/query').json);
//mldb.log(mldb.get('/v1/datasets/test2_embedding/query').json);

var resp1 = mldb.get("/v1/query", {q:"SELECT * FROM test_embedding order by rowHash()", format:'table'});
var resp2 = mldb.get("/v1/query", {q:"SELECT * FROM test2_embedding order by rowHash()", format:'table'});

assertEqual(resp1.json,
            resp2.json);

resp1 = mldb.get("/v1/query", {q:"SELECT * FROM test_row_embedding order by rowHash()", format:'table'});
resp2 = mldb.get("/v1/query", {q:"SELECT * FROM test2_row_embedding order by rowHash()", format:'table'});

assertEqual(resp1.json,
            resp2.json);

// MLDB-1175 transposed dataset as a FROM function

var expected = [
   [ "_rowName", "ex00" ],
   [ "x", 0 ]
]

var resp = mldb.get("/v1/query", {q:"SELECT ex00 FROM transpose(test) WHERE rowName() = 'x'", format:'table'});
plugin.log(resp)
assertEqual(resp.json, expected);

var resp = mldb.get("/v1/query", {q:"SELECT watcha.ex00 as ex00 FROM transpose(test) as watcha WHERE rowName() = 'x'", format:'table'});
plugin.log(resp)
assertEqual(resp.json, expected);

var ref = mldb.get("/v1/query", {q:"SELECT watcha.* FROM test as watcha"});
plugin.log(resp)

var resp = mldb.get("/v1/query", {q:"SELECT watcha.* FROM transpose(transpose(test)) as watcha"});
plugin.log(resp)
assertEqual(resp.json, ref.json);

var resp = mldb.get("/v1/query", {q:"SELECT watcha.ex00 as ex00 FROM transpose((select * from test)) as watcha WHERE rowName() = 'x'", format:'table'});
plugin.log(resp)
assertEqual(resp.json, expected);

var expectedjoin = [
   [ "_rowName", "[ex00]-[x]" ],
   [ "table.label", 0 ],
   [ "table.x", 0 ],
   [ "table.y", 0 ],
   [ "transposed_table.ex00", 0 ],
   [ "transposed_table.ex01", 0 ],
   [ "transposed_table.ex10", 1 ],
   [ "transposed_table.ex110", 1 ],
   [ "transposed_table.ex111", 1 ],
   [ "transposed_table.ex22", 2 ],
   [ "transposed_table.ex31", 3 ]
];

var resp = mldb.get("/v1/query", {q:'SELECT "[ex00]-[x]" FROM transpose(test as table JOIN transpose(test) as transposed_table) as watcha ORDER BY rowName()', format:'table'});
plugin.log(resp)
assertEqual(resp.json, expectedjoin);

"success"
