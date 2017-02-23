// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

function assertNearlyEqual(expr, val, msg, percent, abs)
{
    percent = percent || 1.0;
    abs = abs || 1e-10;
    
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    var diff = Math.abs(val - expr);

    if (diff < abs)
        return;

    var mag = Math.max(Math.abs(val), Math.abs(expr));

    var relativeErrorPercent = diff / mag * 100.0;

    if (relativeErrorPercent <= percent)
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);
    plugin.log("diff", diff);
    plugin.log("abs", abs);
    plugin.log("relativeErrorPercent", relativeErrorPercent);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not approximately equal to " + JSON.stringify(val);
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
    var start = new Date();

    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);

    var end = new Date();

    plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");
}


var config = {
    type: 'import.word2vec',
    params: {
        dataFileUrl: 'file:///home/jeremy/GoogleNews-vectors-negative300.bin.gz',
        outputDataset: {
            type: 'embedding',
            id: 'w2v'
        },
        limit: 100000
    }
};

createAndTrainProcedure(config, "w2v");

var sql_func_res = mldb.put("/v1/functions/nn", {
    type: 'embedding.neighbors',
    params: {
        'dataset': {id: 'w2v', type: "embedding"}
    }
});

var res3 = mldb.get('/v1/query', {q: "select nn({numNeighbors : 10, coords: 'France'})[distances] as *", format: 'table'}).json;

mldb.log(res3[0]);

var expected = [
   "_rowName",
   "Belgium",
   "Europe",
   "France",
   "French",
   "Germany",
   "Italy",
   "Morocco",
   "Paris",
   "Spain",
   "Switzerland"
];

assertEqual(res3[0], expected);

//Check that the result of the pooler can be fed back into the w2vec nearest neighbor
mldb.log("NN to pooler")

mldb.put("/v1/functions/pooler", {
            "type": "pooling",
            "params": {
                "embeddingDataset": "w2v"
            }
        })

var resNNPooler = mldb.get('/v1/query', {q: "select nn({numNeighbors : 10, coords: pooler({words:" +  
                                            "tokenize('France')})[embedding]})[distances] as *", format: 'table'}).json;

assertEqual(resNNPooler[0], expected);

// MLDB-1020 check that we can record both 'null' and '0' which hash
// to the same value.
var res4 = mldb.get("/v1/query", { q: "select * from w2v where rowName() = '0' or rowName() = 'null'", format: 'table'}).json;

mldb.log(res4);

// MLDB-2144 test the "named" parameter

var config_2 = {
    type: 'import.word2vec',
    params: {
        dataFileUrl: 'file:///home/jeremy/GoogleNews-vectors-negative300.bin.gz',
        named: "'banane_'+ word",
        outputDataset: {
            type: 'embedding',
            id: 'w2v_2'
        },
        limit: 10
    }
};

createAndTrainProcedure(config_2, "w2v_2");

var res5 = mldb.query("SELECT rowName() FROM w2v_2");

expected = [
   {
      "columns" : [
         [ "rowName()", "banane_</s>", "-Inf" ]
      ],
      "rowName" : "banane_</s>"
   },
   {
      "columns" : [
         [ "rowName()", "banane_in", "-Inf" ]
      ],
      "rowName" : "banane_in"
   },
   {
      "columns" : [
         [ "rowName()", "banane_for", "-Inf" ]
      ],
      "rowName" : "banane_for"
   },
   {
      "columns" : [
         [ "rowName()", "banane_that", "-Inf" ]
      ],
      "rowName" : "banane_that"
   },
   {
      "columns" : [
         [ "rowName()", "banane_is", "-Inf" ]
      ],
      "rowName" : "banane_is"
   },
   {
      "columns" : [
         [ "rowName()", "banane_on", "-Inf" ]
      ],
      "rowName" : "banane_on"
   },
   {
      "columns" : [
         [ "rowName()", "banane_##", "-Inf" ]
      ],
      "rowName" : "banane_##"
   },
   {
      "columns" : [
         [ "rowName()", "banane_The", "-Inf" ]
      ],
      "rowName" : "banane_The"
   },
   {
      "columns" : [
         [ "rowName()", "banane_with", "-Inf" ]
      ],
      "rowName" : "banane_with"
   },
   {
      "columns" : [
         [ "rowName()", "banane_said", "-Inf" ]
      ],
      "rowName" : "banane_said"
   }
]

assertEqual(res5, expected);

"success"
