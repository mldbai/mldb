// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

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

function createAndRunProcedure(config, name)
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


var dataset1 = mldb.createDataset({type:'sparse.mutable',id:'test1'});
var dataset2 = mldb.createDataset({type:'sparse.mutable',id:'test2'});

var ts = new Date("2015-01-01");

dataset1.recordRow("ex1", [ [ "x", 1, ts ], ["y", 2, ts] ]);
dataset1.recordRow("ex2", [ [ "x", 2, ts ], ["z", 4, ts] ]);
dataset1.recordRow("ex3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset2.recordRow("ex4", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset2.recordRow("ex5", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset2.recordRow("ex6", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset1.commit()
dataset2.commit()

// transform our 4 elements with orderby rowName()
var transform_config2 = {
    type: 'transform',
    params: {
        inputData: 'select * from test1 join test2 on test1.x = test2.x and test1.y is not null',
        outputDataset: { id: 'transformed', type: 'sparse.mutable' }
    }
};

createAndRunProcedure(transform_config2, "transform2");

var resp = mldb.get('/v1/query', { q: 'select * from transformed', format: 'table' });

mldb.log(resp);

assertEqual(resp.responseCode, 200);

var expected = [
    [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
    [ "[ex1]-[ex4]", 1, 2, 1, 2 ]
];

assertEqual(resp.json, expected);

"success"
