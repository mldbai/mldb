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

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
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

var ts = new Date("2015-01-01");

function recordExample(row, x, score, label, test)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["score", score, ts], ["label", label, ts], ['test', test, ts ] ]);
}

/* Record some values... doesn't really matter what since we're
   testing something mechanical.
*/
recordExample("ex00", 0, "0", 0, 'none');
recordExample("ex10", 1, "0", 1, 'none');
recordExample("ex01", 0, "1", 1, 'none');
recordExample("ex111", 1, "1", 1, 'isone');
recordExample("ex110", 1, "1", 0, 'iszero');

dataset.commit()


var modelFileUrl = "file://tmp/MLDB-198.cls";

var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingData: { 
            select : "{x, score} as features, label",
            from : { id: "test" }
        },
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                "regularization": 'l2'
            }
        },
        algorithm: "glz",
        modelFileUrl: modelFileUrl,
        equalizationFactor: 0.0
    }
};

createAndTrainProcedure(trainClassifierProcedureConfig, "cls");

var functionConfig = {
    type: "classifier",
    params: {
        modelFileUrl: modelFileUrl
    }
};

var createFunctionOutput
    = mldb.put("/v1/functions/cls", functionConfig);
plugin.log("classifier function output", createFunctionOutput);

selected = mldb.get("/v1/functions/cls/application",
                    {input:{features:{x:1,score: "1"}}});

plugin.log(selected);

assertEqual(selected.responseCode, 200);

selected = mldb.get("/v1/functions/cls/application",
                    {input:{features:{x:1,score: 1}}});

plugin.log(selected);
assertEqual(selected.responseCode, 200);


"success"

