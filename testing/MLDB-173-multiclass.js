// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of regression. */

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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y, label)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0, "cat");
recordExample("ex2", 1, 1, "dog");
recordExample("ex3", 0.1, 0.1, "cat");
recordExample("ex4", 0.9, 0.9, "dog");

dataset.commit()


var modelFileUrl = "file://tmp/MLDB-173.cls";

var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingData: "select {x,y} as features, label from test",
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                "regularization": 'none'
            }
        },
        algorithm: "glz",
        modelFileUrl: modelFileUrl,
        equalizationFactor: 0.0,
        mode: "categorical"
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);
assertEqual(procedureOutput.responseCode, 201);

var trainingOutput
    = mldb.put("/v1/procedures/cls_train/runs/1", {});

    plugin.log("training output", trainingOutput);


var functionConfig = {
    type: "classifier",
    params: {
        modelFileUrl: modelFileUrl
    }
};

var createFunctionOutput
    = mldb.put("/v1/functions/regressor", functionConfig);
plugin.log("classifier function output", createFunctionOutput);

selected = mldb.get("/v1/functions/regressor/application",
                    {input: { features: {x:0.5,y:0.5}}});
plugin.log("selected", selected);

var result = selected.json.score;

plugin.log(result);

//MLDB-885

var resp = mldb.get('/v1/query', { q: 'select label, sum(regressor(  {{x,y} as features }  )[scores]) from test group by label', format: 'table' });

plugin.log(resp);

assertEqual(resp.responseCode, 200);

if (Math.abs(result - 10) > 0.0001)
    throw "Regressor is not regressing";

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
