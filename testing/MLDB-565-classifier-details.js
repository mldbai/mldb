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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0);
recordExample("ex2", 1, 1);
recordExample("ex3", 2, 2);
recordExample("ex4", 3, 3);

dataset.commit()


var modelFileUrl = "file://tmp/MLDB-174.cls";

var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingData: "select {x} as features, y as label from test",
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                regularization: 'none'
            }
        },
        algorithm: "glz",
        modelFileUrl: modelFileUrl,
        equalizationFactor: 0.0,
        mode: "regression",
        functionName: "cls_func"
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

var trainingOutput
    = mldb.put("/v1/procedures/cls_train/runs/1", {});

plugin.log("training output", trainingOutput);

assertEqual(trainingOutput.responseCode, 201);

var expected =  {
    "params" : {
        "addBias" : true,
        "features" : [
            {
                "extract" : "VALUE",
                "feature" : "x"
            }
        ],
        "link" : "LINEAR",
        "weights" : [
            [ 1, 0 ]
        ]
    },
    "type" : "GLZ"
};

var details = mldb.get("/v1/functions/cls_func/details");

assertEqual(details.json.model, expected);

var functionConfig = {
    type: "classifier",
    params: {
        modelFileUrl: modelFileUrl
    }
};

var createFunctionOutput
    = mldb.put("/v1/functions/regressor", functionConfig);
plugin.log("classifier function output", createFunctionOutput);

details = mldb.get("/v1/functions/regressor/details");

assertEqual(details.json.model, expected);

"success"
