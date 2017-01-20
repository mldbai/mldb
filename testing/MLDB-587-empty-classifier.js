// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of error message for classifier training with no rows. */


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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset_config2 = {
    'type'    : 'sparse.mutable',
    'id'      : 'test2',
};

var dataset = mldb.createDataset(dataset_config)
var dataset2 = mldb.createDataset(dataset_config2)

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
dataset2.commit();

var modelFileUrl = "file://tmp/MLDB-174.cls";

var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingData: "select {x} as features, y as label from test where false",
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
        equalizationFactor: 0.0,
        mode: "regression",
        modelFileUrl: modelFileUrl
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

var trainingOutput
    = mldb.put("/v1/procedures/cls_train/runs/1", {});

plugin.log("training output", trainingOutput);

assertEqual(trainingOutput.responseCode, 400);
assertContains(trainingOutput.json.error, "all rows were filtered");

trainClassifierProcedureConfig.params.trainingData = "select {x} as features, y as label from test2 where false";

procedureOutput
    = mldb.put("/v1/procedures/cls_train2", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

trainingOutput
    = mldb.put("/v1/procedures/cls_train2/runs/1", {});

plugin.log("training output", trainingOutput);

assertEqual(trainingOutput.responseCode, 400);
assertContains(trainingOutput.json.error, "dataset was empty");


"success"
