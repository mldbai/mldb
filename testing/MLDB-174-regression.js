// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Test of regression. */

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
        trainingData: { 
            select: "{x} as features, y as label",
            from: { id: "test" }
        },
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                ridge_regression: false
            }
        },
        algorithm: "glz",
        modelFileUrl: modelFileUrl,
        equalizationFactor: 0.0,
        mode: "regression"
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

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
                    { input: { features: { x:10 }}});

plugin.log("selected", selected);

var result = selected.json.score;

plugin.log(result);

if (Math.abs(result - 10) > 0.0001)
    throw "Regressor is not regressing";

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
