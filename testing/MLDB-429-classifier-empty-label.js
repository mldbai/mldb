// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

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
recordExample("ex112", 1, 1, null);

dataset.commit()

plugin.log(mldb.get('/v1/datasets/test/query'));

function checkSuccess(response)
{
    if (response.responseCode > 200 && response.responseCode < 400)
        return;
    throw "Error: " + JSON.stringify(response);
}

var name = "test";

var modelFileUrl = "file://tmp/MLDB-429_" + name + ".cls";

var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingData: { select : "{x,y} as features, label",
                        from : { id: "test" } },
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

var procedureOutput
    = mldb.put("/v1/procedures/cls_train_" + name,
               trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

var trainingOutput
    = mldb.put("/v1/procedures/cls_train_" + name + "/runs/1", {});

plugin.log("training output", trainingOutput);

checkSuccess(trainingOutput);

"success"
