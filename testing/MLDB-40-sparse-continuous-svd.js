// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of regression. */

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

dataset.recordRow("ex0", [ [ "x", 0, ts ], ["y", 0, ts] ]);
dataset.recordRow("ex1", [ [ "x", 1, ts ], ["y", 1, ts] ]);
dataset.recordRow("ex2", [ [ "x", 2, ts ], ["y", 2, ts] ]);
dataset.recordRow("ex3", [ [ "x", 3, ts ]  ]);
dataset.recordRow("ex4", [ [ "x", 4, ts ], ["y", 4, ts] ]);
dataset.recordRow("ex5", [ [ "x", 5, ts ], ["y", 5, ts] ]);

dataset.commit()


var svdUri = "file://tmp/MLDB-174.svd";

var trainSvdProcedureConfig = {
    type: "svd.train",
    params: {
        trainingData: { "from" : {id: "test" }},
        rowOutputDataset: { id: "svdRowOutput", type: "embedding" },
        columnOutputDataset: { id: "svdColOutput", type: "embedding" },
        modelFileUrl: svdUri
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/svd_train", trainSvdProcedureConfig);

plugin.log("procedure output", procedureOutput);
assertEqual(procedureOutput.responseCode, 201, "failed to train SVD");

var selectRowOutput = mldb.query("SELECT * FROM svdRowOutput");
plugin.log(selectRowOutput);
assertEqual(selectRowOutput.length, 6, "expected 6 rows");

var selectColumnOutput = mldb.query("SELECT * FROM svdColOutput");
plugin.log(selectColumnOutput);
assertEqual(selectColumnOutput.length, 2, "expected 2 rows");


"success"
