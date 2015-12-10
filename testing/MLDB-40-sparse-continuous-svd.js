// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Test of regression. */

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


var svdUri = "tmp/MLDB-174.svd";

var trainSvdProcedureConfig = {
    id: "trainSvdProcedure",
    type: "svd.train",
    params: {
        trainingData: { "from" : {id: "test" }},
        rowOutputDataset: { id: "svdRowOutput", type: "embedding" },
        columnOutputDataset: { id: "svdColOutput", type: "embedding" },
        svdUri: svdUri
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/svd_train", trainSvdProcedureConfig);

plugin.log("procedure output", procedureOutput);

var trainingOutput
    = mldb.put("/v1/procedures/svd_train/trainings/1", {});

plugin.log("training output", trainingOutput);

plugin.log(mldb.get("/v1/datasets/svdRowOutput/query").json);
plugin.log(mldb.get("/v1/datasets/svdColOutput/query").json);

"success"
