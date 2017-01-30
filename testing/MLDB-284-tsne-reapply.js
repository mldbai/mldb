// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Full Reddit example, as a serial procedure. */

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

var numLines = 1000;

// Get a dataset with Reddit in it

var datasetId = "reddit_dataset_" + numLines;


// Now run the rest of the bits in a procedure

var loadDatasetConfig = {
    type: "createEntity",
    params: {
        kind: "plugin",
        type: "javascript",
        id: 'reddit',
        params: {
            address: "file://mldb/testing/reddit_dataset_plugin.js",
            args: { numLines: numLines, datasetId: datasetId }
        }
    }
};

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: { "from" : {"id": datasetId },
                        "select": "COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 500)"
                      },
        columnOutputDataset: { "id": "reddit_svd_embedding", type: "embedding" },
        modelFileUrl: "file://tmp/MLDB-284-tsne.svd.json.gz"
    }
};

var tsneConfig = {
    type: "tsne.train",
    params: {
        trainingData: { "from" : {"id": "reddit_svd_embedding" }},
        rowOutputDataset: { "id": "reddit_tsne_embedding", "type": "embedding" },
        modelFileUrl: "file://tmp/MLDB-284-tsne.bin.gz"
    }
};

var functionConfig = {
    type: "createEntity",
    params: {
        kind: "function",
        type: "serial",
        id: "tsne_apply",
        params: {
            steps: [
                {
                    type: "svd.embedRow",
                    params: {
                        modelFileUrl: "file://tmp/MLDB-284-tsne.svd.json.gz"
                    }
                },
                {
                    type: "tsne.embedRow",
                    'with': 'svd AS embedding',
                    params: {
                        modelFileUrl: "file://tmp/MLDB-284-tsne.bin.gz"
                    }
                }
            ]
        }
    }
};

var functionConfig2 = {
    type: "createEntity",
    params: {
        kind: "function",
        id: "tsne_apply",
        type: "tsne.embedRow",
        params: {
            modelFileUrl: "file://tmp/MLDB-284-tsne.bin.gz"
        }
    }
};

var transformConfig = {
    type: "transform",
    params: {
        inputData: {
            select: 'APPLY FUNCTION tsne_apply WITH (object(SELECT svd*) AS embedding) EXTRACT (x,y)',
            from: "reddit_svd_embedding",
            named: 'rowName()'
        },
        outputDataset: { id: 'transformed', type: 'embedding' }
    }
};

var procedureConfig = {
    type: "serial",
    params: {
        steps: [
            loadDatasetConfig,
            svdConfig,
            tsneConfig,
            functionConfig2,
            transformConfig
        ]
    }
};

createAndTrainProcedure(procedureConfig, "reddit");

var resp = mldb.get("/v1/datasets/transformed/query", {select:'*', orderBy:'rowName()', limit:20}).json;

plugin.log(resp);

var resp2 = mldb.get("/v1/datasets/reddit_tsne_embedding/query", {select:'*', orderBy:'rowName()', limit:20}).json;

plugin.log(resp2);

assertEqual(resp.length, 20);

"success"
