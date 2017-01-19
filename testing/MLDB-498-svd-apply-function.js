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

/* Example script to import a reddit dataset and run an example */

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        'type'    : 'sparse.mutable',
        'id'      : 'reddit_dataset',
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 20000;

    var lineNum = 0;
    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 100000 == 0)
            plugin.log("loaded", lineNum, "lines");
        var line = stream.readLine();
        var fields = line.split(',');
        var tuples = [];
        for (var i = 1;  i < fields.length;  ++i) {
            tuples.push([fields[i], 1, now]);
        }

        dataset.recordRow(fields[0], tuples);
    }

    plugin.log("Committing dataset")
    dataset.commit()

    var end = new Date();
    
    plugin.log("creating dataset took " + (end - start) / 1000 + " seconds");

    return dataset;
}

var dataset = createDataset();

// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: "select COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000) from reddit_dataset",
        modelFileUrl: "file://tmp/MLDB-498.svd.json.gz",
        columnOutputDataset: "svd_output",
        numSingularValues: 10,
        runOnCreation : false
    }
};

createAndTrainProcedure(svdConfig, 'reddit_svd');

plugin.log(mldb.get("/v1/query",
                      { q: 'select rowName() from svd_output limit 10'
                      }));

var svdFunctionConfig = {
    type: "svd.embedRow",
    params: {
        modelFileUrl: "file://tmp/MLDB-498.svd.json.gz"
    }
};

var createFunctionOutput = mldb.put("/v1/functions/svd", svdFunctionConfig);
assertSucceeded("creating SVD", createFunctionOutput);

var vals = mldb.get("/v1/query",
                      { q: 'select * from reddit_dataset limit 2'
                      }).json;

plugin.log(vals);

function getQueryString(vals, n)
{
    var q = {};

    for (var i = 0;  i < vals[n].columns.length;  ++i) {
        q[vals[n].columns[i][0]] = vals[n].columns[i][1];
    }

    return { input: { row: q } };
}


var output0 = mldb.get("/v1/functions/svd/application", getQueryString(vals, 0));

plugin.log(output0);

var output1 = mldb.get("/v1/functions/svd/application", getQueryString(vals, 1));

plugin.log(output1);

assertEqual(output0.json.output.embedding.shape, [10], "output0");
assertEqual(output1.json.output.embedding.shape, [10], "output1");

"success"
