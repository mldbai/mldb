// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and check that the SVD works */

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
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);
}



function createDataset()
{
    var dataset_config = {
        'type'    : 'sparse.mutable',
        'id'      : 'reddit_dataset',
    };

    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    var now = new Date();

    var stream = mldb.openStream(dataset_address);

    var numLines = 1000;

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

    return dataset;
}

var dataset = createDataset();

// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: { "from" : {"id": "reddit_dataset" }},
        columnOutputDataset: { "id": "reddit_svd_embedding", type: "embedding" },
        numSingularValues: 10
    }
};

createAndTrainProcedure(svdConfig, 'reddit_svd');

var resp = mldb.get('/v1/query', { q: 'select count(*) from reddit_svd_embedding', format: 'table' });

mldb.log(resp.json);

var expected = [
   [ "_rowName", "count(*)" ],
   [ "[]", 2553 ]
];

assertEqual(resp.json, expected);


"success"
