/** Load up a Reddit dataset and use a SVD function to apply it to a user's
    profile.
*/

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'beh.mutable',
        id: 'reddit_dataset'
    };

    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
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

    dataset.commit()

    var end = new Date();
    
    plugin.log("Created Reddit dataset for " + lineNum + " users in "
               + (end - start) / 1000 + " seconds");
    
    return dataset;
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{

    if (!succeeded(response)) {
        plugin.log(process, response);
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

createDataset();

// Run the SVD algorithm

var svdConfig = {
    type: "svd",
    params: {
        dataset: { "id": "reddit_dataset" },
        modelFileUrl: "file://tmp/reddit_function_apply.svd.json.gz",
        select: "COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)",
        numSingularValues: 10  // Only choose a small number of singular values
    }
};

createAndTrainProcedure(svdConfig, 'reddit_svd');

var svdFunctionConfig = {
    type: "svd.embedRow",
    params: {
        modelFileUrl: "file://tmp/reddit_function_apply.svd.json.gz"
    }
};

var createFunctionOutput = mldb.put("/v1/functions/svd", svdFunctionConfig);
assertSucceeded("creating SVD", createFunctionOutput);

var vals = mldb.get("/v1/datasets/reddit_dataset/query", {limit:2}).json;

function getQueryString(vals, n)
{
    var q = {};

    for (var i = 0;  i < vals[n].columns.length;  ++i) {
        q[vals[n].columns[i][0]] = vals[n].columns[i][1];
    }

    return q;
}

var q = getQueryString(vals, 0);

plugin.log("query parameters for application of SVD function", q);

var output0 = mldb.get("/v1/functions/svd/application", q).json;

plugin.log("output of SVD application", output0);

// Output of this script is set to "success" for testing purposes
"success"