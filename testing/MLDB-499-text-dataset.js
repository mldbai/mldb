// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Example script to import a reddit dataset and run an example */

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw new Error("Assertion failure: " + msg + ": " + JSON.stringify(expr)
                    + " not equal to " + JSON.stringify(val));
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

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'text.line',
        id: 'reddit_text_file',
        params: {
            //dataFileUrl: 'file://reddit_user_posting_behavior.csv'
            dataFileUrl: 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
        }
    };

    var now = new Date();

    var dataset = mldb.createDataset(dataset_config);

    var end = new Date();
    
    plugin.log("creating dataset took " + (end - start) / 1000 + " seconds");

    return dataset;
}

var dataset = createDataset();

var resp = mldb.get('/v1/datasets/reddit_text_file/query', {format:'table', orderBy:'lineNumber', limit:20});

plugin.log(resp.json);

assertEqual(resp.json[1][1], 1);
assertEqual(resp.json[1][2], "603,politics,trees,pics");

var before = new Date();
var resp2 = mldb.get('/v1/datasets/reddit_text_file/query', {select: "lineNumber, jseval('return lineNumber + 3 + ''_'' + line', 'lineNumber,line', lineNumber, lineText) AS bonus", format:'table', orderBy:'lineNumber', limit:5});
var after = new Date();

plugin.log(resp2.json);

plugin.log("took " + (after - before) / 1000.0 + "s");

assertEqual(resp2.json[1][1], 1);
assertEqual(resp2.json[1][2], "4_603,politics,trees,pics");

var transformConfig = {
    type: "transform",
    params: {
        inputData: { 
            select: "parse_sparse_csv(lineText)",
            from : "reddit_text_file",
            //rowName: "regex_replace(lineText, '([^,]\+).*', '\\1')",
            //rowName: "lineNumber"
            named : "jseval('return x.substr(0, x.indexOf('',''));', 'x', lineText)"
        },
        outputDataset: { type: 'sparse.mutable', id: 'reddit_dataset' }
    }
};

createAndTrainProcedure(transformConfig, "dataset import");

var resp3 = mldb.get('/v1/datasets/reddit_dataset/query', {format:'sparse', limit:20});

plugin.log(resp3.json);

plugin.log(mldb.post("/v1/datasets/reddit_dataset/routes/saves",
                     {dataFileUrl: 'file://tmp/MLDB-499.beh'}));

"success"
