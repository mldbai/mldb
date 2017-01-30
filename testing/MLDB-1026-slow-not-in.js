// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// Testcase for MLDB-1026

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

// Get a dataset with Reddit in it

var numLines = 100000;
var datasetId = "reddit";

var redditConfig = {
    type: "javascript",
    id: 'reddit',
    params: {
        address: "file://mldb/testing/reddit_dataset_plugin.js",
        args: { numLines: numLines, datasetId: datasetId }
    }
};

var resp = mldb.post('/v1/plugins', redditConfig);

numLines += 10000;
datasetId = "reddit2";

redditConfig = {
    type: "javascript",
    id: 'reddit',
    params: {
        address: "file://mldb/testing/reddit_dataset_plugin.js",
        args: { numLines: numLines, datasetId: datasetId }
    }
};

var resp = mldb.post('/v1/plugins', redditConfig);

mldb.log(resp);

var before = new Date();

var resp = mldb.get('/v1/query', { q: 'select count(*) from reddit2 where rowName() not in (select rowName() from reddit)',
                                   format: 'table' });

var after = new Date();

mldb.log(resp.json);

var timeTaken = (after - before) / 1000.0;

mldb.log("query took", timeTaken, "seconds");

if (timeTaken > 15.0)
    throw "Query took too long";

"success"

