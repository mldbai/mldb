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

// Get a dataset with Reddit in it

var numLines = 2000;
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

mldb.log(resp);

var resp = mldb.get('/v1/query', { q: 'select * from reddit limit 10',
                                   format: 'sparse' });

mldb.log(resp);

var resp2 = mldb.get('/v1/query', { q: 'select * from (select * from reddit) limit 10',
                                    format: 'sparse' });


mldb.log(resp2);

assertEqual(resp, resp2);

"success"

