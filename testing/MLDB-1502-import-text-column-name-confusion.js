// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

// Contents of fixture:
// a,b,c.a,c.b,"""d.a"""
// 1,2,3,4,5


var importConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: 'https://s3.amazonaws.com/public.mldb.ai/reddit.csv.gz',
        outputDataset: { id: 'reddit_text_file' },
        limit: 1000,
        delimiter: "",
        quotechar: "",
        headers: ['customLine']
    }
};

var resp = mldb.put('/v1/procedures/import', importConfig);

mldb.log(resp);

assertEqual(resp.responseCode, 201);

"success"
