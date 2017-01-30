// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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
        dataFileUrl: "file://mldb/testing/dataset/MLDB-1638.csv",
        runOnCreation: true,
        outputDataset: 'unstructured',
        structuredColumnNames: false
    }
};

var resp = mldb.put('/v1/procedures/importUnstructured', importConfig);

mldb.log(resp);

assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', { q: 'select * from unstructured', format: 'table' });

var expected =  [
    [ "_rowName", "\"\"\"d.a\"\"\"", "a", "b", "\"c.a\"", "\"c.b\"" ],
    [ "2", 5, 1, 2, 3, 4 ]
];

assertEqual(resp.json, expected);

var importConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: "file://mldb/testing/dataset/MLDB-1638.csv",
        runOnCreation: true,
        outputDataset: 'structured',
        structuredColumnNames: true
    }
};

var resp = mldb.put('/v1/procedures/importStructured', importConfig);

assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', { q: 'select * from structured', format: 'table' });

mldb.log(resp);

var expected = [
    [ "_rowName", "a", "b", "c.a", "c.b", "\"d.a\"" ],
    [ "2", 1, 2, 3, 4, 5 ]
];

assertEqual(resp.json, expected);

"success"

