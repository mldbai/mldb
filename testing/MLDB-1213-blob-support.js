// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}


function testQuery(query, expected) {
    mldb.log("testing query", query);

    var resp = mldb.get('/v1/query', {q: query, format: 'table'});

    mldb.log("received", resp.json);
    mldb.log("expected", expected);
    
    assertEqual(resp.responseCode, 200);
    assertEqual(resp.json, expected);
}

assertEqual(mldb.query("SELECT base64_encode('hello123') AS x")[0].columns[0][1], "aGVsbG8xMjM=");
assertEqual(mldb.query("SELECT base64_decode(base64_encode('hello')) AS x")[0].columns[0][1], "hello");

"success"


