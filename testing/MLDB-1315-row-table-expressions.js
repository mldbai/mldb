// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

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

var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'SELECT upper(column) AS column, value FROM row_dataset($input) WHERE CAST (value AS NUMBER) IS NULL',
        output: 'NAMED_COLUMNS'
    }
});

mldb.log(resp);

assertEqual(resp.responseCode, 201);

var expected = [
    [ "_rowName", "Z" ],
    [ "", "three" ]
];

testQuery("SELECT poil({input: {x: 1, y: 2, z: 'three'}})[output] as *", expected);

"success"
