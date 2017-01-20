// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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


var dataset1 = mldb.createDataset({type:'sparse.mutable',id:'test1'});

var ts = new Date("2015-01-01");

dataset1.recordRow("all systems", [ ["value", "GO", ts] ]);
dataset1.recordRow("hello", [ ["value", "world", ts] ]);

dataset1.commit()

var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, rowName() AS column from test1 order by rowName()',
        output: 'NAMED_COLUMNS'
    }
});
assertEqual(resp.responseCode, 201);


var expected = [
    [ "_rowName", "all systems", "hello" ],
    [ "result", "GO", "world" ]
];

testQuery('SELECT poil()[output] as *', expected);



// Test with a limit only
var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, rowName() AS column from test1 order by rowName() LIMIT 1',
        output: 'NAMED_COLUMNS'
    }
});

mldb.log(resp);

assertEqual(resp.responseCode, 201);

var expected = [
    [ "_rowName", "all systems" ],
    [ "result", "GO" ]
];

testQuery('SELECT poil()[output] as *', expected);




// Test with a limit and offset
var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, rowName() AS column from test1 order by rowName() LIMIT 1 OFFSET 1',
        output: 'NAMED_COLUMNS'
    }
});

mldb.log(resp);

assertEqual(resp.responseCode, 201);

var expected = [
    [ "_rowName", "hello" ],
    [ "result", "world" ]
];

testQuery('SELECT poil()[output] as *', expected);


// Test that a doubled column gives an error
var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, rowName() AS column, rowName() as column from test1 order by rowName()',
        output: 'NAMED_COLUMNS'
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', {q: "select poil()[output] as *", format: 'table'});

mldb.log(resp);
assertEqual(resp.responseCode, 400);

//assertEqual(resp.json.error.indexOf("must contain exactly one") != -1, true);


// Test that an extra column gives an error
var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, rowName() AS column, 1 as xxx from test1 order by rowName()',
        output: 'NAMED_COLUMNS'
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', {q: "select poil()[output] as *", format: 'table'});

mldb.log(resp);
assertEqual(resp.responseCode, 400);

assertEqual(resp.json.error.indexOf("can only contain") != -1, true);


// Test that a null column name gives an error
var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'select *, NULL AS column from test1 order by rowName()',
        output: 'NAMED_COLUMNS'
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', {q: "select poil()[output] as *", format: 'table'});

mldb.log(resp);
assertEqual(resp.responseCode, 400);

assertEqual(resp.json.error.indexOf("can't be null") != -1, true);

"success"
