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


var dataset1 = mldb.createDataset({type:'sparse.mutable',id:'test1'});
var dataset2 = mldb.createDataset({type:'sparse.mutable',id:'test2'});

var ts = new Date("2015-01-01");

dataset1.recordRow("1", [ [ "x", 1, ts ], ["y", 2, ts] ]);
dataset1.recordRow("2", [ [ "x", 2, ts ], ["z", 4, ts] ]);
dataset1.recordRow("3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset2.recordRow("1", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset2.recordRow("2", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset2.recordRow("3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset1.commit()
dataset2.commit()

var resp = mldb.put('/v1/functions/poil', {
    'type': 'sql.query',
    'params': {
        'query': 'select * from test1 join test2 on test1.rowName() = test2.rowName() order by rowName()'
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.put('/v1/functions/poil2', {
    'type': 'sql.query',
    'params': {
        'query': 'select * from test1 join test2 on cast (test1.rowName() as integer) = cast (test2.rowName() as integer) order by rowName()'
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.put('/v1/functions/poil3', {
    'type': 'sql.query',
    'params': {
        'query': 'select * from test1 join test2 on cast (test1.rowName() as integer) = cast(test2.rowName() as integer) + $n order by rowName()'
    }
});
assertEqual(resp.responseCode, 201);

var expected = [
    [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
    [ "result", 1, 2, 1, 2 ]
];

testQuery('SELECT poil() as *', expected);
testQuery('SELECT poil2() as *', expected);

var expected2 = [
    [ "_rowName", "test1.x", "test1.z", "test2.x", "test2.z" ],
    [ "result", 2, 4, 1, 2 ]
];

testQuery('SELECT poil3({n:1}) as *', expected2);


mldb.log(resp);

"success"
