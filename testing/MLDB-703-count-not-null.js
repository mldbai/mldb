// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var dataset1 = mldb.createDataset({type:'sparse.mutable',id:'test1'});

var ts = new Date("2015-01-01");

dataset1.recordRow("ex1", [ [ "x", 1, ts ], ["y", 2, ts] ]);
dataset1.recordRow("ex2", [ [ "x", 2, ts ], ["z", 4, ts] ]);
dataset1.recordRow("ex3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset1.commit()

var resp = mldb.get('/v1/query', { q: 'select count({*}) as c from test1 group by 1', format: 'table' });
assertEqual(resp.responseCode, 200);
var expected = [
    [ "_rowName", "c.x", "c.y", "c.z" ],
    [ "[1]", 2, 1, 2 ]
];
assertEqual(resp.json, expected)

//  MLDB-1256
var resp1 = mldb.get('/v1/query', { q: 'select x, count(x) as a from test1 where x is not null group by x', format: 'table' });
mldb.log(resp1.json);
assertEqual(resp1.responseCode, 200);
var resp2 = mldb.get('/v1/query', { q: 'select x, count(*) as a from test1 where x is not null group by x', format: 'table' });
mldb.log(resp2.json);
assertEqual(resp2.responseCode, 200);

assertEqual(resp1.json, resp2.json)

"success"
