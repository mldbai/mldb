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


function testQuery(query, expected, sortColumns) {
    if (sortColumns === undefined)
        sortColumns = false;
    mldb.log("testing query", query);

    var resp = mldb.get('/v1/query', {q: query, format: 'table', sortColumns: sortColumns});

    mldb.log("received", resp.json);
    mldb.log("expected", expected);
    
    assertEqual(resp.responseCode, 200);
    assertEqual(resp.json, expected);
}


var dataset1 = mldb.createDataset({type:'sparse.mutable',id:'test1'});
var dataset2 = mldb.createDataset({type:'sparse.mutable',id:'test2'});

var ts = new Date("2015-01-01");

dataset1.recordRow("ex1", [ [ "x", 1, ts ], ["y", 2, ts] ]);
dataset1.recordRow("ex2", [ [ "x", 2, ts ], ["z", 4, ts] ]);
dataset1.recordRow("ex3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset2.recordRow("ex4", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset2.recordRow("ex5", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset2.recordRow("ex6", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset1.commit()
dataset2.commit()

var expected = [
   [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z", "test1.z" ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2, null ],
   [ "[ex1]-[ex5]", 1, 2, 2, 2, null ],
   [ "[ex1]-[ex6]", 1, 2, null, 3, null ],
   [ "[ex2]-[ex4]", 2, null, 1, 2, 4 ],
   [ "[ex2]-[ex5]", 2, null, 2, 2, 4 ],
   [ "[ex2]-[ex6]", 2, null, null, 3, 4 ],
   [ "[ex3]-[ex4]", null, null, 1, 2, 3 ],
   [ "[ex3]-[ex5]", null, null, 2, 2, 3 ],
   [ "[ex3]-[ex6]", null, null, null, 3, 3 ]
];

testQuery('select * from test1 join test2 order by rowName()', expected);

testQuery('select * from test1 join test2 on true order by rowName()', expected);
testQuery('select * from test1 join test2 on true and true and (test1.x = test1.x or test1.x is null) and (test1.y = test1.y or test1.y is null) order by rowName()', expected);
testQuery('select * from test1 join test2 on true and false and (test1.x = test1.x or test1.x is null) and (test1.y = test1.y or test1.y is null) order by rowName()', [["_rowName"]]);
testQuery('select * from test1 join test2 on true and (test1.x = test1.x and test1.x is null) and (test1.y = test1.y and test1.y is null) order by rowName()', [["_rowName"]]);

var expected = [
   [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2 ]
];

testQuery('select * from test1 join test2 on test1.x = test2.x' +
          ' and test1.y is not null', expected);
testQuery('select * from test1 join test2 on test1.x = test2.x and test1.x = test2.x' +
          ' and test1.y is not null', expected);
testQuery('select * from test1 join test2 on test1.x = test2.x and test1.x != test2.x' +
          ' and test1.y is not null', [["_rowName"]]);
testQuery('select * from test1 join test2 on test1.x = test2.x and test1.x = test2.x' +
          ' and test1.y is not null where {*}', expected);


expected = [
   [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z", "test1.z" ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2, null ],
   [ "[ex2]-[ex5]", 2, null, 2, 2, 4 ]
];

testQuery('select * from test1 join test2 on test1.x = test2.x',
          expected);

expected = [
   [ "_rowName", "t1.x", "t1.z", "t2.x", "t2.z", "t3.x", "t3.z" ],
   [ "[ex2]-[ex5]-[ex5]", 2, 4, 2, 2, 2, 2 ]
];

testQuery('SELECT * FROM test1 AS t1 JOIN test2 AS t2 ON t1.x = t2.x' +
          ' JOIN test2 AS t3 ON t1.z = t3.x * 2', expected);

expected = [
    [ "_rowName", "t1.x", "t2.x", "t2.z" ],
    [ "[ex1]-[ex4]", 1, 1, 2],
    [ "[ex1]-[ex5]", 1, 2, 2],
    [ "[ex2]-[ex4]", 2, 1, 2],
    [ "[ex2]-[ex5]", 2, 2, 2],
    [ "[ex3]-[ex4]", null, 1, 2],
    [ "[ex3]-[ex5]", null, 2, 2]];
testQuery(
    'SELECT t1.x, t2.x, t2.z FROM test1 AS t1 JOIN test2 AS t2 ON t2.z = 2' +
    ' ORDER BY rowName()', expected);

expected = expected.slice(0,5);
testQuery(
    'SELECT t1.x, t2.x, t2.z FROM test1 AS t1 JOIN test2 AS t2 ON t2.z = 2' +
    ' AND t1.x IS NOT NULL ORDER BY rowName()', expected);

// same as previous
testQuery(
    'SELECT t1.x, t2.x, t2.z FROM test1 AS t1 JOIN test2 AS t2 ON t2.z = 2' +
    ' WHERE t1.x IS NOT NULL ORDER BY rowName()', expected);

expected = [
    [ "_rowName", "t1.x", "t2.x", "t3.x" ],
    ['[ex1]-[ex4]-[ex4]', 1, 1, 1],
    ['[ex1]-[ex4]-[ex5]', 1, 1, 2],
    ['[ex2]-[ex5]-[ex4]', 2, 2, 1],
    ['[ex2]-[ex5]-[ex5]', 2, 2, 2]];

testQuery(
    'SELECT t1.x, t2.x, t3.x FROM test1 as t1' +
    ' JOIN test2 as t2 ON t2.x = t1.x' +
    ' JOIN test2 as t3 ON t3.z = 2' +
    ' ORDER BY rowName()', expected);

// some join through a sql.query function (without AS)
var resp = mldb.put('/v1/functions/poil', {
    'type': 'sql.query',
    'params': {
        'query': 'select * from test1 join test2 on test1.x = test2.x order by rowName()'
    }
});
assertEqual(resp.responseCode, 201);

// (with the AS)
var resp = mldb.put('/v1/functions/poil_as', {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': '*',
            'from': 'test1 as t1 join test2 as t2 on t1.x = t2.x',
            'orderBy': 'rowName()'
        }
    }
});
assertEqual(resp.responseCode, 201);

testQuery(
    'SELECT poil() as *',
    [
        [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
        [ "result", 1, 2, 1, 2 ]
    ]);

testQuery(
    'SELECT poil_as() as *',
    [
        [ "_rowName", "t1.x", "t1.y", "t2.x", "t2.z" ],
        [ "result", 1, 2, 1, 2 ]
    ]);

// almost same again but with a groupBy
var resp = mldb.put('/v1/functions/poil_group', {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': 't1.x, max(t1.y), min(t3.x), rowName() as rn',
            'from': 'test1 as t1' +
                ' join test2 as t2 on t1.x = t2.x' +
                ' join test2 as t3 on t3.x = 1',
            'groupBy': 't1.x',
            'orderBy': 'rowName()'
        }
    }
});
assertEqual(resp.responseCode, 201);

mldb.log("testing query poil_group");
testQuery(
    'SELECT poil_group() as *',
    [
        [ "_rowName", "\"max(t1.y)\"", "\"min(t3.x)\"", "rn", "t1.x" ],
        [ "result", 2, 1, "[1]", 1 ]
    ]);

// big example with where and groupby
// two identical functions, but one without parameters, and one with patameters
var resp = mldb.put('/v1/functions/patate', {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': 't1.x, max(t1.y), min(t3.x)',
            'from': 'test1 as t1' +
                ' join test2 as t2 on t1.x = t2.x' +
                ' join test2 as t3 on t3.x = 1',
            'where': 't1.x = 1',
            'groupBy': 't1.x'
        }
    }
});
assertEqual(resp.responseCode, 201);

var resp = mldb.put('/v1/functions/patate_params', {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': 't1.x, max(t1.y), min(t3.x)',
            'from': 'test1 as t1' +
                ' join test2 as t2 on t1.x = t2.x' +
                ' join test2 as t3',
            'where': 't1.x = $b and t3.x = $a',
            'groupBy': 't1.x'
        }
    }
});

mldb.log(resp.json);
assertEqual(resp.responseCode, 201);

// MLDB-845... 
var resp = mldb.put('/v1/functions/patate_params_on_clause', {
    'type': 'sql.query',
    'params': {
        'query' : {
            'select': 't1.x, max(t1.y), min(t3.x)',
            'from': 'test1 as t1' +
                ' join test2 as t2 on t1.x = t2.x' +
                ' join test2 as t3 on t3.x = $a',
            'where': 't1.x = $b',
            'groupBy': 't1.x'
        }
    }
});

mldb.log(resp.json);
assertEqual(resp.responseCode, 201);

// they should return the same thing
var funcs = ['patate' ];
for (var i in funcs) {
    var func = funcs[i];
    mldb.log("testing function " + func);
    testQuery(
        "SELECT " + func +
        "() AS *",
        [
            [ "_rowName", "\"max(t1.y)\"", "\"min(t3.x)\"", "t1.x" ],
            [ "result", 2, 1, 1 ]
        ]);
}

var funcs = ['patate_params', 'patate_params_on_clause' ];
for (var i in funcs) {
    var func = funcs[i];
    mldb.log("testing function " + func);
    testQuery(
        "SELECT " + func +
        "({1 AS a, 1 AS b}) AS *",
        [
            [ "_rowName", "\"max(t1.y)\"", "\"min(t3.x)\"", "t1.x" ],
            [ "result", 2, 1, 1 ]
        ]);
}


// MLDB-1088

expected = [
   [ "_rowName", "test1.x", "test1.y", "test1.z" ],
   [ "[ex1]-[ex4]", 1, 2, null ],
   [ "[ex2]-[ex5]", 2, null, 4 ]
];

testQuery('SELECT test1.* FROM test1 JOIN test2 ON test1.x = test2.x',
          expected);

// MLDB-1189 OUTER JOINS

expected = [
   [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2 ],
   [ "[ex1]-[ex5]", 1, 2, 2, 2 ],
   [ "[ex1]-[ex6]", 1, 2, null, 3 ]
];

testQuery('SELECT * FROM test1 INNER JOIN test2 ON test1.x = 1',
          expected);

expected = [
   [ "_rowName", "test1.x", "test1.z", "test1.y", "test2.x", "test2.z" ],
   [ "[ex2]-[]", 2, 4, null, null, null ],
   [ "[ex3]-[]", null, 3, null, null, null ],
   [ "[ex1]-[ex4]", 1, null, 2, 1, 2 ],
   [ "[ex1]-[ex5]", 1, null, 2, 2, 2 ],
   [ "[ex1]-[ex6]", 1, null, 2, null, 3 ]
];

testQuery('SELECT * FROM test1 LEFT JOIN test2 ON test1.x = 1',
          expected);

expected = [
   [ "_rowName", "test1.x", "test1.y", "test2.x", "test2.z" ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2 ],
   [ "[ex1]-[ex5]", 1, 2, 2, 2 ],
   [ "[ex1]-[ex6]", 1, 2, null, 3 ]
];

testQuery('SELECT * FROM test1 RIGHT JOIN test2 ON test1.x = 1',
          expected);

expected = [
   [ "_rowName", "test2.x", "test2.z", "test1.x", "test1.y", "test1.z" ],
   [ "[]-[ex5]", 2, 2, null, null, null ],
   [ "[]-[ex6]", null, 3, null, null, null ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2, null ],
   [ "[ex2]-[ex4]", 1, 2, 2, null, 4 ],
   [ "[ex3]-[ex4]", 1, 2, null, null, 3 ]
];

testQuery('SELECT * FROM test1 RIGHT JOIN test2 ON test2.x = 1',
          expected);

expected = [
   [ "_rowName", "test1.x", "test1.z", "test2.x", "test2.z", "test1.y" ],
   [ "[ex2]-[]", 2, 4, null, null, null ],
   [ "[ex3]-[]", null, 3, null, null, null ],
   [ "[]-[ex5]", null, null, 2, 2, null ],
   [ "[]-[ex6]", null, null, null, 3, null ],
   [ "[ex1]-[ex4]", 1, null, 1, 2, 2 ]
];

testQuery('SELECT * FROM test1 FULL JOIN test2 ON test1.x = 1 AND test2.x = 1',
          expected);

testQuery('SELECT * FROM test1 OUTER JOIN test2 ON test1.x = 1 AND test2.x = 1',
          expected);

testQuery('SELECT * FROM test1 FULL OUTER JOIN test2 ON test1.x = 1 AND test2.x = 1',
          expected);

expected = [
   [ "_rowName", "test1.x", "test1.z", "test2.x", "test2.z", "test1.y" ],
   [ "[ex3]-[]", null, 3, null, null, null ],
   [ "[]-[ex6]", null, null, null, 3, null ],
   [ "[ex1]-[ex4]", 1, null, 1, 2, 2 ],
   [ "[ex2]-[ex5]", 2, 4, 2, 2, null ]
];

testQuery('SELECT * FROM test1 FULL JOIN test2 ON test1.x = test2.x',
          expected);

expected = [
   [ "_rowName", "test2.x", "test2.z", "test1.x", "test1.z", "test1.y" ],
   [ "[]-[ex5]", 2, 2, null, null, null ],
   [ "[]-[ex6]", null, 3, null, null, null ],
   [ "[ex3]-[]", null, null, null, 3, null ],
   [ "[ex1]-[ex4]", 1, 2, 1, null, 2 ],
   [ "[ex2]-[]", null, null, 2, 4, null ]
];

testQuery('SELECT * FROM test1 FULL JOIN test2 ON (test1.x = test2.x) AND (test2.x != 2)',
          expected);

expected = [
   [ "_rowName", "test1.x", "test1.z", "test1.y", "test2.x", "test2.z" ],
   [ "[ex3]-[]", null, 3, null, null, null ],
   [ "[ex1]-[ex4]", 1, null, 2, 1, 2 ],
   [ "[ex2]-[]", 2, 4, null, null, null ]
];

testQuery('SELECT * FROM test1 LEFT JOIN test2 ON test1.x = test2.x AND test2.x != 2',
          expected);

testQuery('SELECT * FROM test1 LEFT OUTER JOIN test2 ON test1.x = test2.x AND test2.x != 2',
          expected);

expected = [
   [ "_rowName", "test2.x", "test2.z", "test1.x", "test1.y" ],
   [ "[]-[ex5]", 2, 2, null, null ],
   [ "[]-[ex6]", null, 3, null, null ],
   [ "[ex1]-[ex4]", 1, 2, 1, 2 ]
];

testQuery('SELECT * FROM test1 RIGHT JOIN test2 ON test1.x = test2.x AND test2.x != 2',
          expected);

testQuery('SELECT * FROM test1 RIGHT OUTER JOIN test2 ON test1.x = test2.x AND test2.x != 2',
          expected);

// MLDB-1255

mldb.log("row name queries");

var dataset3 = mldb.createDataset({type:'sparse.mutable',id:'test3'});
var dataset4 = mldb.createDataset({type:'sparse.mutable',id:'test4'});
var dataset5 = mldb.createDataset({type:'sparse.mutable',id:'test5'});

var ts = new Date("2015-01-01");

dataset3.recordRow("ex1", [ [ "x", 1, ts ], ["y", 2, ts] ]);
dataset3.recordRow("ex2", [ [ "x", 2, ts ], ["z", 4, ts] ]);
dataset3.recordRow("ex3", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset4.recordRow("ex3", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset4.recordRow("ex4", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset4.recordRow("ex5", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset5.recordRow("ex5", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset5.recordRow("ex6", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset5.recordRow("ex1", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset3.commit()
dataset4.commit()
dataset5.commit()

expected = [
   [ "_rowName", "test3.x", "test3.y", "test3.z", "test4.x", "test4.z" ],
   [ "[ex1]-[]", 1, 2, null, null, null ],
   [ "[ex2]-[]", 2, null, 4, null, null ],
   [ "[ex3]-[ex3]", null, null, 3, 1, 2 ],
   [ "[]-[ex4]", null, null, null, 2, 2 ],
   [ "[]-[ex5]", null, null, null, null, 3 ]
];

testQuery('SELECT * FROM test3 OUTER JOIN test4 ON test3.rowName() = test4.rowName()',
          expected);

expected = [
   [ "_rowName", "test4.x", "test4.z", "test3.x", "test3.y", "test5.x", "test5.z", "test3.z"],
   [ "[]-[ex4]-[]", 2, 2, null, null, null, null, null ],
   [ "[]-[ex5]-[]", null, 3, null, null, null, null, null ],
   [ "[ex1]-[]-[ex1]", null, null, 1, 2, null, 3, null ],
   [ "[ex2]-[]-[]", null, null, 2, null, null, null, 4 ],
   [ "[ex3]-[ex3]-[]", 1, 2, null, null, null, null, 3 ],
   [ "[]-[]-[ex5]", null, null, null, null, 1, 2, null ],
   [ "[]-[]-[ex6]", null, null, null, null, 2, 2, null ]
];

testQuery('SELECT * FROM test3 OUTER JOIN test4 ON test3.rowName() = test4.rowName() OUTER JOIN test5 on test3.rowName() = test5.rowName()',
          expected);

var dataset6 = mldb.createDataset({type:'sparse.mutable',id:'test6'});
dataset6.recordRow("ex3", [ [ "x", 1, ts ], ["z", 2, ts] ]);
dataset6.recordRow("ex4", [ [ "x", 2, ts ], ["z", 2, ts] ]);
dataset6.recordRow("ex6", [ [ "x", null, ts ], ["z", 3, ts] ]);

dataset6.commit()

expected = [
   [ "_rowName","test3.x","test3.y","test3.z","test4.x","test4.z","test5.x","test5.z","test6.x","test6.z"],
   [ "[]-[]-[]-[ex4]", null, null, null, null, null, null, null, 2, 2 ],
   [ "[]-[]-[]-[ex6]", null, null, null, null, null, null, null, null, 3 ],
   [ "[]-[]-[ex5]-[]", null, null, null, null, null, 1, 2, null, null ],
   [ "[]-[]-[ex6]-[]", null, null, null, null, null, 2, 2, null, null ],
   [ "[]-[ex4]-[]-[]", null, null, null, 2, 2, null, null, null, null ],
   [ "[]-[ex5]-[]-[]", null, null, null, null, 3, null, null, null, null ],
   [ "[ex1]-[]-[ex1]-[]", 1, 2, null, null, null, null, 3, null, null ],
   [ "[ex2]-[]-[]-[]", 2, null, 4, null, null, null, null, null, null ],
   [ "[ex3]-[ex3]-[]-[ex3]", null, null, 3, 1, 2, null, null, 1, 2 ]
];


testQuery('SELECT * FROM test3 OUTER JOIN test4 ON test3.rowName() = test4.rowName() OUTER JOIN test5 on test3.rowName() = test5.rowName() OUTER JOIN test6 on test3.rowName() = test6.rowName() ORDER BY rowName()',
          expected, true /* sortColumns */);

//MLDB-1384
//asking for unknow column in the WHERE should not throw.
expected = [[ "_rowName" ]];

testQuery('SELECT 1 FROM (SELECT 2) as a OUTER JOIN (SELECT 2) as b WHERE x',
          expected);

//MLDB-1559
expected = [[ "_rowName" ]];

testQuery('SELECT * FROM test1 as patate JOIN test2 as banane where patate.rowName() in (3, banane.x)',
          expected);

var dataset7 = mldb.createDataset({type:'sparse.mutable',id:'test7'});
dataset7.recordRow("x", [[ "k", 1, ts ]] );
dataset7.recordRow("blah", [[ "k", 1, ts ]] );

dataset7.commit()

// Test that "table2.* as *" returns the right thing (non-prefixed names)
expected = [
   [ "_rowName", "x", "z" ],
   [ "[blah]-[ex4]", 1, 2 ],
   [ "[blah]-[ex5]", 2, 2 ],
   [ "[blah]-[ex6]", null, 3 ],
   [ "[x]-[ex4]", 1, 2 ],
   [ "[x]-[ex5]", 2, 2 ],
   [ "[x]-[ex6]", null, 3 ]
];

testQuery('SELECT table2.* as * FROM test7 as table1 JOIN test2 as table2',
          expected);

expected = [
   [ "_rowName", "table1.k", "table2.x", "table2.z" ],
   [ "[x]-[ex4]", 1, 1, 2 ],
   [ "[x]-[ex5]", 1, 2, 2 ],
   [ "[x]-[ex6]", 1, null, 3 ]];

// This one matches nothing, because the keys of table2.* all start with table2.
testQuery('SELECT * FROM test7 as table1 JOIN test2 as table2 where table1.rowName() IN (KEYS OF ({table2.*}))',
          [["_rowName"]]);

testQuery('SELECT * FROM test7 as table1 JOIN test2 as table2 where table1.rowName() IN (KEYS OF ({table2.* as *}))',
          expected);

"success"
