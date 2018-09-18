// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')


function testQuery(query, expected) {
    mldb.log("testing query", query);

    var resp = mldb.get('/v1/query', {q: query, format: 'table'});

    mldb.log("received", resp.json);
    mldb.log("expected", expected);
    
    unittest.assertEqual(resp.responseCode, 200);
    unittest.assertEqual(resp.json, expected);
}

var resp = mldb.put('/v1/functions/poil', {
    type: 'sql.query',
    params: {
        query: 'SELECT upper(column) AS column, value FROM row_dataset($input) WHERE CAST (value AS NUMBER) IS NULL',
        output: 'NAMED_COLUMNS'
    }
});

mldb.log(resp);

unittest.assertEqual(resp.responseCode, 201);

var expected = [
    [ "_rowName", "Z" ],
    [ "result", "three" ]
];

testQuery("SELECT poil({input: {x: 1, y: 2, z: 'three'}})[output] as *", expected);

// MLDB-1374
expected = [
   [ "_rowName", "column", "value" ],
   [ "0", "x", 1 ],
   [ "1", "y", 2 ],
   [ "2", "z", "three" ]
];

testQuery("SELECT * FROM row_dataset({x:1, y:2, z:'three'}) ORDER BY rowName()",
          expected);

expected = [
   [ "_rowName", "column", "value" ],
   [ "0", "x", 1 ],
   [ "1", "y", 2 ],
   [ "2", "z", "three" ]
];
testQuery('SELECT x.* FROM row_dataset({x: 1, y:2, z: \'three\'}) AS x ORDER BY rowName()',
          expected);

var resp = mldb.put("/v1/functions/x", { type: "sql.query", params: { query: "SELECT * from row_dataset({a:1,b:2})", output: "NAMED_COLUMNS"} });

unittest.assertEqual(resp.responseCode, 201);

expected = [
   [ "_rowName", "output.a", "output.b" ],
   [ "result", 1, 2 ]
];

testQuery("SELECT x() AS *", expected);

expected = [
   [ "_rowName", "column", "value" ],
   [ "0", "output.a", 1 ],
   [ "1", "output.b", 2 ]
];

testQuery("SELECT * FROM atom_dataset(x())", expected);

"success"
