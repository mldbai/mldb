// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

unittest.assertEqual(mldb.query("SELECT base64_encode('hello123') AS x")[0].columns[0][1], "aGVsbG8xMjM=");
unittest.assertEqual(mldb.query("SELECT base64_decode(base64_encode('hello')) AS x")[0].columns[0][1], "hello");

"success"


