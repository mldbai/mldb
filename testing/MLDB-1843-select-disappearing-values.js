// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var query = "SELECT tokenize('a,b,c') AS *";
var tokQuery = "SELECT tokenize('a,b,c') AS tok";

var analysis = mldb.get("/v1/query", { q: "SELECT static_type({tokenize('a,b,c') AS tok}) as *", format: 'aos' });

mldb.log(analysis.json);

unittest.assertEqual(analysis.json[0]["hasUnknownColumnsRecursive"], 1);

var resp = mldb.put("/v1/functions/f1", {
    "type": "sql.query",
    "params": {
        "query": query,
    }
});

unittest.assertEqual(resp.responseCode, 201);

var resp = mldb.put("/v1/functions/f2", {
    "type": "sql.query",
    "params": {
        "query": "SELECT * FROM (" + query + ")"
    }
});

unittest.assertEqual(resp.responseCode, 201);

mldb.log("--------------- first query");

var resp1 = mldb.query("SELECT f1() AS *");

mldb.log(resp1);

mldb.log("--------------- second query");

var resp2 = mldb.query("SELECT f2() AS *");

mldb.log(resp2);

unittest.assertEqual(resp1, resp2);

resp = mldb.put("/v1/functions/f3", {
    "type": "sql.query",
    "params": {
        "query": "SELECT tok.* as * FROM (" + tokQuery + ")"
    }
});

unittest.assertEqual(resp.responseCode, 201);

mldb.log("--------------- third query");

var resp3 = mldb.query("SELECT f3() AS *");

mldb.log(resp3);

unittest.assertEqual(resp1, resp3);

resp = mldb.put("/v1/functions/f4", {
    "type": "sql.query",
    "params": {
        "query": "SELECT COLUMN EXPR (AS columnName()) FROM (" + query + ")"
    }
});

unittest.assertEqual(resp.responseCode, 201);

mldb.log("--------------- fourth query");

var resp4 = mldb.query("SELECT f4() AS *");

mldb.log(resp4);

unittest.assertEqual(resp1, resp4);



"success"
