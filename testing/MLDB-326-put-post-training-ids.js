// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var resp = mldb.put("/v1/procedures/null", {type: "null"});

unittest.assertEqual(resp.responseCode, 201, "Procedure creation", resp);

// Put without run name should throw

var resp = mldb.put("/v1/procedures/null/runs", {});
unittest.assertEqual(resp.responseCode, 404, "PUT with no name", resp);

// Put of run config with type should work
var resp = mldb.put("/v1/procedures/null/runs/test5", {});
unittest.assertEqual(resp.responseCode, 201, "PUT with correct params", resp);

// Put of run config with wrong id should not work
var resp = mldb.put("/v1/procedures/null/runs/test2", {id: "test1"});
unittest.assertEqual(resp.responseCode, 400, "PUT with wrong id", resp);

// Post of run config with empty id should work
var resp = mldb.post("/v1/procedures/null/runs", {});
unittest.assertEqual(resp.responseCode, 201, "POST with empty id", resp);

// Post of run config with id should work
var resp = mldb.post("/v1/procedures/null/runs", {id: "test3"});
unittest.assertEqual(resp.responseCode, 201, "POST with real id", resp);

"success";
