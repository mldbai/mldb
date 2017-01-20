// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var status = "success";
var numRun = 0;
var numFailed = 0;

function assertEqual(expr, val, msg, info)
{
    numRun += 1;

    if (expr != val) {
        numFailed += 1;
        var msg = "Assertion failure: " + msg + ": " + expr + " not equal to " + val;

        if (val) {
            plugin.log(msg);
            if (info)
                plugin.log(info);
        }
        
    }
    
    if (numFailed != 0)
        status = numFailed + " of " + numRun + " failed";
}

var resp = mldb.put("/v1/procedures/null", {type: "null"});

assertEqual(resp.responseCode, 201, "Procedure creation", resp);

// Put without run name should throw

var resp = mldb.put("/v1/procedures/null/runs", {});
assertEqual(resp.responseCode, 404, "PUT with no name", resp);

// Put of run config with type should work
var resp = mldb.put("/v1/procedures/null/runs/test5", {});
assertEqual(resp.responseCode, 201, "PUT with correct params", resp);

// Put of run config with wrong id should not work
var resp = mldb.put("/v1/procedures/null/runs/test2", {id: "test1"});
assertEqual(resp.responseCode, 400, "PUT with wrong id", resp);

// Post of run config with empty id should work
var resp = mldb.post("/v1/procedures/null/runs", {});
assertEqual(resp.responseCode, 201, "POST with empty id", resp);

// Post of run config with id should work
var resp = mldb.post("/v1/procedures/null/runs", {id: "test3"});
assertEqual(resp.responseCode, 201, "POST with real id", resp);

status;
