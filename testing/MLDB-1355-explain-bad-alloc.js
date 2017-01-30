// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-1010
// Check the error message for std::bad_alloc

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

var dataset = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

var row = 0;

function recordExample(who, what, how)
{
    dataset.recordRow(row++, [ [ "who", who, ts ], ["what", what, ts], ["how", how, ts] ]);
}

recordExample("mustard", "moved", "kitchen");
recordExample("plum", "moved", "kitchen");
recordExample("mustard", "stabbed", "plum");
recordExample("mustard", "killed", "plum");
recordExample("plum", "died", "stabbed");

dataset.commit()


var resp = mldb.get("/v1/query", { q: 'select _fail_memory_allocation()' });

mldb.log(resp.json);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.details.context.error, "Out of memory");

// Check it works in the context of a table query

var resp = mldb.get("/v1/query", { q: 'select *, _fail_memory_allocation() from test' });

mldb.log(resp.json);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.details.context.error, "Out of memory");


// Check that it also works when setting up a join
var resp = mldb.get("/v1/query", { q: 'select * from test as x join test as y on _fail_memory_allocation()' });

mldb.log(resp.json);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.details.context.error, "Out of memory");


"success"
