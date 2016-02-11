// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Test of SQL expression function. */

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

// Create a serial function that has just one subfunction and default
// with and extract, and make sure it's the same as the sub-
// function.
var functionConfig = {
    type: "serial",
    id: "test0",
    params: {
        steps: [
            {
                type: "sql.expression",
                id: "test0.subfunction",
                params: {
                    expression: "x + y AS z"
                },
                'with': "*",
                extract: "*"
            }
        ]
    }
};

var res = mldb.post("/v1/functions", functionConfig);

if (res.responseCode != 201)
    throw "Error creating function: " + res.response;

plugin.log("config", functionConfig);

var infoSubfunction = mldb.get("/v1/functions/test0.subfunction/info");

var infoParent = mldb.get("/v1/functions/test0/info");

plugin.log("parent", infoParent);
plugin.log("subfunction", infoSubfunction);

assertEqual(infoParent.json.output.values.z.valueInfo.type, "Datacratic::MLDB::AnyValueInfo");


assertEqual(infoParent, infoSubfunction);

var resParent = mldb.get("/v1/functions/test0/application",
                         {input:{x:1, y:2}}).json;
var resSubfunction = mldb.get("/v1/functions/test0.subfunction/application",
                           {input:{x:1, y:2}}).json;

plugin.log(resParent);
plugin.log(resSubfunction);

assertEqual(resParent, resSubfunction);

var resp = mldb.get("/v1/functions/test/application",
                    {input:{x:1, y:2}}).json;



var functionConfig = {
    type: "serial",
    id: "test",
    params: {
        steps: [
            {
                type: "sql.expression",
                params: {
                    expression: "x + y AS z"
                },
                'with': "*",
                extract: "z"
            },
            {
                type: "sql.expression",
                params: {
                    expression: "p + q AS z"
                },
                'with': "z AS p, z AS q",
                extract: "z AS r"
            }
        ]
    }
};

var res = mldb.post("/v1/functions", functionConfig);

if (res.responseCode != 201)
    throw "Error creating function: " + res.response;

var resp = mldb.get("/v1/functions/test/application",
                    {input:{x:1, y:2}}).json;

plugin.log(resp);

var expected = {
    output : {
        "r" : 6,
        "z" : 3
    }
};

assertEqual(resp, expected);

functionConfig = {
    type: "serial",
    id: "test2",
    params: {
        steps: [
            {
                type: "sql.expression",
                params: {
                    expression: "x + y AS z"
                },
                'with': "x,y",
                extract: "*"
            },
            {
                type: "sql.expression",
                params: {
                    expression: "p + q AS r"
                },
                'with': "z AS p, z AS q",
                extract: "*"
            }
        ]
    }
};

var res = mldb.post("/v1/functions", functionConfig);

if (res.responseCode != 201)
    throw "Error creating function: " + res.response;

var resp2 = mldb.get("/v1/functions/test2/application",
                    {input:{x:1, y:2}}).json;

plugin.log(resp2);

assertEqual(resp, expected);


// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
