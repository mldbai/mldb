// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

var dataset_config = {
    type: 'sparse.mutable',
    id: 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0);
recordExample("ex2", 1, 1);
recordExample("ex3", 2, 2);
recordExample("ex4", 3, 3);

dataset.commit()


var functionConfig = {
    id: "expr",
    type: "llvm.expression",
    params: {
        expression: "x + y AS z"
    }
};

var res = mldb.post("/v1/functions", functionConfig);

if (res.responseCode != 201)
    throw "Error creating function: " + res.response;

var resp = mldb.get("/v1/functions/expr/info").json;

plugin.log(resp);

resp = mldb.get("/v1/functions/expr/application",
                    {input:{x:1, y:2}});

plugin.log(resp);

assertEqual(resp.json.output.z, 3);

resp = mldb.get("/v1/query",
                {q: "SELECT expr({*}) AS * FROM test ORDER BY rowName()",
                 format: 'table'});

plugin.log(resp);

var expected = [
    [ "_rowName", "z" ],
    [ "ex1", 0 ],
    [ "ex2", 2 ],
    [ "ex3", 4 ],
    [ "ex4", 6 ]
];

assertEqual(resp.json, expected);

if (false) {

    var functionConfig2 = {
        id: "query",
        type: "llvm.query",
        params: {
            query: {
                select: "x + y + $offset AS z",
                from: { id: 'test' },
                where: 'rowName() = $row'
            }
        }
    };

    res = mldb.post("/v1/functions", functionConfig2);

    if (res.responseCode != 201)
        throw "Error creating function: " + res.response;

    var resp = mldb.get("/v1/functions/query/application",
                        {input:{row: 'ex1', offset:1}});

    plugin.log(resp);

    assertEqual(resp.json.output.z, 1);

    var resp2 = mldb.get("/v1/functions/query/application",
                         {input:{row: 'ex2', offset:100}});

    plugin.log(resp2);

    assertEqual(resp2.json.output.z, 102);
}

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
