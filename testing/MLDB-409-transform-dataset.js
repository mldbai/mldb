// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var dataset = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

function recordExample(row, x, y, label)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0, "cat");
recordExample("ex2", 1, 1, "dog");
recordExample("ex3", 1, 2, "cat");
recordExample("ex4", 6, 6, "poil");

dataset.commit()


function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndRunProcedure(config, name)
{
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded(name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.post("/v1/procedures/" + name + "/runs", {});
    assertSucceeded("procedure running", trainingOutput);
}

// transform our 4 elements with limit=3 (MLDB-799)
// Note: Transform is not ordering-stable.
var transform_config = {
    type: 'transform',
    params: {
        inputData: {
            select: "x, y, x * 10 AS z, y + 6 AS q",
            from: 'test',
            named: "rowName() + '_transformed'",
            limit: 3
        },
        outputDataset: { id: 'transformed', type: 'sparse.mutable' },
        runOnCreation : false
    }
};

createAndRunProcedure(transform_config, "transform");

var resp = mldb.get("/v1/query", {q: 'SELECT x,y,z,q from transformed order by rowName()',format: 'table'});

plugin.log("transform limit 3 query result", resp.json);

var expected = [
   [ "_rowName", "q", "x", "y", "z" ],
   [ "ex2_transformed", 7, 1, 1, 10 ],
   [ "ex3_transformed", 8, 1, 2, 10 ],
   [ "ex4_transformed", 12, 6, 6, 60 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");

// transform our 4 elements with orderby rowName()
var transform_config2 = {
    type: 'transform',
    params: {
        inputData: {
            select: 'x, y, x * 10 AS z, y + 6 AS q',
            from : 'test',
            orderBy: "rowName()",
            named: "rowName() + '_transformed'"
        },
        outputDataset: { id: 'transformed2', type: 'sparse.mutable' },
        runOnCreation : false        
    }

};

createAndRunProcedure(transform_config2, "transform2");

var resp = mldb.get("/v1/query", {q: 'select x,y,z,q from transformed2 order by rowName()', format: 'table'});

plugin.log(resp);

var expected = [
    [ "_rowName", "q", "x", "y", "z" ],
    [ "ex1_transformed", 6, 0, 0, 0 ],
    [ "ex2_transformed", 7, 1, 1, 10 ],
    [ "ex3_transformed", 8, 1, 2, 10 ],
    [ "ex4_transformed", 12, 6, 6, 60 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");

// transform and skip empty rows

var dataset2 = mldb.createDataset({type:'sparse.mutable',id:'test2'});

function recordExample2(row, x, y, label)
{
    dataset2.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

dataset2.recordRow("ex1", [ [ "x", 1, ts ], ["y", 2, ts]]);
dataset2.recordRow("ex2", [ ["y", 3, ts]]);
dataset2.recordRow("ex3", [ [ "x", 4, ts ]]);

dataset2.commit()

var transform_config3 = {
    type: 'transform',
    params: {
        inputData: {
            select: 'x',
            from: 'test2',
            orderBy: "rowName()",
            named: "rowName() + '_transformed'"
        },
        outputDataset: { id: 'transformed3', type: 'sparse.mutable' },
        skipEmptyRows: true,
        runOnCreation : false
    }
};

createAndRunProcedure(transform_config3, "transform3");

var resp = mldb.get("/v1/query", {q: 'select * from transformed3 order by rowName()', format: 'table'});

plugin.log(resp);

var expected = [
    [ "_rowName", "x"],
    [ "ex1_transformed", 1],
    [ "ex3_transformed", 4]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");


var transform_config4 = {
    type: 'transform',
    params: {
        inputData: {
            select: 'y',
            from: 'test2',
            orderBy: "rowName()",
            named: "rowName() + '_transformed'",
            groupBy: "y"
        },
        outputDataset: { id: 'transformed4', type: 'sparse.mutable' },
        skipEmptyRows: true,
        runOnCreation : false        
    }

};

createAndRunProcedure(transform_config4, "transform4");

var resp = mldb.get("/v1/query", {q: 'select * from transformed4 order by rowName()', format: 'table'});

plugin.log(resp);

var expected = [
    [ "_rowName", "y" ],
    [ "[2]_transformed", 2 ],
    [ "[3]_transformed", 3 ]
];

assertEqual(mldb.diff(expected, resp.json, false), {},
            "Output was not the same as expected output");

function runTransformWithNoFrom(query, expected) {
    var transform_config_no_from = {
        type: 'transform',
        params: {
            inputData: query,
            outputDataset: { id: 'transformed_no_from', type: 'sparse.mutable' },
            skipEmptyRows: true,
            runOnCreation : false
        }
    };

    createAndRunProcedure(transform_config_no_from, "transform_no_from");

    var resp = mldb.get("/v1/query", {q: 'select * from transformed_no_from', format: 'table'});

    plugin.log(resp);

    assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
                "Output was not the same as expected output");
}

runTransformWithNoFrom("select 1 as col", [
    [ "_rowName", "col"],  [ "result", 1]
]);

runTransformWithNoFrom("select 1 as col named 'row'", [
    [ "_rowName", "col"],  [ "row", 1]
]);

runTransformWithNoFrom("select 1+1 as col named 'row'", [
    [ "_rowName", "col"],  [ "row", 2]
]);

runTransformWithNoFrom("select to_timestamp('2015-11-25') + INTERVAL '1 month' as chrismas", [
    [ "_rowName", "chrismas"],  [ "result", "2015-12-25T00:00:00Z"]
]);

runTransformWithNoFrom("select now() - to_timestamp('2015-01-01') > INTERVAL '454D' as col", [
    [ "_rowName", "col"],  [ "result", true]
]);


"success"
