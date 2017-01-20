// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/**
 * MLDB-1043-bucketize-procedure.js
 * Mich, 2015-10-27
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.
 **/

var res;
function assertEqual(expr, val, msg) {
    if (expr === val)
        return;
    if (JSON.stringify(expr) === JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);
    mldb.log("--------Latest res start--------");
    mldb.log(res);
    mldb.log("--------Latest res end----------");

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
          + " not equal to " + JSON.stringify(val);
}

function checkResults(result, goldStandard) {
    var rows = result['json'];
    for (var index in rows) {
        var goldValue = goldStandard[rows[index]['rowName']];
        var value = rows[index]['columns'][0][1];
        if (typeof(goldValue) === "string") {
            if (value !== goldValue) {
                throw value + " !== " + goldValue + " for row "
                      + rows[index]['rowName'];
            }
        } else {
            if (goldValue.indexOf(value) === -1) {
                throw value + " not in  " + string(goldValue) + " for row "
                      + rows[index]['rowName'];
            }
        }
    }
}

function runTest(testNumber, buckets, goldStandard) {
    var url = "/v1/procedures/test" + testNumber;
    var datasetName = "test" + testNumber;
    res = mldb.put(url, {
        type: 'bucketize',
        params: {
            inputData: "select 1 from rNamedScores order by score DESC",
            outputDataset: {id : datasetName, type: "sparse.mutable"},
            percentileBuckets: buckets
        }
    });
    assertEqual(res['responseCode'], 201);

    res = mldb.post(url + "/runs", {});
    assertEqual(res['responseCode'], 201);

    res = mldb.get("/v1/query", {q: "SELECT * FROM " + datasetName});
    assertEqual(res['responseCode'], 200);
    checkResults(res, goldStandard);

    res = mldb.get("/v1/query", {q: "SELECT *, latest_timestamp({*}) FROM " + datasetName,
                                 format: "full"});
}

// Create base dataset --------------------------------------------------------
csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-1043-bucketize-data.csv",
        outputDataset: {
            id: "rNamedScores",
        },
        runOnCreation: true,
        encoding: 'latin1',
        named: 'uid',
        select: '* excluding (uid)'
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

res = mldb.get("/v1/datasets/rNamedScores");
assertEqual(res['json']['status']['rowCount'], 7);

var goldStandard;

// Test 1----------------------------------------------------------------------
// 50-50 bucketing
goldStandard = {
    roger      : "b2",
    rim        : "b2",
    rolland    : "b2",
    rantanplan : "b1",
    rudolph    : "b1",
    ricardo    : ["b1", "b2"], // May change bucket since they have equal score
    rita       : ["b1", "b2"]
}
runTest(1, {b1: [0, 50], b2: [50, 100]}, goldStandard);

// Test 2 ---------------------------------------------------------------------
// Rejected procedure, invalid range
res = mldb.put("/v1/procedures/bucketizeMyScoreInvalid", {
    type: 'bucketize',
    params: {
        inputData: "select * from rNamedScores order by score DESC",
        outputDataset: {id : "bucketedScoresInvalid", type: "sparse.mutable"},
        percentileBuckets: {b1: [0, 80], b2: [50, 100]}
    }
});
assertEqual(res['responseCode'], 400);

// Test 3 ---------------------------------------------------------------------
// Empty bucket, range too smal
goldStandard = {
    roger      : "b2",
    rim        : "b2",
    rolland    : "b2",
    rantanplan : "b2",
    rudolph    : "b2",
    ricardo    : "b2",
    rita       : "b2"
}
runTest(3, {b1: [0, 10], b2: [10, 100]}, goldStandard);

// Test 4 ---------------------------------------------------------------------
// Varied size buckets
goldStandard = {
    roger      : "b3",
    rim        : "b2",
    rolland    : "b3",
    rantanplan : "b1",
    rudolph    : "b2",
    ricardo    : "b2",
    rita       : "b2"
}
runTest(4, {b1: [0, 25], b2: [25, 75], b3: [75, 100]}, goldStandard);

// Test 5 ---------------------------------------------------------------------
// Bucket gap
goldStandard = {
    roger      : "b3",
    rolland    : "b3",
    rantanplan : "b1",
}
runTest(5, {b1: [0, 25], b3: [75, 100]}, goldStandard);

// Test 6 ---------------------------------------------------------------------
// Out of range lower bound
res = mldb.put("/v1/procedures/bucketizeMyScoreInvalid", {
    type: 'bucketize',
    params: {
        inputData: "select * from rNamedScores order by score DESC",
        outputDataset: {id : "bucketedScoresInvalid", type: "sparse.mutable"},
        percentileBuckets: {b1: [-0.2, 50], b2: [50, 1]}
    }
});
assertEqual(res['responseCode'], 400);

// Test 7 ---------------------------------------------------------------------
// Out of range higher bound
res = mldb.put("/v1/procedures/bucketizeMyScoreInvalid", {
    type: 'bucketize',
    params: {
        inputData: "select * from rNamedScores order by score DESC",
        outputDataset: {id : "bucketedScoresInvalid", type: "sparse.mutable"},
        percentileBuckets: {b1: [0, 50], b2: [50, 100.1]}
    }
});
assertEqual(res['responseCode'], 400);

// Test 8 ---------------------------------------------------------------------
// Inverted bounds
res = mldb.put("/v1/procedures/bucketizeMyScoreInvalid", {
    type: 'bucketize',
    params: {
        inputData: "select * from rNamedScores order by score DESC",
        outputDataset: {id : "bucketedScoresInvalid", type: "sparse.mutable"},
        percentileBuckets: {b1: [50, 0], b2: [50, 100]}
    }
});
assertEqual(res['responseCode'], 400);

// Test 9 ---------------------------------------------------------------------
// Bucketize on no data
res = mldb.put('/v1/datasets/emptyDataset', {
    type : 'sparse.mutable'
});
assertEqual(res['responseCode'], 201);

res = mldb.perform('POST', '/v1/datasets/emptyDataset/commit', {});
assertEqual(res['responseCode'], 200);

res = mldb.put("/v1/procedures/bucketizeEmptyDataset", {
    type: 'bucketize',
    params: {
        inputData: "select * from emptyDataset order by score DESC",
        outputDataset: {id : "bucketedEmptyDataset", type: "sparse.mutable"},
        percentileBuckets: {b1: [0, 50], b2: [50, 100]}
    }
});
assertEqual(res['responseCode'], 201);

"success"

