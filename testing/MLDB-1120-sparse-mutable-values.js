// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
    type:    'sparse.mutable',
    id:      'test'
};

var dataset = mldb.createDataset(dataset_config);

var ts = new Date(2015, 01, 01);

// Check all lengths of strings
dataset.recordRow("rowa1", [ [ "a", "a", ts ] ]);
dataset.recordRow("rowa2", [ [ "ab", "ab", ts ] ]);
dataset.recordRow("rowa3", [ [ "abc", "abc", ts ] ]);
dataset.recordRow("rowa4", [ [ "abcd", "abcd", ts ] ]);
dataset.recordRow("rowa5", [ [ "abcde", "abcde", ts ] ]);
dataset.recordRow("rowa6", [ [ "abcdef", "abcdef", ts ] ]);
dataset.recordRow("rowa7", [ [ "abcdefg", "abcdefg", ts ] ]);

// Check all lengths of utf-8 strings
dataset.recordRow("rowb1", [ [ "é", "é", ts ] ]);
dataset.recordRow("rowb2", [ [ "éb", "éb", ts ] ]);
dataset.recordRow("rowb3", [ [ "ébc", "ébc", ts ] ]);
dataset.recordRow("rowb4", [ [ "ébcd", "ébcd", ts ] ]);
dataset.recordRow("rowb5", [ [ "ébcde", "ébcde", ts ] ]);
dataset.recordRow("rowb6", [ [ "ébcdef", "ébcdef", ts ] ]);
dataset.recordRow("rowb7", [ [ "ébcdefg", "ébcdefg", ts ] ]);

// Check small integers
dataset.recordRow("rowc1", [ [ "x", 0, ts ] ]);
dataset.recordRow("rowc2", [ [ "x", 1, ts ] ]);
dataset.recordRow("rowc3", [ [ "x", -1, ts ] ]);


dataset.commit();

var resp = mldb.get('/v1/query', { q: "select * from test order by rowName()", format: 'sparse' }).json;

mldb.log(resp);


var expected = [
    [
        [ "_rowName", "rowa1" ],
        [ "a", "a" ]
    ],
    [
        [ "_rowName", "rowa2" ],
        [ "ab", "ab" ]
    ],
    [
        [ "_rowName", "rowa3" ],
        [ "abc", "abc" ]
    ],
    [
        [ "_rowName", "rowa4" ],
        [ "abcd", "abcd" ]
    ],
    [
        [ "_rowName", "rowa5" ],
        [ "abcde", "abcde" ]
    ],
    [
        [ "_rowName", "rowa6" ],
        [ "abcdef", "abcdef" ]
    ],
    [
        [ "_rowName", "rowa7" ],
        [ "abcdefg", "abcdefg" ]
    ],
    [
        [ "_rowName", "rowb1" ],
        [ "é", "é" ]
    ],
    [
        [ "_rowName", "rowb2" ],
        [ "éb", "éb" ]
    ],
    [
        [ "_rowName", "rowb3" ],
        [ "ébc", "ébc" ]
    ],
    [
        [ "_rowName", "rowb4" ],
        [ "ébcd", "ébcd" ]
    ],
    [
        [ "_rowName", "rowb5" ],
        [ "ébcde", "ébcde" ]
    ],
    [
        [ "_rowName", "rowb6" ],
        [ "ébcdef", "ébcdef" ]
    ],
    [
        [ "_rowName", "rowb7" ],
        [ "ébcdefg", "ébcdefg" ]
    ],
    [
      [ "_rowName", "rowc1" ],
        [ "x", 0 ]
    ],
    [
        [ "_rowName", "rowc2" ],
      [ "x", 1 ]
    ],
    [
        [ "_rowName", "rowc3" ],
        [ "x", -1 ]
    ]
];

assertEqual(expected, resp);

"success"



