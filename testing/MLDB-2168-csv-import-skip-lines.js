// This file is part of MLDB. Copyright 2017 Element AI Inc. All rights reserved.

/**
 * MLDB-2168-csv-import-skip-lines.js
 * Copyright (c) 2017 mldb.ai inc. All rights reserved.
 **/

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

var mldb2168Config = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/MLDB-1312-quotechar.csv",
            outputDataset: {
                id: 'mldb2168',
            },
            runOnCreation: true,
            encoding: 'latin1',
            ignoreBadLines: false,
            quoteChar: '#',
            skipLineRegex: '.*#b#.*'
        }
    }

var res = mldb.put("/v1/procedures/csv_proc", mldb2168Config);

mldb.log(res);

expected = [
   [ "_rowName", "a", "b" ],
   [ "2", "a", "b" ],
   [ "4", "a,b", "c" ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb2168 order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quoteChar test");

"success"
