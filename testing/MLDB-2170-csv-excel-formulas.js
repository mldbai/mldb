// This file is part of MLDB. Copyright 2017 Element AI Inc. All rights reserved.

/**
 * MLDB-2170-skip-extra-columns.js
 * Copyright (c) 2017 Element AI Inc. All rights reserved.
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

var mldb2170Config = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/fixtures/MLDB-2170-csv-excel-formulas.csv",
            outputDataset: {
                id: 'mldb2170',
            },
            runOnCreation: true,
            encoding: 'latin1',
            ignoreBadLines: false,
            processExcelFormulas: true
        }
    }

var res = mldb.put("/v1/procedures/csv_proc", mldb2170Config);

mldb.log(res);

assertEqual(res.responseCode, 201);

expected = [
   [ "_rowName", "a", "b" ],
   [ "2", 1, 2 ],
   [ "3", 3, 4 ],
   [ "4", 5, 6 ],
   [ "5", "=7", "=8" ],
   [ "6", 9, 10 ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb2170 order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quoteChar test");

"success"
