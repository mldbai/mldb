// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// Tests the following issues
// - MLDB-1034 (createInterval)
// - MLDB-1038 (empty intervals should write "0S")
// - MLDB-1037 (fractional seconds in intervals)
// - MLDB-1033 (writing of timestamps)
// - MLDB-1032 (sparse.mutable record of intervals, timestamps)
// - MLDB-1041 (sparse.mutable 4 character strings)

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

var datasetConfig = {
    id: 'test',
    type: 'sparse.mutable'
};

var dataset = mldb.createDataset(datasetConfig);

dataset.recordRow("row1", [ [ "col1", new Date("2015-01-01"), new Date("2015-02-02") ] ]);

var interval = mldb.createInterval({ months: 0, days: 0, seconds: 4567.89 });
var interval2 = mldb.createInterval({ months: 0, days: 0, seconds: 6789.0123 });
var interval3 = mldb.createInterval({ months: 0, days: 0, seconds: 56789.0123 });
var interval4 = mldb.createInterval({ months: 0, days: 0, seconds: -0.1 });
var interval5 = mldb.createInterval({ months: 0, days: 0, seconds: 0 });

dataset.recordRow("row2", [ [ "col1", interval, new Date("2015-02-02") ] ]);
dataset.recordRow("row3", [ [ "col1", interval2, new Date("2015-02-02") ] ]);
dataset.recordRow("row4", [ [ "col1", interval3, new Date("2015-02-02") ] ]);
dataset.recordRow("row5", [ [ "col1", interval4, new Date("2015-02-02") ] ]);
dataset.recordRow("row6", [ [ "col1", interval5, new Date("2015-02-02") ] ]);
dataset.recordRow("row7", [ [ "col1", "Date", new Date("2015-02-02") ] ]);

dataset.commit();

var resp = mldb.get("/v1/query", { q: "select * from test order by rowName()", format: "table" }).json;


var expected = [
    [ "_rowName", "col1" ],
    [ "row1", "2015-01-01T00:00:00Z" ],
    [ "row2", "1H 16M 7.89S" ],
    [ "row3", "1H 53M 9.0123S" ],
    [ "row4", "15H 46M 29.0123S" ],
    [ "row5", "-0.1S" ],
    [ "row6", "0S" ],
    [ "row7", "Date" ]
];

mldb.log(resp);

assertEqual(resp, expected);

"success"


