// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw new Error("Assertion failure: " + msg + ": " + JSON.stringify(expr)
                    + " not equal to " + JSON.stringify(val));
}

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://s3.amazonaws.com/benchm-ml--main/test.csv",
        outputDataset: {
            id: "test",
        },
        runOnCreation: true,
    }
}

mldb.put("/v1/procedures/csv_proc", csv_conf)

var counts = mldb.get("/v1/query",
                      { q: 'select count(*) named dep_delayed_15min from test group by dep_delayed_15min',
                        format: 'table'
                      }).json;

mldb.log(counts);


var expected = [
    [ "_rowName", "count(*)" ],
    [ "N", 78383 ],
    [ "Y", 21617 ]
];

assertEqual(counts, expected);

"success"
