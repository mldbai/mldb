// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var dataset = mldb.createDataset({type:'sparse.mutable',id:'airports'});

var ts = new Date("2015-01-01");

var row = 0;

function recordExample(where, lat, lon)
{
    dataset.recordRow(where, [ [ "lat", lat, ts ], ["lon", lon, ts] ]);
}

recordExample("lhr", 51.477500, -0.461388);
recordExample("syd", -33.946110, 151.177222);
recordExample("lax", 33.942495, -118.408067);
recordExample("sfo", 37.619105, -122.375236);
recordExample("oak", 37.721306, -122.220721);

// Distances between them, in metres
//      lhr-syd 17015628
//      syd-lax 12050690
//      lax-lhr 8780169
//      sfo-oak 17734]

dataset.commit()

var resp = mldb.get("/v1/query", {q: "select round(geo_distance(x.lat, x.lon, y.lat, y.lon) / 1000) as dst from airports as x join airports as y where x.rowName() < y.rowName() order by dst desc, rowName()", format: 'table'});

plugin.log(resp.json);

assertEqual(resp.responseCode, 200, "Error executing query");

// Number of kilometers between the airports, rounded to 1
var expected = [
   [ "_rowName", "dst" ],
   [ "[lhr]-[syd]", 17020 ],
   [ "[lax]-[syd]", 12061 ],
   [ "[oak]-[syd]", 11967 ],
   [ "[sfo]-[syd]", 11950 ],
   [ "[lax]-[lhr]", 8759 ],
   [ "[lhr]-[sfo]", 8615 ],
   [ "[lhr]-[oak]", 8599 ],
   [ "[lax]-[oak]", 543 ],
   [ "[lax]-[sfo]", 543 ],
   [ "[oak]-[sfo]", 18 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");


"success"
