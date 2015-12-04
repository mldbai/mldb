// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y, z)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
    if (z)
        dataset.recordRow(row, [ [ "z", z, ts ] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 3);
recordExample("ex2", 1, 2, "yes");
recordExample("ex3", 2, 1);
recordExample("ex4", 3, 0, "no");

dataset.commit()

var res1 = mldb.get("/v1/datasets/test/query",
                    { select: 'x,y,z', orderBy: 'rowName()', format: "aos" }).json;

plugin.log(res1);

var expected1 = [
   {
      "_rowName" : "ex1",
      "x" : 0,
      "y" : 3,
      "z" : null
   },
   {
      "_rowName" : "ex2",
      "x" : 1,
      "y" : 2,
      "z" : "yes"
   },
   {
      "_rowName" : "ex3",
      "x" : 2,
      "y" : 1,
      "z" : null
   },
   {
      "_rowName" : "ex4",
      "x" : 3,
      "y" : 0,
      "z" : "no"
   }
];

assertEqual(res1, expected1);


var res2 = mldb.get("/v1/datasets/test/query",
                    { select: 'x,y,z', orderBy: 'rowName()', format: "soa" }).json;

plugin.log(res2);

var expected2 = {
   "_rowName" : [ "ex1", "ex2", "ex3", "ex4" ],
   "x" : [ 0, 1, 2, 3 ],
   "y" : [ 3, 2, 1, 0 ],
   "z" : [ null, "yes", null, "no" ]
};

assertEqual(res2, expected2);

var res3 = mldb.get("/v1/datasets/test/query",
                    { select: 'x,y,z', orderBy: 'rowName()', format: "table" }).json;

plugin.log(res3);

var expected3 = [
   [ "_rowName", "x", "y", "z" ],
   [ "ex1", 0, 3, null ],
   [ "ex2", 1, 2, "yes" ],
   [ "ex3", 2, 1, null ],
   [ "ex4", 3, 0, "no" ]
];

assertEqual(res3, expected3);

var res3a = mldb.get("/v1/datasets/test/query",
                     { select: 'x,y,z', orderBy: 'rowName()', format: "table", headers:false }).json;

plugin.log(res3a);

var expected3a = [
   [ "ex1", 0, 3, null ],
   [ "ex2", 1, 2, "yes" ],
   [ "ex3", 2, 1, null ],
   [ "ex4", 3, 0, "no" ]
];

assertEqual(res3a, expected3a);

var res3b = mldb.get("/v1/query",
                     { q: 'select x,y,z from test order by rowName()', format: "table", headers:false }).json;

plugin.log(res3b);

var expected3b = [
   [ "ex1", 0, 3, null ],
   [ "ex2", 1, 2, "yes" ],
   [ "ex3", 2, 1, null ],
   [ "ex4", 3, 0, "no" ]
];

assertEqual(res3b, expected3b);

var res4 = mldb.get("/v1/datasets/test/query",
                    { select: 'x,y,z', orderBy: 'rowName()', format: "sparse" }).json;

plugin.log(res4);

var expected4 = [
   [
      [ "_rowName", "ex1" ],
      [ "x", 0 ],
      [ "y", 3 ],
      [ "z", null ]
   ],
   [
      [ "_rowName", "ex2" ],
      [ "x", 1 ],
      [ "y", 2 ],
      [ "z", "yes" ]
   ],
   [
      [ "_rowName", "ex3" ],
      [ "x", 2 ],
      [ "y", 1 ],
      [ "z", null ]
   ],
   [
      [ "_rowName", "ex4" ],
      [ "x", 3 ],
      [ "y", 0 ],
      [ "z", "no" ]
   ]
];

assertEqual(res4, expected4);

var res5 = mldb.get("/v1/datasets/test/query",
                    { select: 'x,y,z', orderBy: 'rowName()', format: "full" }).json;

plugin.log(res5);

var expected5 = [
   {
      "columns" : [
         [ "x", 0, "2015-01-01T00:00:00Z" ],
         [ "y", 3, "2015-01-01T00:00:00Z" ],
         [ "z", null, "-Inf" ]
      ],
      "rowHash" : "397de880d5f0376e",
      "rowName" : "ex1"
   },
   {
      "columns" : [
         [ "x", 1, "2015-01-01T00:00:00Z" ],
         [ "y", 2, "2015-01-01T00:00:00Z" ],
         [ "z", "yes", "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "ed64a202cef7ccf1",
      "rowName" : "ex2"
   },
   {
      "columns" : [
         [ "x", 2, "2015-01-01T00:00:00Z" ],
         [ "y", 1, "2015-01-01T00:00:00Z" ],
         [ "z", null, "-Inf" ]
      ],
      "rowHash" : "418b8ce19e0de7a3",
      "rowName" : "ex3"
   },
   {
      "columns" : [
         [ "x", 3, "2015-01-01T00:00:00Z" ],
         [ "y", 0, "2015-01-01T00:00:00Z" ],
         [ "z", "no", "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "213ca5902e95224e",
      "rowName" : "ex4"
   }
];

assertEqual(res5, expected5);

var res5a = mldb.get("/v1/datasets/test/query",
                     { select: 'x,y,z', orderBy: 'rowName()'}).json;

plugin.log(res5a);

assertEqual(res5a, expected5);

var res5b = mldb.get("/v1/query",
                     { q: 'select x,y,z from test order by rowName()'}).json;

plugin.log(res5b);

assertEqual(res5b, expected5);

"success"
