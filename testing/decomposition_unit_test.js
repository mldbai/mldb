// decomposition_unit_test.js

//mldb = require('mldb')
//unittest = require('mldb/unittest')

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

unittest = { assertEqual: assertEqual }


// Test copying a column into two output columns works (no problems with
// input/output name confusion or moving values that are later re-used).

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "http://public.mldb.ai/tweets.gz",
        outputDataset: {
            id: "tweets",
        },
        runOnCreation: true,
        limit: 100,
        delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
        select: "tweet, tweet as tweet2",
        ignoreBadLines: true
    }
};

var res = mldb.put("/v1/procedures/csv_proc", config);
mldb.log(res["json"]);

res = mldb.get("/v1/query", { q: 'select count(*) as count from tweets where tweet = tweet2' });
mldb.log(res["json"]);

var expected = [
   {
      "columns" : [
         [ "count", 99, "-Inf" ]
      ],
      "rowName" : "[]"
   }
];

unittest.assertEqual(res["json"], expected)

mldb.del('/v1/datasets/tweets');
mldb.del('/v1/procedures/csv_proc');


// Same thing, but reverse order

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "http://public.mldb.ai/tweets.gz",
        outputDataset: {
            id: "tweets",
        },
        runOnCreation: true,
        limit: 100,
        delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
        select: "tweet as tweet2, tweet",
        ignoreBadLines: true
    }
};

var res = mldb.put("/v1/procedures/csv_proc", config);
mldb.log(res["json"]);

res = mldb.get("/v1/query", { q: 'select count(*) as count from tweets where tweet = tweet2' });
mldb.log(res["json"]);

var expected = [
   {
      "columns" : [
         [ "count", 99, "-Inf" ]
      ],
      "rowName" : "[]"
   }
];

unittest.assertEqual(res["json"], expected)

mldb.del('/v1/datasets/tweets');
mldb.del('/v1/procedures/csv_proc');


// Test copying a column into two output columns works (no problems with
// input/output name confusion or moving values that are later re-used).

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "http://public.mldb.ai/tweets.gz",
        outputDataset: {
            id: "tweets",
        },
        runOnCreation: true,
        limit: 100,
        delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
        select: "tweet as tweet1, tweet as tweet2",
        ignoreBadLines: true
    }
};

var res = mldb.put("/v1/procedures/csv_proc", config);
mldb.log(res["json"]);

res = mldb.get("/v1/query", { q: 'select count(*) as count from tweets where tweet1 = tweet2' });
mldb.log(res["json"]);

var expected = [
   {
      "columns" : [
         [ "count", 99, "-Inf" ]
      ],
      "rowName" : "[]"
   }
];

unittest.assertEqual(res["json"], expected)

mldb.del('/v1/datasets/tweets');
mldb.del('/v1/procedures/csv_proc');



// Test that a function of a copied column works as expected

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "http://public.mldb.ai/tweets.gz",
        outputDataset: {
            id: "tweets",
        },
        runOnCreation: true,
        limit: 100,
        delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
        select: "tweet, hash(tweet) as hash",
        ignoreBadLines: true
    }
};

var res = mldb.put("/v1/procedures/csv_proc", config);
mldb.log(res["json"]);

res = mldb.get("/v1/query", { q: 'select count(*) as count from tweets where hash(tweet) = hash' });
mldb.log(res["json"]);

var expected = [
   {
      "columns" : [
         [ "count", 99, "-Inf" ]
      ],
      "rowName" : "[]"
   }
];

unittest.assertEqual(res["json"], expected)

mldb.del('/v1/datasets/tweets');
mldb.del('/v1/procedures/csv_proc');

"success"
