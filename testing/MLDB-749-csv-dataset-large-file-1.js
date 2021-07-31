// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

// Test loading of large file (MLDB-806, MLDB-807)
csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/mldb_test_data/worldcitiespop.txt.gz",
        outputDataset: {
            id: "cities",
        },
        runOnCreation: true,
        encoding: 'latin1',
	timestamp: "TIMESTAMP '2012-05-03T03:14:46Z'"
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

mldb.log("cities import: ", res);

var res = mldb.get("/v1/query", { q: 'select * from cities limit 10', format: 'table' });

mldb.log(res);

// Check no row names are duplicated
var res = mldb.get("/v1/query", { q: 'select count(*) as cnt from cities group by rowName() order by cnt desc limit 10', format: 'table' }).json;

mldb.log(res);

// Check that the highest count is 1, ie each row name occurs exactly once
unittest.assertEqual(res[1][1], 1);

var res = mldb.get("/v1/query", { q: 'select count(*), min(cast (rowName() as integer)), max(cast (rowName() as integer)) from cities', format: 'table' }).json;

mldb.log(res);

var expected = [
   [
      "_rowName",
      "count(*)",
      "max(cast (rowName() as integer))",
      "min(cast (rowName() as integer))"
   ],
   [ "[]", 3173958, 3173959, 2 ]
];

unittest.assertEqual(res, expected);

var res = mldb.get("/v1/query", { q: 'select * from cities where cast (rowName() as integer) in (2, 1000, 1000000, 2000000, 3000000, 3173959) order by cast (rowName() as integer)' }).json;

mldb.log(res);

expected = [
   {
      "columns" : [
         [ "AccentCity", "Aixàs", "2012-05-03T03:14:46Z" ],
         [ "City", "aixas", "2012-05-03T03:14:46Z" ],
         [ "Country", "ad", "2012-05-03T03:14:46Z" ],
         [ "Latitude", 42.48333330, "2012-05-03T03:14:46Z" ],
         [ "Longitude", 1.46666670, "2012-05-03T03:14:46Z" ],
         [ "Region", 6, "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "2"
   },
   {
      "columns" : [
         [ "AccentCity", "`Abd ur Rahim Khel", "2012-05-03T03:14:46Z" ],
         [ "City", "`abd ur rahim khel", "2012-05-03T03:14:46Z" ],
         [ "Country", "af", "2012-05-03T03:14:46Z" ],
         [ "Latitude", 33.9111520, "2012-05-03T03:14:46Z" ],
         [ "Longitude", 68.4411010, "2012-05-03T03:14:46Z" ],
         [ "Region", 27, "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "1000"
   },
   {
      "columns" : [
         [ "AccentCity", "Ryde", "2012-05-03T03:14:46Z" ],
         [ "City", "ryde", "2012-05-03T03:14:46Z" ],
         [ "Country", "gb", "2012-05-03T03:14:46Z" ],
         [ "Latitude", 50.7166670, "2012-05-03T03:14:46Z" ],
         [ "Longitude", -1.1666670, "2012-05-03T03:14:46Z" ],
         [ "Population", 24107, "2012-05-03T03:14:46Z" ],
         [ "Region", "G2", "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "1000000"
   },
   {
      "columns" : [
         [ "AccentCity", "Kajia", "2012-05-03T03:14:46Z" ],
         [ "City", "kajia", "2012-05-03T03:14:46Z" ],
         [ "Country", "ng", "2012-05-03T03:14:46Z" ],
         [ "Latitude", 12.62740, "2012-05-03T03:14:46Z" ],
         [ "Longitude", 10.81920, "2012-05-03T03:14:46Z" ],
         [ "Region", 44, "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "2000000"
   },
   {
      "columns" : [
         [ "AccentCity", "Greasy Ridge", "2012-05-03T03:14:46Z" ],
         [ "City", "greasy ridge", "2012-05-03T03:14:46Z" ],
         [ "Country", "us", "2012-05-03T03:14:46Z" ],
         [ "Latitude", 38.64527780, "2012-05-03T03:14:46Z" ],
         [ "Longitude", -82.42111110, "2012-05-03T03:14:46Z" ],
         [ "Region", "OH", "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "3000000"
   },
   {
      "columns" : [
         [ "AccentCity", "Zvishavane", "2012-05-03T03:14:46Z" ],
         [ "City", "zvishavane", "2012-05-03T03:14:46Z" ],
         [ "Country", "zw", "2012-05-03T03:14:46Z" ],
         [ "Latitude", -20.33333330, "2012-05-03T03:14:46Z" ],
         [ "Longitude", 30.03333330, "2012-05-03T03:14:46Z" ],
         [ "Population", 79876, "2012-05-03T03:14:46Z" ],
         [ "Region", 7, "2012-05-03T03:14:46Z" ]
      ],
      "rowName" : "3173959"
   }
];

unittest.assertEqual(res, expected, "City populations CSV");

