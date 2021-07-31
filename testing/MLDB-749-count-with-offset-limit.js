// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function getCountWithOffsetLimit(dataset, offset, limit) {
    // Test offset and limit
    var config = {
        type: "import.text",
        params: {
            dataFileUrl : "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
            outputDataset: {
                id: dataset,
            },
            runOnCreation: true,
            offset: offset,
            limit: limit
        }
    }

    mldb.put("/v1/procedures/csv_proc", config);

    var res = mldb.get("/v1/query", { q: 'select count(*) as count from ' + dataset });
    mldb.log(res["json"]);
    return res["json"][0].columns[0][1];
}

var totalSize = getCountWithOffsetLimit("test1", 0, -1);
unittest.assertEqual(getCountWithOffsetLimit("test2", 0, 10), 10, "expecting 10 rows only");
unittest.assertEqual(getCountWithOffsetLimit("test3", 0, totalSize + 2000), totalSize, "we can't get more than what there is!");
unittest.assertEqual(getCountWithOffsetLimit("test4", 10, -1), totalSize - 10, "expecting all set except 10 rows");

function getCountWithOffsetLimit2(dataset, offset, limit) {
    var config = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/mldb_test_data/tweets.gz",
            outputDataset: {
                id: dataset,
            },
            runOnCreation: true,
            offset: offset,
            limit: limit,
            delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
            select: "tweet",
            ignoreBadLines: true
        }
    }

    res = mldb.put("/v1/procedures/csv_proc", config);
    mldb.log(res["json"]);
    var numLineErrors = res["json"]['status']['firstRun']['status']['numLineErrors'];
    res = mldb.get("/v1/datasets/"+dataset);
    mldb.log(res["json"]);
    return numLineErrors + res["json"]["status"]["rowCount"];
}

var totalSize = getCountWithOffsetLimit2("test_total", 0, -1);
unittest.assertEqual(getCountWithOffsetLimit2("test_100000", 0, 100000), 100000, "expecting 100000 rows only");
unittest.assertEqual(getCountWithOffsetLimit2("test_98765", 0, 98765), 98765, "expecting 98765 rows only");
unittest.assertEqual(getCountWithOffsetLimit2("test_1234567", 0, 999999), 999999, "expecting 999999 rows only");
unittest.assertEqual(getCountWithOffsetLimit2("test_0", 0, 0), 0, "expecting 0 rows only");
unittest.assertEqual(getCountWithOffsetLimit2("test_1", 0, 1), 1, "expecting 1 row only");
unittest.assertEqual(getCountWithOffsetLimit2("test_10_1", 10, 1), 1, "expecting 1 row only");
unittest.assertEqual(getCountWithOffsetLimit2("test_12", 0, 12), 12, "expecting 12 rows only");
unittest.assertEqual(getCountWithOffsetLimit2("test_total+2000", 0, totalSize + 2000), totalSize, "we can't get more than what there is!");
unittest.assertEqual(getCountWithOffsetLimit2("test_total-10", 10, -1), totalSize - 10, "expecting all set except 10 rows");
