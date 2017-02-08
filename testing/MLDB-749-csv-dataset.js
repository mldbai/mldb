// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/**
 * MLDB-749-csv-dataset.js
 * Nicolas, 2015-07-23
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.
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

var csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/dataset/iris.data",
        outputDataset: {
            id: "iris",
        },
        runOnCreation: false,
        headers: [ 'sepal length', 'sepal width', 'petal length', 'petal width', 'class' ],
        ignoreBadLines: true
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)
mldb.log(res);
assertEqual(res["responseCode"], 201);

var res = mldb.put("/v1/procedures/csv_proc/runs/myrun", {});

mldb.log(res);

assertEqual(res['json']['status']['numLineErrors'], 0);

res = mldb.get('/v1/datasets/iris');
assertEqual(res['json']['status']['rowCount'], 150);
mldb.log(res);


res = mldb.get('/v1/datasets/iris/query', { limit: 10, format: 'table', orderBy: 'CAST (rowName() AS NUMBER)'});

mldb.log(res.json);

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
        outputDataset: {
            id: "titanic",
        },
        runOnCreation: true,
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

var res = mldb.get('/v1/datasets/titanic/query', { limit: 10, format: 'table', orderBy: 'rowName()'});

mldb.log(res.json);

try {

    csv_conf = {
        type: "import.text",
        params: {
            dataFileUrl : "file://modes20130525-0705.csv",
            outputDataset: {
                id: "test",
            },
            runOnCreation: true,
            delimiter: '|'
        }
    }

    var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

    var res = mldb.get('/v1/datasets/test/query', { limit: 10, format: 'table'});

    mldb.log(res);
} catch (e) {
}

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
        outputDataset: {
            id: "titanic2",
        },
        named: 'lineNumber() % 10'
    }
}

mldb.put("/v1/procedures/csv_proc", csv_conf)
res = mldb.put("/v1/procedures/csv_proc/runs/0", {})
mldb.log(res);
assertEqual(res['responseCode'], 400);
assertEqual(res['json']['error'], "Duplicate row name in tabular dataset");

// Test correctness of parser
var correctnessConfig = {
    type: 'text.csv.tabular',
    id: 'correctness',
    params: {
        dataFileUrl: 'https://raw.githubusercontent.com/uniVocity/csv-parsers-comparison/master/src/main/resources/correctness.csv'
    }
};

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://raw.githubusercontent.com/uniVocity/csv-parsers-comparison/master/src/main/resources/correctness.csv",
        outputDataset: {
            id: "correctness",
        },
        runOnCreation: true,
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

// TODO: this requires support for multi-line CSV files
//mldb.createDataset(correctnessConfig);

//var res = mldb.get("/v1/query", { q: 'select * from correctness order by rowName()' });

mldb.log(res);

// Test loading of large file (MLDB-806, MLDB-807)
csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "http://www.maxmind.com/download/worldcities/worldcitiespop.txt.gz",
        outputDataset: {
            id: "cities",
        },
        runOnCreation: true,
        encoding: 'latin1'
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

var res = mldb.get("/v1/query", { q: 'select * from cities limit 10', format: 'table' });

mldb.log(res);

// Check no row names are duplicated
var res = mldb.get("/v1/query", { q: 'select count(*) as cnt from cities group by rowName() order by cnt desc limit 10', format: 'table' }).json;

mldb.log(res);

// Check that the highest count is 1, ie each row name occurs exactly once
assertEqual(res[1][1], 1);

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

assertEqual(res, expected);

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

assertEqual(res, expected, "City populations CSV");

// Test loading of broken file (MLDB-994)
// broken that fails
var brokenConfigFail = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-749_broken_csv.csv",
        outputDataset: {
            id: "broken_fail",
        },
        runOnCreation: true,
        encoding: 'latin1'
    }
}

var res = mldb.put("/v1/procedures/csv_proc", brokenConfigFail);
assertEqual(res['responseCode'], 400);
assertEqual(res['json']['details']['runError']['details']['lineNumber'], 5);

// MLDB-1404: do it 100 times to ensure we don't terminate

for (var i = 0;  i < 100;  ++i) {
    res = mldb.put("/v1/procedures/csv_proc", brokenConfigFail);
    assertEqual(res['responseCode'], 400);
    assertEqual(res['json']['details']['runError']['details']['lineNumber'], 5);
}

/**/

var brokenConfigNoHeader = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-749_broken_csv_no_header.csv",
        outputDataset: {
            id: "broken_fail",
        },
        runOnCreation: true,
        encoding: 'latin1',
        headers: ['a', 'b', 'c']
    }
}


var res = mldb.put("/v1/procedures/csv_proc", brokenConfigNoHeader)
assertEqual(res['responseCode'], 400);
assertEqual(res['json']['details']['runError']['details']['lineNumber'], 4);

brokenConfig = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-749_broken_csv.csv",
        outputDataset: {
            id: "broken",
        },
        runOnCreation: true,
        encoding: 'latin1',
        ignoreBadLines: true
    }
}

res = mldb.put("/v1/procedures/csv_proc", brokenConfig)
mldb.log(res);
assertEqual(res['json']['status']['firstRun']['status']['numLineErrors'], 4);

var res = mldb.get("/v1/query", { q: 'select * from broken order by CAST (rowName() AS NUMBER) ASC limit 10', format: 'table' });
mldb.log(res);
if(res["json"].length != 5) {
    throw "Wrong number !!";
}

// make sure the rowNames marches the line number in the csv file    
rowName = res["json"][4][0]
if(rowName.substr(rowName.length-1) != "9") {
    throw "Wrong rowName!";
}

// Test skip first n lines 
brokenConfig = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-749_broken_csv.csv",
        outputDataset: {
            id: "skippinnn",
        },
        runOnCreation: true,
        encoding: 'latin1',
        ignoreBadLines: true,
        offset: 2
    }
}

mldb.put("/v1/procedures/csv_proc", brokenConfig);

var res = mldb.get("/v1/query", { q: 'select * from skippinnn order by a ASC limit 10', format: 'table' });
mldb.log(res);
if(res["json"].length != 3) {
    throw "Wrong number !!";
}

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/testing/MLDB-749_bad_header_row_name.csv",
        outputDataset: {
            id: "badHeaderRowName",
        },
        runOnCreation: true,
        encoding: 'latin1',
        named: 'c'
    }
}

res = mldb.put("/v1/procedures/csv_proc", config);
assertEqual(res['responseCode'], 400); //bad rowNameColumn

var config = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/MLDB-749_bad_header_row_name.csv",
            outputDataset: {
                id: "headerRowName",
            },
            runOnCreation: true,
            encoding: 'latin1',
            select: '* EXCLUDING (a)',
            named: 'a'
        }
    }

mldb.put("/v1/procedures/csv_proc", config);

res = mldb.get("/v1/datasets/headerRowName");
assertEqual(res['json']['status']['rowCount'], 2);

var res = mldb.get("/v1/query", {
    q: 'SELECT * FROM headerRowName ORDER BY rowName() ASC LIMIT 10',
    format: 'table' }
);
try {
    assertEqual(res['json'].length, 3);
    assertEqual(res['json'][0], ["_rowName", "b"]);
    assertEqual(res['json'][1], ["jambon", 4]);
} catch (e) {
    mldb.log(res);
    throw e;
}

var res = mldb.get("/v1/query", {
    q: 'SELECT * FROM headerRowName ORDER BY rowName() DESC LIMIT 10',
    format: 'table' }
);
try {
    assertEqual(res['json'].length, 3);
    assertEqual(res['json'][0], ["_rowName", "b"]);
    assertEqual(res['json'][2], ["jambon", 4]);
} catch (e) {
    mldb.log(res);
    throw e;
}

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
assertEqual(getCountWithOffsetLimit("test2", 0, 10), 10, "expecting 10 rows only");
assertEqual(getCountWithOffsetLimit("test3", 0, totalSize + 2000), totalSize, "we can't get more than what there is!");
assertEqual(getCountWithOffsetLimit("test4", 10, -1), totalSize - 10, "expecting all set except 10 rows");

function getCountWithOffsetLimit2(dataset, offset, limit) {
    var config = {
        type: "import.text",
        params: {
            dataFileUrl : "http://public.mldb.ai/tweets.gz",
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
assertEqual(getCountWithOffsetLimit2("test_100000", 0, 100000), 100000, "expecting 100000 rows only");
assertEqual(getCountWithOffsetLimit2("test_98765", 0, 98765), 98765, "expecting 98765 rows only");
assertEqual(getCountWithOffsetLimit2("test_1234567", 0, 999999), 999999, "expecting 999999 rows only");
assertEqual(getCountWithOffsetLimit2("test_0", 0, 0), 0, "expecting 0 rows only");
assertEqual(getCountWithOffsetLimit2("test_1", 0, 1), 1, "expecting 1 row only");
assertEqual(getCountWithOffsetLimit2("test_10_1", 10, 1), 1, "expecting 1 row only");
assertEqual(getCountWithOffsetLimit2("test_12", 0, 12), 12, "expecting 12 rows only");
assertEqual(getCountWithOffsetLimit2("test_total+2000", 0, totalSize + 2000), totalSize, "we can't get more than what there is!");
assertEqual(getCountWithOffsetLimit2("test_total-10", 10, -1), totalSize - 10, "expecting all set except 10 rows");

//MLDB-1312 specify quoteChar
var mldb1312Config = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/MLDB-1312-quotechar.csv",
            outputDataset: {
                id: 'mldb1312',
            },
            runOnCreation: true,
            encoding: 'latin1',
            quoteChar: '#'
        }
    }

mldb.put("/v1/procedures/csv_proc", mldb1312Config);

expected = 
[
      [ "_rowName", "a", "b" ],
      [ "2", "a", "b" ],
      [ "3", "a#b", "c" ],
      [ "4", "a,b", "c" ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb1312 order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quoteChar test");

var mldb1312Config_b = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/MLDB-1312-quoteChar.csv",
            outputDataset: {
                id: 'mldb1312_b',
            },
            runOnCreation: true,
            encoding: 'latin1',
            quoteChar: '#',
            delimiter: ''
        }
    }


res = mldb.put("/v1/procedures/csv_proc", mldb1312Config_b);
assertEqual(res['responseCode'], 400);

var mldb1312Config_c = {
        type: "import.text",
        params: {
            dataFileUrl : "file://mldb/testing/MLDB-1312-quotechar.csv",
            outputDataset: {
                id: 'mldb1312_c',
            },
            runOnCreation: true,
            encoding: 'latin1',
            quoteChar: '',
            delimiter: ',',
            ignoreBadLines: true
        }
    }

mldb.put("/v1/procedures/csv_proc", mldb1312Config_c);

expected = 
[
   [ "_rowName", "a", "b" ],
   [ "2", "#a#", "b" ],
   [ "3", "#a##b#", "c" ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb1312_c order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quoteChar test");

"success"
