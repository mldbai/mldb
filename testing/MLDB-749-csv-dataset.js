// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/**
 * MLDB-749-csv-dataset.js
 * Nicolas, 2015-07-23
 * Copyright (c) 2015 Datacratic Inc. All rights reserved.
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

var irisConfig = {
    type: 'text.csv.tabular',
    id: 'iris',
    params: {
        dataFileUrl: 'file://mldb/testing/dataset/iris.data',
        headers: [ 'sepal length', 'sepal width', 'petal length', 'petal width', 'class' ]
    }
};

mldb.createDataset(irisConfig);

var res = mldb.get('/v1/datasets/iris');
try {
    assertEqual(res['json']['status']['rowCount'], 150);
} catch (e) {
    mldb.log(res);
    throw e;
}

res = mldb.get('/v1/datasets/iris/query', { limit: 10, format: 'table', orderBy: 'CAST (rowName() AS NUMBER)'});

mldb.log(res.json);

var titanicConfig = {
    type: 'text.csv.tabular',
    id: 'titanic',
    params: {
        dataFileUrl: 'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv'
    }

};

mldb.createDataset(titanicConfig);

var res = mldb.get('/v1/datasets/titanic/query', { limit: 10, format: 'table', orderBy: 'rowName()'});

mldb.log(res.json);

try {

    var dataset_config = {
        type: 'text.csv.tabular',
        id: 'test',
        params: {
            dataFileUrl: 'file://modes20130525-0705.csv',
            delimiter: '|'
        }
    };

    var dataset = mldb.createDataset(dataset_config);

    var res = mldb.get('/v1/datasets/test/query', { limit: 10, format: 'table'});

    mldb.log(res);
} catch (e) {
}

titanicConfig = {
    type: 'text.csv.tabular',
    id: 'titanic2',
    params: {
        dataFileUrl: 'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv',
        named: 'lineNumber() % 10'
    }

};

try {
    mldb.createDataset(titanicConfig);
    throw "Shouldn't have succeeded";
} catch (e) {
    mldb.log("got exception", e);
    assertEqual(e.error, "Duplicate row name in CSV dataset");
}


// Test correctness of parser
var correctnessConfig = {
    type: 'text.csv.tabular',
    id: 'correctness',
    params: {
        dataFileUrl: 'https://raw.githubusercontent.com/uniVocity/csv-parsers-comparison/master/src/main/resources/correctness.csv'
    }
};

// TODO: this requires support for multi-line CSV files
//mldb.createDataset(correctnessConfig);

//var res = mldb.get("/v1/query", { q: 'select * from correctness order by rowName()' });

mldb.log(res);

// Test loading of large file (MLDB-806, MLDB-807)
var citiesConfig = {
    type: 'text.csv.tabular',
    id: 'cities',
    params: {
        dataFileUrl: 'http://www.maxmind.com/download/worldcities/worldcitiespop.txt.gz',
        encoding: 'latin1'
    }
};

mldb.createDataset(citiesConfig);

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
      "min(cast (rowName() as integer))",
      "max(cast (rowName() as integer))"
   ],
   [ "[]", 3173958, 2, 3173959 ]
];

assertEqual(res, expected);

var res = mldb.get("/v1/query", { q: 'select * from cities where cast (rowName() as integer) in (2, 1000, 1000000, 2000000, 3000000, 3173959) order by cast (rowName() as integer)' }).json;

mldb.log(res);

expected = [
   {
      "columns" : [
         [ "Country", "ad", "1970-01-01T00:00:00Z" ],
         [ "City", "aixas", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "Aixàs", "1970-01-01T00:00:00Z" ],
         [ "Region", 6, "1970-01-01T00:00:00Z" ],
         [ "Population", null, "1970-01-01T00:00:00Z" ],
         [ "Latitude", 42.48333330, "1970-01-01T00:00:00Z" ],
         [ "Longitude", 1.46666670, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "2ef190d7a29f2916",
      "rowName" : 2
   },
   {
      "columns" : [
         [ "Country", "af", "1970-01-01T00:00:00Z" ],
         [ "City", "`abd ur rahim khel", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "`Abd ur Rahim Khel", "1970-01-01T00:00:00Z" ],
         [ "Region", 27, "1970-01-01T00:00:00Z" ],
         [ "Population", null, "1970-01-01T00:00:00Z" ],
         [ "Latitude", 33.9111520, "1970-01-01T00:00:00Z" ],
         [ "Longitude", 68.4411010, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "f03303280841601c",
      "rowName" : 1000
   },
   {
      "columns" : [
         [ "Country", "gb", "1970-01-01T00:00:00Z" ],
         [ "City", "ryde", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "Ryde", "1970-01-01T00:00:00Z" ],
         [ "Region", "G2", "1970-01-01T00:00:00Z" ],
         [ "Population", 24107, "1970-01-01T00:00:00Z" ],
         [ "Latitude", 50.7166670, "1970-01-01T00:00:00Z" ],
         [ "Longitude", -1.1666670, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "3514bc1ed7064254",
      "rowName" : 1000000
   },
   {
      "columns" : [
         [ "Country", "ng", "1970-01-01T00:00:00Z" ],
         [ "City", "kajia", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "Kajia", "1970-01-01T00:00:00Z" ],
         [ "Region", 44, "1970-01-01T00:00:00Z" ],
         [ "Population", null, "1970-01-01T00:00:00Z" ],
         [ "Latitude", 12.62740, "1970-01-01T00:00:00Z" ],
         [ "Longitude", 10.81920, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "bb7fbc8842295295",
      "rowName" : 2000000
   },
   {
      "columns" : [
         [ "Country", "us", "1970-01-01T00:00:00Z" ],
         [ "City", "greasy ridge", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "Greasy Ridge", "1970-01-01T00:00:00Z" ],
         [ "Region", "OH", "1970-01-01T00:00:00Z" ],
         [ "Population", null, "1970-01-01T00:00:00Z" ],
         [ "Latitude", 38.64527780, "1970-01-01T00:00:00Z" ],
         [ "Longitude", -82.42111110, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "ec051b54935e150d",
      "rowName" : 3000000
   },
   {
      "columns" : [
         [ "Country", "zw", "1970-01-01T00:00:00Z" ],
         [ "City", "zvishavane", "1970-01-01T00:00:00Z" ],
         [ "AccentCity", "Zvishavane", "1970-01-01T00:00:00Z" ],
         [ "Region", 7, "1970-01-01T00:00:00Z" ],
         [ "Population", 79876, "1970-01-01T00:00:00Z" ],
         [ "Latitude", -20.33333330, "1970-01-01T00:00:00Z" ],
         [ "Longitude", 30.03333330, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "ac6161ca77cad488",
      "rowName" : 3173959
   }
];

assertEqual(res, expected, "City populations CSV");

// Test loading of broken file (MLDB-994)
// broken that fails
var brokenConfigFail = {
    type: 'text.csv.tabular',
    id: 'broken_fail',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-749_broken_csv.csv',
        encoding: 'latin1',
    }
};

var failed = false;
try {
    var res = mldb.createDataset(brokenConfigFail);
} catch (e) {
    failed = true;
}
if(!failed) {
    throw "did not throw !!"
}


//broken that doesn't fail
var brokenConfig = {
    type: 'text.csv.tabular',
    id: 'broken',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-749_broken_csv.csv',
        encoding: 'latin1',
        ignoreBadLines: true
    }
};

mldb.createDataset(brokenConfig);


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

var res = mldb.get("/v1/datasets/broken")
mldb.log(res);
if(res["json"]["status"]["numLineErrors"] != 4) {
    throw "Wrong number skipped!";
}


// Test skip first n lines 
var brokenConfig = {
    type: 'text.csv.tabular',
    id: 'skippinnn',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-749_broken_csv.csv',
        encoding: 'latin1',
        ignoreBadLines: true,
        offset: 2
    }
};

mldb.createDataset(brokenConfig);

var res = mldb.get("/v1/query", { q: 'select * from skippinnn order by a ASC limit 10', format: 'table' });
mldb.log(res);
if(res["json"].length != 3) {
    throw "Wrong number !!";
}

var gotErr = false;
try {
    mldb.createDataset({
        type: 'text.csv.tabular',
        id: 'badHeaderRowName',
        params: {
            dataFileUrl: 'file://mldb/testing/MLDB-749_bad_header_row_name.csv',
            encoding: 'latin1',
            named: 'c'
        }
    });
} catch (e) {
    gotErr = true;
}

if (!gotErr) {
    throw "Should have caught bad rowNameColumn";
}

mldb.createDataset({
    type: 'text.csv.tabular',
    id: 'headerRowName',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-749_bad_header_row_name.csv',
        encoding: 'latin1',
        select: '* EXCLUDING (a)',
        named: 'a'
    }
});

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
    var offsetLimitConfig = {
        type: 'text.csv.tabular',
        id: dataset,
        params: {
            dataFileUrl: 'https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv',
            offset: offset,
            limit: limit
        }
    };

    mldb.createDataset(offsetLimitConfig);

    var res = mldb.get("/v1/query", { q: 'select count(*) as count from ' + dataset });
    mldb.log(res["json"]);
    return res["json"][0].columns[0][1];
}

var totalSize = getCountWithOffsetLimit("test1", 0, -1);
assertEqual(getCountWithOffsetLimit("test2", 0, 10), 10, "expecting 10 rows only");
assertEqual(getCountWithOffsetLimit("test3", 0, totalSize + 2000), totalSize, "we can't get more than what there is!");
assertEqual(getCountWithOffsetLimit("test4", 10, -1), totalSize - 10, "expecting all set except 10 rows");


"success"
