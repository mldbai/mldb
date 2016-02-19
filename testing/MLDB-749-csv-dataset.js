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

/**/
var irisConfig = {
    type: 'text.csv.tabular',
    id: 'iris',
    params: {
        dataFileUrl: 'file://mldb/testing/dataset/iris.data',
        headers: [ 'sepal length', 'sepal width', 'petal length', 'petal width', 'class' ],
        ignoreBadLines: true
    }
};

res = mldb.post('/v1/datasets', irisConfig);
assertEqual(res["responseCode"], 201);

res = mldb.get('/v1/datasets/iris');
assertEqual(res['json']['status']['rowCount'], 150);
assertEqual(res['json']['status']['numLineErrors'], 0);
mldb.log(res);


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
        "columns": [
            ["Country","ad","2012-05-03T03:14:46Z"],
            ["City","aixas","2012-05-03T03:14:46Z"],
            ["AccentCity","Aixàs","2012-05-03T03:14:46Z"],
            ["Region",6,"2012-05-03T03:14:46Z"],
            ["Population",null,"2012-05-03T03:14:46Z"],
            ["Latitude",42.4833333,"2012-05-03T03:14:46Z"],
            ["Longitude",1.4666667,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"2ef190d7a29f2916",
        "rowName":2
    } ,{
        "columns":[
            ["Country","af","2012-05-03T03:14:46Z"],
            ["City","`abd ur rahim khel","2012-05-03T03:14:46Z"],
            ["AccentCity","`Abd ur Rahim Khel","2012-05-03T03:14:46Z"],
            ["Region",27,"2012-05-03T03:14:46Z"],
            ["Population",null,"2012-05-03T03:14:46Z"],
            ["Latitude",33.911152,"2012-05-03T03:14:46Z"],
            ["Longitude",68.441101,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"f03303280841601c",
        "rowName":1000
    },{
        "columns":[
            ["Country","gb","2012-05-03T03:14:46Z"],
            ["City","ryde","2012-05-03T03:14:46Z"],
            ["AccentCity","Ryde","2012-05-03T03:14:46Z"],
            ["Region","G2","2012-05-03T03:14:46Z"],
            ["Population",24107,"2012-05-03T03:14:46Z"],
            ["Latitude",50.716667,"2012-05-03T03:14:46Z"],
            ["Longitude",-1.166667,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"3514bc1ed7064254",
        "rowName":1000000
    },{
        "columns":[
            ["Country","ng","2012-05-03T03:14:46Z"],
            ["City","kajia","2012-05-03T03:14:46Z"],
            ["AccentCity","Kajia","2012-05-03T03:14:46Z"],
            ["Region",44,"2012-05-03T03:14:46Z"],
            ["Population",null,"2012-05-03T03:14:46Z"],
            ["Latitude",12.6274,"2012-05-03T03:14:46Z"],
            ["Longitude",10.8192,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"bb7fbc8842295295",
        "rowName":2000000
    },{
        "columns":[
            ["Country","us","2012-05-03T03:14:46Z"],
            ["City","greasy ridge","2012-05-03T03:14:46Z"],
            ["AccentCity","Greasy Ridge","2012-05-03T03:14:46Z"],
            ["Region","OH","2012-05-03T03:14:46Z"],
            ["Population",null,"2012-05-03T03:14:46Z"],
            ["Latitude",38.6452778,"2012-05-03T03:14:46Z"],
            ["Longitude",-82.4211111,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"ec051b54935e150d",
        "rowName":3000000
    },{
        "columns":[
            ["Country","zw","2012-05-03T03:14:46Z"],
            ["City","zvishavane","2012-05-03T03:14:46Z"],
            ["AccentCity","Zvishavane","2012-05-03T03:14:46Z"],
            ["Region",7,"2012-05-03T03:14:46Z"],
            ["Population",79876,"2012-05-03T03:14:46Z"],
            ["Latitude",-20.3333333,"2012-05-03T03:14:46Z"],
            ["Longitude",30.0333333,"2012-05-03T03:14:46Z"]
        ],
        "rowHash":"ac6161ca77cad488",
        "rowName":3173959
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
        encoding: 'latin1'
    }
};

var failed = false;
try {
    var res = mldb.createDataset(brokenConfigFail);
} catch (e) {
    mldb.log(e);
    assertEqual(e.details.lineNumber, 5, "expected bad line number at 5");
    failed = true;
}
if(!failed) {
    throw "did not throw !!"
}
/**/

var brokenConfigNoHeader = {
    type: 'text.csv.tabular',
    id: 'broken_fail',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-749_broken_csv_no_header.csv',
        encoding: 'latin1',
        headers: ['a', 'b', 'c']
    }
};

var failed = false;
try {
    var res = mldb.createDataset(brokenConfigNoHeader);
} catch (e) {
    mldb.log(e);
    assertEqual(e.details.lineNumber, 4, "expected bad line number at 4");
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

function getCountWithOffsetLimit2(dataset, offset, limit) {
    var offsetLimitConfig = {
        type: "text.csv.tabular",
        id: dataset,
        params: {
            dataFileUrl: "http://s3.amazonaws.com/public.mldb.ai/tweets.gz",
            offset: offset,
            limit: limit,
            delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
            select: "tweet",
            ignoreBadLines: true
        }
    };

    mldb.createDataset(offsetLimitConfig);
    res = mldb.get("/v1/datasets/"+dataset);
    mldb.log(res["json"]);
    return res["json"]["status"]["numLineErrors"] + res["json"]["status"]["rowCount"];
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

//MLDB-1312 specify quotechar
var mldb1312Config = {
    type: 'text.csv.tabular',
    id: 'mldb1312',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-1312-quotechar.csv',
        encoding: 'latin1',
        quotechar: '#'
    }
};

mldb.createDataset(mldb1312Config);

expected = 
[
      [ "_rowName", "a", "b" ],
      [ "2", "a", "b" ],
      [ "3", "a#b", "c" ],
      [ "4", "a,b", "c" ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb1312 order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quotechar test");

var mldb1312Config_b = {
    type: 'text.csv.tabular',
    id: 'mldb1312_b',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-1312-quotechar.csv',
        encoding: 'latin1',
        quotechar: '#',
        delimiter: ''
    }
};

try {
    mldb.createDataset(mldb1312Config_b);
} catch (e) {
    gotErr = true;
}

if (!gotErr) {
    throw "Should have caught bad mldb1312Config_b";
}

var mldb1312Config_c = {
    type: 'text.csv.tabular',
    id: 'mldb1312_c',
    params: {
        dataFileUrl: 'file://mldb/testing/MLDB-1312-quotechar.csv',
        encoding: 'latin1',
        quotechar: '',
        delimiter: ',',
        ignoreBadLines: true
    }
};

mldb.createDataset(mldb1312Config_c);

expected = 
[
   [ "_rowName", "a", "b" ],
   [ "2", "#a#", "b" ],
   [ "3", "#a##b#", "c" ]
];

var res = mldb.get("/v1/query", { q: 'select * from mldb1312_c order by rowName()', format: 'table' });
assertEqual(res.json, expected, "quotechar test");

"success"
