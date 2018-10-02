// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

/**
 * MLDB-749-csv-dataset.js
 * Nicolas, 2015-07-23
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.
 **/

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
unittest.assertEqual(res["responseCode"], 201);

var res = mldb.put("/v1/procedures/csv_proc/runs/myrun", {});

mldb.log(res);

unittest.assertEqual(res['json']['status']['numLineErrors'], 0);

res = mldb.get('/v1/datasets/iris');
unittest.assertEqual(res['json']['status']['rowCount'], 150);
mldb.log(res);


res = mldb.get('/v1/datasets/iris/query', { limit: 10, format: 'table', orderBy: 'CAST (rowName() AS NUMBER)'});

mldb.log(res.json);

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : {
            url: "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
            etag: {
                authority: "https://raw.githubusercontent.com",
                value: "fdb7d4717c5befa93d0f241ac4245bed1a2a10e7"
            },
            sha256: "31ad156ab55993d901bd607828045a3de18cac4144be6dc1c520bc41573a8115",
            sha3: "9a87daef2da4915211114d4cfecae175636a8074ee6dbd17483c237f" 
        },
        outputDataset: {
            id: "titanic",
        },
        runOnCreation: true,
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf);

mldb.log(res);

unittest.assertEqual(res.responseCode, 201);

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
unittest.assertEqual(res['responseCode'], 400);
unittest.assertEqual(res['json']['error'], "Duplicate row name(s) in tabular dataset: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9");

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
unittest.assertEqual(res.json, expected, "quoteChar test");

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
unittest.assertEqual(res['responseCode'], 400);

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
unittest.assertEqual(res.json, expected, "quoteChar test");

"success"
