// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

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
unittest.assertEqual(res['responseCode'], 400);
unittest.assertEqual(res['json']['details']['runError']['details']['lineNumber'], 5);

// MLDB-1404: do it 100 times to ensure we don't terminate

for (var i = 0;  i < 100;  ++i) {
    res = mldb.put("/v1/procedures/csv_proc", brokenConfigFail);
    unittest.assertEqual(res['responseCode'], 400);
    unittest.assertEqual(res['json']['details']['runError']['details']['lineNumber'], 5);
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
unittest.assertEqual(res['responseCode'], 400);
unittest.assertEqual(res['json']['details']['runError']['details']['lineNumber'], 4);

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
unittest.assertEqual(res['json']['status']['firstRun']['status']['numLineErrors'], 4);

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
unittest.assertEqual(res['responseCode'], 400); //bad rowNameColumn

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
unittest.assertEqual(res['json']['status']['rowCount'], 2);

var res = mldb.get("/v1/query", {
    q: 'SELECT * FROM headerRowName ORDER BY rowName() ASC LIMIT 10',
    format: 'table' }
);
try {
    unittest.assertEqual(res['json'].length, 3);
    unittest.assertEqual(res['json'][0], ["_rowName", "b"]);
    unittest.assertEqual(res['json'][1], ["jambon", 4]);
} catch (e) {
    mldb.log(res);
    throw e;
}

var res = mldb.get("/v1/query", {
    q: 'SELECT * FROM headerRowName ORDER BY rowName() DESC LIMIT 10',
    format: 'table' }
);
try {
    unittest.assertEqual(res['json'].length, 3);
    unittest.assertEqual(res['json'][0], ["_rowName", "b"]);
    unittest.assertEqual(res['json'][2], ["jambon", 4]);
} catch (e) {
    mldb.log(res);
    throw e;
}

