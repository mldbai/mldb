// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */


var file1 = "file://mldb/testing/dataset/iris.data";

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'iris'
};

var dataset = mldb.createDataset(dataset_config)

var now = new Date();

var stream = mldb.openStream(file1);

var numLines = 500;

var colNames = ["sepal length", "sepal width", "petal length", "petal width", "class"];

var lineNum = 0;
while (!stream.eof() && lineNum < numLines) {
    ++lineNum;
    var line;
    try {
        line = stream.readLine();
    } catch (e) {
        break;
    }
    var fields = line.split(',');
    if (fields.length != 5)
        continue;
    var tuples = [];
    for (var i = 0;  i < fields.length;  ++i) {
        tuples.push([colNames[i], fields[i] + 0.0, now]);
    }
    
    dataset.recordRow("line"+(lineNum + 1), tuples);
}

plugin.log("Committing dataset with", lineNum, "lines")
dataset.commit()

plugin.log(mldb.get("/v1/datasets/iris/query", {limit:10}).json);

var svd_config = {
    'type' : 'svd.train',
    'params' :
    {
        "trainingData": {"from" : {"id": "iris"},
                         "select" : "\"petal\", \"width\", \"sepal\", \"length\"" },
        "columnOutputDataset": {
            "type": "sparse.mutable",
            "id": "svd_iris_col"
        },
        "rowOutputDataset": {
            "id": "svd_iris_row",
            'type': "embedding"
        },
        "numSingularValues": 4,
        "numDenseBasisVectors": 2
    }
};

function checkSuccess(response)
{
    if (response.responseCode > 200 && response.responseCode < 400)
        return;
    throw "Error: " + JSON.stringify(response);
}

var procedureOutput = mldb.put("/v1/procedures/svd_iris", svd_config);

checkSuccess(procedureOutput);

var trainingOutput = mldb.put("/v1/procedures/svd_iris/runs/1", {});

plugin.log("training output", trainingOutput);

checkSuccess(trainingOutput);

"success"
