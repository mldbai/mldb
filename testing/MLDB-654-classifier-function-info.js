// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw new Error("Assertion failure: " + msg + ": " + JSON.stringify(expr)
                    + " not equal to " + JSON.stringify(val));
}



var file1 = "file://mldb/testing/dataset/iris.data";

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'iris'
};

var dataset = mldb.createDataset(dataset_config)

var now = new Date("2015-01-01");

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
        tuples.push([colNames[i], (i == 4 ? fields[i] : +fields[i]), now]);
    }

    //plugin.log(tuples);
    
    dataset.recordRow("line"+(lineNum + 1), tuples);
}

plugin.log("Committing dataset with", lineNum, "lines")
dataset.commit()

plugin.log(mldb.get("/v1/datasets/iris/query", {limit:10}).json);


var trainClassifierProcedureConfig = {
    type: "classifier.train",
    params: {
        trainingDataset: { id: "iris" },
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                ridge_regression: false
            }
        },
        algorithm: "glz",
        select: "* EXCLUDING (class)",
        label: "class",
        weight: "1",
        equalizationFactor: 0.0,
        modelFileUrl: "file://tmp/MLDB-654.cls",
        mode: 'categorical'
    }
};

var procedureOutput
    = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

var trainingOutput
    = mldb.put("/v1/procedures/cls_train/runs/1", {});

plugin.log("training output", trainingOutput);

assertEqual(trainingOutput.responseCode, 201);

var functionConfig = {
    type: "classifier",
    params: {
        modelFileUrl: "file://tmp/MLDB-654.cls"
    }
};

var functionOutput = mldb.put("/v1/functions/iris_cls", functionConfig);

plugin.log(functionOutput);

assertEqual(functionOutput.responseCode, 201);

var functionInfo = mldb.get("/v1/functions/iris_cls/info");

plugin.log(functionInfo);

assertEqual(functionInfo.json.input.values.features.valueInfo.kind, "row");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns.length, 4);
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[0].columnName, "petal length");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[0].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[0].valueInfo.scalar, "float");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[1].columnName, "petal width");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[1].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[1].valueInfo.scalar, "float");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[2].columnName, "sepal length");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[2].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[2].valueInfo.scalar, "float");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[3].columnName, "sepal width");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[3].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.input.values.features.valueInfo.knownColumns[3].valueInfo.scalar, "float");

assertEqual(functionInfo.json.output.values.scores.valueInfo.kind, "row");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns.length, 3);
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[0].columnName, '"Iris-setosa"');
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[0].sparsity, "dense");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[0].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[0].valueInfo.scalar, "float");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[1].columnName, '"Iris-versicolor"');
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[1].sparsity, "dense");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[1].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[1].valueInfo.scalar, "float");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[2].columnName, '"Iris-virginica"');
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[2].sparsity, "dense");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[2].valueInfo.kind, "scalar");
assertEqual(functionInfo.json.output.values.scores.valueInfo.knownColumns[2].valueInfo.scalar, "float");

"success"
