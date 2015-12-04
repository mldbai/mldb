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
        if (i == 3 && fields[i] > 1.00) {
            tuples.push(["petalCat", "long", now]);
        }
        else {
            tuples.push(["petalCat", "short", now]);
        }
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
                ridge_regression: false,
                condition: true
            }
        },
        algorithm: "glz",
        select: "* EXCLUDING (class)",
        label: "class",
        weight: "1",
        equalizationFactor: 0.0,
        modelFileUrl: "file://tmp/MLDB-961.cls",
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
        modelFileUrl: "file://tmp/MLDB-961.cls"
    }
};

var functionOutput = mldb.put("/v1/functions/iris_cls", functionConfig);

plugin.log(functionOutput);

assertEqual(functionOutput.responseCode, 201);


var expected = {
   "params" : {
      "addBias" : true,
      "features" : [
         {
            "extract" : "VALUE",
            "feature" : "petal length"
         },
         {
            "category" : "long",
            "extract" : "VALUE_EQUALS",
            "feature" : "petalCat"
         },
         {
            "category" : "short",
            "extract" : "VALUE_EQUALS",
            "feature" : "petalCat"
         },
         {
            "extract" : "VALUE",
            "feature" : "sepal length"
         },
         {
            "extract" : "VALUE",
            "feature" : "sepal width"
         },
         {
            "extract" : "VALUE",
            "feature" : "petal width"
         }
      ],
      "link" : "LINEAR",
      "weights" : [
         [
            -0.1344368159770966,
            -0.1527329087257385,
            0.1884149461984634,
            0.04914193972945213,
            0.2088359445333481,
            -0.04185565933585167,
            0
         ],
         [
            -0.06347871571779251,
            2.095180511474609,
            1.186962485313416,
            0.06839417666196823,
            -0.3454444408416748,
            -0.4863662421703339,
            0
         ],
         [
            0.1979155391454697,
            -0.9424476623535156,
            -0.3753774762153625,
            -0.1175361126661301,
            0.1366084963083267,
            0.5282219052314758,
            0
         ]
      ]
   },
   "type" : "GLZ"
};

var details = mldb.get("/v1/functions/iris_cls/details");

assertEqual(details.json.model, expected);


//////////// try with condition is false

trainClassifierProcedureConfig.params.configuration.glz.condition = false;

procedureOutput
    = mldb.put("/v1/procedures/cls_train2", trainClassifierProcedureConfig);

plugin.log("procedure output", procedureOutput);

trainingOutput
    = mldb.put("/v1/procedures/cls_train2/runs/1", {});

plugin.log("training output", trainingOutput);

assertEqual(trainingOutput.responseCode, 201);

var functionOutput = mldb.put("/v1/functions/iris_cls2", functionConfig);

plugin.log(functionOutput);

assertEqual(functionOutput.responseCode, 201);

var details = mldb.get("/v1/functions/iris_cls2/details");

expected = [
    {
        "extract" : "VALUE",
        "feature" : "petal length"
    },
    {
        "category" : "long",
        "extract" : "VALUE_EQUALS",
        "feature" : "petalCat"
    },
    {
        "category" : "short",
        "extract" : "VALUE_EQUALS",
        "feature" : "petalCat"
    },
    {
        "extract" : "VALUE",
        "feature" : "sepal length"
    },
    {
        "extract" : "VALUE",
        "feature" : "sepal width"
    },
    {
        "extract" : "VALUE",
        "feature" : "petal width"
    }
];

assertEqual(details.json.model.params.features, expected);

"success"
