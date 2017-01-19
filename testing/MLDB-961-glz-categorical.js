// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
        if (i == 3) {
            if(fields[i] > 1.00) {
                tuples.push(["petalCat", "long", now]);
            }
            else {
                tuples.push(["petalCat", "short", now]);
            }
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
        trainingData: { 
            select: "{* EXCLUDING (class)} as features, class as label",
            from: "iris"
        },
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                regularization: 'none',
                condition: true
            }
        },
        algorithm: "glz",
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
            -0.1605280190706253,
            0,
            0.2956319749355316,
            0.04642588272690773,
            0.2305849939584732,
            -0.01987788267433643,
            -0.1272970438003540
         ],
         [
            0.02964247018098831,
            0,
            -0.8972460031509399,
            0.03677969053387642,
            -0.4046253561973572,
            -0.6151338815689087,
            2.321021080017090
         ],
         [
            0.1308855563402176,
            0,
            0.6016140580177307,
            -0.08320557326078415,
            0.1740403622388840,
            0.6350117325782776,
            -1.193723917007446
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
