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
            from : { id: "iris" }
        },
        configuration: {
            glz: {
                type: "glz",
                verbosity: 3,
                normalize: false,
                link_function: 'linear',
                regularization: 'none'
            }
        },
        algorithm: "glz",
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

plugin.log(functionInfo.json);

var expected = {
   "input" : [
      {
         "hasUnknownColumns" : false,
         "hasUnknownColumnsRecursive" : false,
         "isConstant" : false,
         "kind" : "row",
         "knownColumns" : [
            {
               "columnName" : "features",
               "sparsity" : "dense",
               "valueInfo" : {
                  "hasUnknownColumns" : false,
                  "hasUnknownColumnsRecursive" : false,
                  "isConstant" : false,
                  "kind" : "row",
                  "knownColumns" : [
                     {
                        "columnName" : "petal length",
                        "sparsity" : "dense",
                        "valueInfo" : {
                           "isConstant" : false,
                           "kind" : "scalar",
                           "scalar" : "float",
                           "type" : "MLDB::Float32ValueInfo"
                        }
                     },
                     {
                        "columnName" : "petal width",
                        "sparsity" : "dense",
                        "valueInfo" : {
                           "isConstant" : false,
                           "kind" : "scalar",
                           "scalar" : "float",
                           "type" : "MLDB::Float32ValueInfo"
                        }
                     },
                     {
                        "columnName" : "sepal length",
                        "sparsity" : "dense",
                        "valueInfo" : {
                           "isConstant" : false,
                           "kind" : "scalar",
                           "scalar" : "float",
                           "type" : "MLDB::Float32ValueInfo"
                        }
                     },
                     {
                        "columnName" : "sepal width",
                        "sparsity" : "dense",
                        "valueInfo" : {
                           "isConstant" : false,
                           "kind" : "scalar",
                           "scalar" : "float",
                           "type" : "MLDB::Float32ValueInfo"
                        }
                     }
                  ],
                  "type" : "MLDB::RowValueInfo"
               }
            }
         ],
         "type" : "MLDB::RowValueInfo"
      }
   ],
   "output" : {
      "hasUnknownColumns" : false,
      "hasUnknownColumnsRecursive" : false,
      "isConstant" : false,
      "kind" : "row",
      "knownColumns" : [
         {
            "columnName" : "scores",
            "offset" : 0,
            "sparsity" : "dense",
            "valueInfo" : {
               "hasUnknownColumns" : false,
               "hasUnknownColumnsRecursive" : false,
               "isConstant" : false,
               "kind" : "row",
               "knownColumns" : [
                  {
                     "columnName" : "Iris-setosa",
                     "offset" : 0,
                     "sparsity" : "dense",
                     "valueInfo" : {
                        "isConstant" : false,
                        "kind" : "scalar",
                        "scalar" : "float",
                        "type" : "MLDB::Float32ValueInfo"
                     }
                  },
                  {
                     "columnName" : "Iris-versicolor",
                     "offset" : 1,
                     "sparsity" : "dense",
                     "valueInfo" : {
                        "isConstant" : false,
                        "kind" : "scalar",
                        "scalar" : "float",
                        "type" : "MLDB::Float32ValueInfo"
                     }
                  },
                  {
                     "columnName" : "Iris-virginica",
                     "offset" : 2,
                     "sparsity" : "dense",
                     "valueInfo" : {
                        "isConstant" : false,
                        "kind" : "scalar",
                        "scalar" : "float",
                        "type" : "MLDB::Float32ValueInfo"
                     }
                  }
               ],
               "type" : "MLDB::RowValueInfo"
            }
         }
      ],
      "type" : "MLDB::RowValueInfo"
   }
};

assertEqual(functionInfo.json, expected);

"success"
