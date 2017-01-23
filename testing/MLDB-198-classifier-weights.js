// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y, label, test)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts], ['test', test, ts ] ]);
}

/*
  We set up our example like this

  ^
  | 0              0,1
  |
  |
  |
  | 0              1
  +------------------------>

  and we vary the weights of the two points in the upper right corner:
  - equal weights
  - 0 has a much higher weight
  - 1 has a much higher weight

  We then train linear separators under the three scenarios, and check
  that this has the correct effect on the output.
*/

recordExample("ex00", 0, 0, 0, 'none');
recordExample("ex10", 1, 0, 1, 'none');
recordExample("ex01", 0, 1, 1, 'none');
recordExample("ex111", 1, 1, 1, 'isone');
recordExample("ex110", 1, 1, 0, 'iszero');

dataset.commit()


function checkSuccess(response)
{
    if (response.responseCode > 200 && response.responseCode < 400)
        return;
    throw "Error: " + JSON.stringify(response);
}


function trainClassifier(name, weight)
{

    var modelFileUrl = "file://tmp/MLDB-198_" + name + ".cls";

    var trainClassifierProcedureConfig = {
        type: "classifier.train",
        params: {
            trainingData: { 
                select: "{x, y} as features, label, " + weight  + " as weight",
                from: "test"
            },
            configuration: {
                glz: {
                    type: "glz",
                    verbosity: 3,
                    normalize: false,
                    link: 'linear',
                    "regularization": 'l2'
                }
            },
            algorithm: "glz",
            modelFileUrl: modelFileUrl,
            equalizationFactor: 0.0
        }
    };

    var procedureOutput
        = mldb.put("/v1/procedures/cls_train_" + name,
                   trainClassifierProcedureConfig);
    
    plugin.log("procedure output", procedureOutput);
    
    var trainingOutput
        = mldb.put("/v1/procedures/cls_train_" + name + "/runs/1", {});

    plugin.log("training output", trainingOutput);


    var functionConfig = {
        type: "classifier",
        params: {
            modelFileUrl: modelFileUrl
        }
    };

    var createFunctionOutput
        = mldb.put("/v1/functions/" + name, functionConfig);
    plugin.log("classifier function output", createFunctionOutput);

    selected = mldb.get("/v1/functions/" + name + "/application",
                        {input: { features: {x:1,y:1}}});
    
    var result = selected.json.score;

    plugin.log(selected);

    return result;
}

var score = trainClassifier("unweighted", "1.0");
var score1 = trainClassifier("isone", "1 + 1000 * (test = 'isone')");
var score0 = trainClassifier("iszero", "1 + 1000 * (test = 'iszero')");

plugin.log(score, score1, score0);

// Check that they are in the order (score 0, score, score 1)
if (score > score1)
    throw "Balanced score is higher than score with 1";
if (score0 > score)
    throw "Balanced score is lower than score with 0";

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
