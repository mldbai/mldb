// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** Test that a decision tree can split purely on feature is or is not missing. */
function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

// Creates a dataset with 50% density, which causes bucketed training to be used
function createDenseDataset()
{
    mldb.del('/v1/datasets/test');

    var dataset_config = {
        'type'    : 'sparse.mutable',
        'id'      : 'test',
    };

    var dataset = mldb.createDataset(dataset_config)

    var ts = new Date("2015-01-01");

    function recordExample(row, label, x, y)
    {
        dataset.recordRow(row, [ [ "label", label, ts ], ["x", x, ts], ["y", y, ts ] ]);
    }

    // variable x is a dummy variable; it can't be split on
    // variable y can perfectly separate the data based upon whether it's missing or not

    recordExample("exf1", 0, 1);
    recordExample("exf2", 0, 3);
    recordExample("exf3", 0, 5);
    recordExample("exf4", 0, 7);
    recordExample("exf5", 0, 9);

    recordExample("ext1", 1, 2,   1);
    recordExample("ext2", 1, 4,   1);
    recordExample("ext3", 1, 6,   1);
    recordExample("ext4", 1, 8,   1);
    recordExample("ext5", 1, 10,  1);

    dataset.commit()
}

// Creates a dataset with 50% density, which causes bucketed training to be used
function createSparseDataset()
{
    mldb.del('/v1/datasets/test');

    var dataset_config = {
        'type'    : 'sparse.mutable',
        'id'      : 'test',
    };

    var dataset = mldb.createDataset(dataset_config)

    var ts = new Date("2015-01-01");

    function recordExample(row, label, x, y)
    {
        dataset.recordRow(row, [ [ "label", label, ts ], ["x", x, ts], ["y", y, ts ] ]);
    }

    // variable x is a dummy variable; it can't be split on
    // variable y can perfectly separate the data based upon whether it's missing or not

    recordExample("exf1", 0, 1);
    recordExample("exf2", 0, 3);
    recordExample("exf3", 0, 5);
    recordExample("exf4", 0, 7);
    recordExample("exf5", 0, 9);
    recordExample("exf6", 0, 1);
    recordExample("exf7", 0, 3);
    recordExample("exf8", 0, 5);
    recordExample("exf9", 0, 7);
    recordExample("exf10", 0, 9);
    recordExample("exf11", 0, 1);
    recordExample("exf12", 0, 3);
    recordExample("exf13", 0, 5);
    recordExample("exf14", 0, 7);
    recordExample("exf15", 0, 9);
    recordExample("exf16", 0, 1);
    recordExample("exf17", 0, 3);
    recordExample("exf18", 0, 5);
    recordExample("exf19", 0, 7);
    recordExample("exf20", 0, 9);
    recordExample("exf21", 0, 1);
    recordExample("exf22", 0, 3);
    recordExample("exf23", 0, 5);
    recordExample("exf24", 0, 7);
    recordExample("exf25", 0, 9);
    recordExample("exf26", 0, 1);
    recordExample("exf27", 0, 3);
    recordExample("exf28", 0, 5);
    recordExample("exf29", 0, 7);
    recordExample("exf30", 0, 9);

    recordExample("ext1", 1, 2,   1);
    recordExample("ext2", 1, 4,   1);
    recordExample("ext3", 1, 6,   1);
    recordExample("ext4", 1, 8,   1);
    recordExample("ext5", 1, 10,  1);

    dataset.commit()
}

function trainClassifier(algorithm)
{
    mldb.del('/v1/procedures/cls_train');
    mldb.del('/v1/functions/classifier');

    var modelFileUrl = "file://tmp/MLDB-785-decision-tree-missing.cls";

    var trainClassifierProcedureConfig = {
        type: "classifier.train",
        params: {
            trainingData: "select {x,y} as features, label from test",
            configuration: {
                dt: {
                    type: "decision_tree",
                    verbosity: 3,
                    max_depth: 1,
                    trace: 5
                },
                stump: { 
                    type: "stump",
                    verbosity: 3,
                    trace: 5,
                    update_alg: "prob"
                },
                boosted_trees: {
                    type: "boosting",
                    min_iter: 1,
                    max_iter: 1,
                    weak_learner: {
                        type: "decision_tree",
                        verbosity: 3,
                        max_depth: 1,
                        trace: 5
                    }
                },
                boosting_and_stumps: {
                    type: "boosting",
                    min_iter: 1,
                    max_iter: 1,
                    weak_learner: {
                        type: "stump",
                        verbosity: 3,
                        trace: 5,
                        update_alg: "prob"
                    }
                },
                boosted_stumps: {
                    type: "boosted_stumps",
                    min_iter: 1,
                    max_iter: 1,
                    verbosity: 5
                }
            },
            algorithm: algorithm,
            modelFileUrl: modelFileUrl,
            equalizationFactor: 0.0,
            mode: "boolean",
            functionName: "classifier"
        }
    };

    var procedureOutput
        = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig);

    plugin.log("procedure output", procedureOutput);

    var trainingOutput
        = mldb.put("/v1/procedures/cls_train/runs/1", {});

    plugin.log("training output", trainingOutput);

}

function testOutput(algorithm)
{
    var model = mldb.get("/v1/functions/classifier/details").json.model;

    mldb.log(model);

    if (algorithm == "stump") {
        assertEqual(model.params.split.feature, "y");
        assertEqual(model.params.split.op, "PRESENT");
        assertEqual(model.params.action.missing, [ 1, 0 ]);
        assertEqual(model.params.action["true"], [ 0, 1 ]);
        assertEqual(model.params.action["false"], [ 0, 0 ]);
    } else if (algorithm == "dt") {
        assertEqual(model.params.tree.root.split.feature, "y");
        assertEqual(model.params.tree.root.split.op, "PRESENT");
        assertEqual(model.params.tree.root.missing.pred, [ 1, 0 ]);
        assertEqual(model.params.tree.root["true"].pred, [ 0, 1 ]);
    }
    else if (algorithm == "boosted_trees") {
        assertEqual(model.params.classifiers[0].params.tree.root.split.feature, "y");
        assertEqual(model.params.classifiers[0].params.tree.root.split.op, "PRESENT");
        assertEqual(model.params.classifiers[0].params.tree.root.missing.pred, [ 1, 0 ]);
        assertEqual(model.params.classifiers[0].params.tree.root["true"].pred, [ 0, 1 ]);
    }

    // Applying it with y present should return 1
    var result1 = mldb.get("/v1/functions/classifier/application",
                           { input: { features: { y:1 }}}).json.output.score;

    // Applying it with y missing should return 0
    var result2 = mldb.get("/v1/functions/classifier/application",
                           { input: { features: { q:1 }}}).json.output.score;

    if (algorithm != "boosted_stumps") {
        assertEqual(result1, 1);
        assertEqual(result2, 0);
    }
    else {
        assertEqual(result1 > result2, true);
    }
}

function testWithDenseDataset(algorithm)
{
    createDenseDataset();
    trainClassifier(algorithm);
    testOutput(algorithm);
}

function testWithSparseDataset(algorithm)
{
    createSparseDataset();
    trainClassifier(algorithm);
    testOutput(algorithm);
}

testWithDenseDataset("stump");
testWithSparseDataset("stump");
testWithDenseDataset("dt");
testWithSparseDataset("dt");
testWithDenseDataset("boosted_trees");
testWithDenseDataset("boosting_and_stumps");
testWithDenseDataset("boosted_stumps");

"success"
