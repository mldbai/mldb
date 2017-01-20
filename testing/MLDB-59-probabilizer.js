// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'reddit_dataset',
};

var dataset = mldb.createDataset(dataset_config)
plugin.log("Reddit data loader created dataset")

var dataset_address = 'http://public.mldb.ai/reddit.csv.gz'
var now = new Date();

var stream = mldb.openStream(dataset_address);

var numLines = 10000;

var lineNum = 0;
while (!stream.eof() && lineNum < numLines) {
    ++lineNum;
    var line = stream.readLine();
    var fields = line.split(',');
    var tuples = [];
    for (var i = 1;  i < fields.length;  ++i) {
        tuples.push([fields[i], '1', now]);
    }

    dataset.recordRow(fields[0], tuples);
}

plugin.log("Committing dataset")
dataset.commit()


function checkSuccess(response)
{
    if (response.responseCode > 200 && response.responseCode < 400)
        return;
    throw "Error: " + JSON.stringify(response);
}

function checkOutput(title, response)
{
    plugin.log(title, response);
    checkSuccess(response);
}

// Create and train an SVD procedure.  It gets 1/4 of the data

var trainSvd = true;

if (trainSvd) {

    var svdConfig = {
        type: "svd.train",
        params: {
            numSingularValues: 200,
            trainingData: { from : {"id": "reddit_dataset" },
            select: "* EXCLUDING (adventuretime)"
                             },
            columnOutputDataset: { "id": "svd_output", type: "embedding" },
            rowOutputDataset: { "id": "svd_embedding", type: "embedding" }
        }
    };

    checkOutput("svd procedure", mldb.put("/v1/procedures/reddit_svd", svdConfig));
    checkOutput("svd training", mldb.put("/v1/procedures/reddit_svd/runs/1", {}));
}

// Merge the two datasets to get the SVD

var mergedConfig = {
    type: "merged",
    id: "reddit_embeddings",
    params: {
        "datasets": [
            { "id": "reddit_dataset" },
            { "id": "svd_embedding" }
        ]
    }
};

var mergedDataset = mldb.createDataset(mergedConfig);

plugin.log(mldb.perform("GET", "/v1/datasets/reddit_embeddings"));

var query = [
    [ "select", "count(1)" ],
    [ "groupBy", "1" ],
    [ "where", "adventuretime IS NOT NULL" ],
    [ "limit", "10" ]];
plugin.log(mldb.perform("GET", "/v1/datasets/reddit_embeddings/query", query), null, 4);

var trainClassifier = true;

if (trainClassifier) {

    var trainClassifierProcedureConfig = {
        id: "reddit_cls_train",
        type: "classifier.train",
        params: {
            trainingData: { 
                select: '{embedding*} as features, adventuretime IS NOT NULL as label',
                where: "rowHash() % 4 = 1",
                from : "reddit_embeddings"
            },
            configuration: {
                bbdt: {
                    type: "bagging",
                    verbosity: 3,
                    weak_learner:  {
                        type: "boosting",
                        verbosity: 3,
                        weak_learner: {
                            type: "decision_tree",
                            max_depth: 3,
                            verbosity: 0,
                            update_alg: "gentle",
                            random_feature_propn: 0.5,
                        },
                        min_iter: 5,
                        max_iter: 30,
                    },
                    num_bags: 5,
                },
                glz: {
                    type: "glz",
                    verbosity: 3,
                    normalize: true,
                    regularization: 'l2'
                }
            },
            algorithm: "glz",
            modelFileUrl: "file://tmp/reddit.cls",
            equalizationFactor: 1.0
        }
    };

    checkOutput("cls procedure", mldb.put("/v1/procedures/reddit_cls_train", trainClassifierProcedureConfig));
    checkOutput("cls training", mldb.put("/v1/procedures/reddit_cls_train/runs/1", {}));
}

var classifierFunctionConfig = {
    id: "classifier",
    type: "classifier",
    params: {
        modelFileUrl: "file://tmp/reddit.cls"
    }
};

checkOutput("classifier", mldb.put("/v1/functions/classifier", classifierFunctionConfig));

var trainProbabilizer = true;

if (trainProbabilizer) {

    var trainProbabilizerProcedureConfig = {
        id: "reddit_prob_train",
        type: "probabilizer.train",
        params: {
            trainingData: { 
                select: "classifier({{ * EXCLUDING (adventuretime)} AS features})[score] as score, adventuretime IS NOT NULL as label",
                where: "rowHash() % 4 = 2",
                from: { id: "reddit_embeddings" },
            },
            modelFileUrl: "file://tmp/reddit_probabilizer.json",
            functionName: "probabilizer"
       }
    };

    checkOutput("prob-train procedure", mldb.put("/v1/procedures/reddit_prob_train", trainProbabilizerProcedureConfig));
    checkOutput("prob-train training", mldb.put("/v1/procedures/reddit_prob_train/runs/1", {}));
}

var testClassifier = true;

if (testClassifier) {
    var testClassifierProcedureConfig = {
        id: "accuracy",
        type: "classifier.test",
        params: {
            testingData: "select adventuretime IS NOT NULL as label, \
                          probabilizer(classifier({{ * EXCLUDING (adventuretime) } AS features}))[prob] as score \
                          from reddit_embeddings where rowHash() % 4 = 3",
            outputDataset: { id: "cls_test_results", type: "sparse.mutable" }
        }
    };

    checkOutput("accuracy procedure", mldb.put("/v1/procedures/accuracy", testClassifierProcedureConfig));
    checkOutput("accuracy training", mldb.put("/v1/procedures/accuracy/runs/1", {}));
}

plugin.log(mldb.get("/v1/datasets/cls_test_results/query",
                    {select:'*',orderBy:'index',limit:100}).json);


// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
