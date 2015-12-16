// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Example script to import mnist and run an example */

// Last line of file is the result of the script

var file1 = "http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz";
var file2 = "http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz";
var file3 = "http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz";
var file4 = "http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz";

plugin.serveStaticFolder("/static", "file://mldb/testing/static");



/* from http://yann.lecun.com/exdb/mnist/

   TRAINING SET LABEL FILE (train-labels-idx1-ubyte):

   [offset] [type]          [value]          [description] 
   0000     32 bit integer  0x00000801(2049) magic number (MSB first) 
   0004     32 bit integer  60000            number of items 
   0008     unsigned byte   ??               label 
   0009     unsigned byte   ??               label 
   ........ 
   xxxx     unsigned byte   ??               label
   The labels values are 0 to 9.

   TRAINING SET IMAGE FILE (train-images-idx3-ubyte):

   [offset] [type]          [value]          [description] 
   0000     32 bit integer  0x00000803(2051) magic number 
   0004     32 bit integer  60000            number of images 
   0008     32 bit integer  28               number of rows 
   0012     32 bit integer  28               number of columns 
   0016     unsigned byte   ??               pixel 
   0017     unsigned byte   ??               pixel 
   ........ 
   xxxx     unsigned byte   ??               pixel

   Pixels are organized row-wise. Pixel values are 0 to 255. 0 means background (white), 255 means foreground (black).
*/

function loadDataset(imageFile, labelFile, dataset)
{
    var imageStream = mldb.openStream(imageFile);
    var labelStream = mldb.openStream(labelFile);

    var magic = imageStream.readU32BE();
    var nitems = imageStream.readU32BE();
    var nrows = imageStream.readU32BE();
    var ncols = imageStream.readU32BE();

    plugin.log("magic", magic, "nitems", nitems, "nrows", nrows, "ncols", ncols);

    var labelMagic = labelStream.readU32BE();
    var numLabels = labelStream.readU32BE();

    var ts = new Date();

    var colNames = [];

    for (var row = 0;  row < nrows;  ++row) {
        for (var col = 0;  col < ncols;  ++col) {
            colNames.push("r" + row + "c" + col);
        }
    }

    for (var item = 0;  item < nitems;  ++item) {
        var tuples = [];
        var colIndex = 0;
        for (var row = 0;  row < nrows;  ++row) {
            var cols = imageStream.readBytes(ncols);
            for (var i = 0;  i < cols.length;  ++i)
                tuples.push([colNames[i + colIndex], cols[i], ts]);
            colIndex += ncols;
        }

        var label = labelStream.readU8();
        tuples.push(["label", label, ts]);

        dataset.recordRow("example" + item, tuples);
        if (item % 1000 == 0)
            plugin.log("did item", item, "of", nitems);
    }

    dataset.commit();
}

function getDataset(which, imageFile, labelFile)
{
    // First try to load it
    var datasetConfig = {
        type: "beh",
        id: "mnist_" + which,
        params: {
            dataFileUrl: "file://tmp/mnist_" + which + ".beh.gz"
        }
    };

    try {
        plugin.log("creating dataset with config", JSON.stringify(datasetConfig));
        return mldb.createDataset(datasetConfig);
    } catch (exc) {
        plugin.log("couldn't load dataset; creating:", JSON.stringify(exc));
        // Delete this dataset (it doesn't delete the underlying file), and create another
        mldb.perform("DELETE", "/v1/datasets/mnist_" + which);
    }

    datasetConfig.type = "beh.mutable";
    var dataset = mldb.createDataset(datasetConfig);

    loadDataset(imageFile, labelFile, dataset);

    return dataset;
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

var trainingDataset = getDataset("training", file1, file2);
var status = trainingDataset.status();

plugin.log("training dataset status", JSON.stringify(status));

var testDataset = getDataset("testing", file3, file4);
status = testDataset.status();

plugin.log("test dataset status", JSON.stringify(status));

function createAndTrainProcedure(config)
{
    var createOutput = mldb.put("/v1/procedures/mnist_svd", config);
    assertSucceeded("procedure creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/mnist_svd/runs/1", {});
    assertSucceeded("training", trainingOutput);
}

// Create a SVD procedure

var trainSvd = true;

if (trainSvd) {

    var svdConfig = {
        type: "svd.train",
        params: {
            trainingData: "select * EXCLUDING (label) from mnist_testing",
            columnOutputDataset: { "id": "svd_col_output", type: "embedding" },
            rowOutputDataset: { "id": "svd_row_output", type: "embedding" },
            numSingularValues: 50
        }
    };

    createAndTrainProcedure(svdConfig, "SVD");
}

var trainKmeans = true;

if (trainKmeans) {
    
    var kmeansConfig = {
        type: "kmeans.train",
        params: {
            trainingData: "select svd* from svd_row_output",
            outputDataset: { "id": "kmeans_row_output", type: "embedding" }
        }
    };

    createAndTrainProcedure(kmeansConfig, "k-means");
}

var trainTsne = true;

if (trainTsne) {

    var tsneConfig = {
        type: "tsne.train",
        params: {
            trainingData: "select * from svd_row_output",
            rowOutputDataset: { "id": "tsne_row_output", "type": "embedding" }
        }
    };

    createAndTrainProcedure(tsneConfig, "t-SNE");
}

var mergedConfig = {
    type: "merged",
    id: "cls_training",
    params: {
        "datasets": [
            { "id": "mnist_testing" },
            { "id": "svd_row_output" },
            { "id": "tsne_row_output" },
            { "id": "kmeans_row_output" }
        ]
    }
};

var mergedDataset = mldb.createDataset(mergedConfig);


var trainClassifier = true;

if (trainClassifier) {  

    var trainClassifierProcedureConfig = {
        type: "classifier.train",
        params: {
            trainingData: { 
                where: "rowHash() % 2 = 1",
                select: "* EXCLUDING (label)",
                from: "cls_training" 
            },
            configuration: {
                "bbdt": {
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
                }
            },
            algorithm: "bbdt",
            modelFileUrl: "file://tmp/mnist.cls",
            label: "label > 4",
            weight: "1.0"
        }
    };

    createAndTrainProcedure(trainClassifierProcedureConfig, "classifier");
}

var functionConfig = {
    id: "testClassifierFunction",
    type: "classifier",
    params: {
        modelFileUrl: "file://mnist.cls"
    }
};

var testClassifier = false;

if (testClassifier) {

    var testClassifierProcedureConfig = {
        type: "classifier.test",
        params: {
            testingDataset: { id: "cls_training" },
            outputDataset: { id: "cls_test_results", type: "beh.mutable" },
            score: "APPLY FUNCTION { 'type': 'classifier', 'params': { 'modelFileUrl': 'file://mnist.cls'}} WITH (* EXCLUDING (LABEL)) EXTRACT (score)",
            where: "rowHash() % 2 = 0",
            label: "label > 4",
            weight: "1.0"
        }
    };
    
    createAndTrainProcedure(testClassifierProcedureConfig, "test classifier");
}

// Deploy the classifier to use.  This means an SVD and an output.

var deployClassifier = false;

if (deployClassifier) {
    var svdConfig = {
        id: "applySvdFunction2",
        type: "svd.embedRow",
        params: { svdUri: "svd.json.gz", maxSingularValues: 100 }
    };

    var classifierConfig = {
        id: "classifierFunction2",
        type: "classifier",
        params: {
            modelFileUrl: "file://mnist.cls"
        }
    };
    
    var functionConfig = {
        id: "predictme",
        type: "serial",
        params: {
            "steps": [
                { id: "applySvdFunction" },
                { id: "classifierFunction" }
            ]
        }
    };
    
}

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success!"
