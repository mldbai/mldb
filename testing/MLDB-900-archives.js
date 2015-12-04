// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var dir = mldb.ls("archive+http://www.cs.waikato.ac.nz/~ml/weka/agridatasets.jar");
mldb.log(dir);

assertEqual(dir.objects["archive+http://www.cs.waikato.ac.nz/~ml/weka/agridatasets.jar#eucalyptus.arff"].exists, true);


var stream = mldb.openStream("archive+http://www.cs.waikato.ac.nz/~ml/weka/agridatasets.jar#eucalyptus.arff");

var numLines = 0;

while (!stream.eof()) {
    try {
        var line = stream.readLine();
        mldb.log(line);
        ++numLines;
    } catch (e) {
    }
}

mldb.log(numLines, " lines");
assertEqual(numLines, 823);

var dir = mldb.ls("archive+http://files.grouplens.org/datasets/movielens/ml-20m.zip");
mldb.log(dir);

var dsConfig = {
    type:"text.csv.tabular", 
    params: {
        dataFileUrl:"archive+http://files.grouplens.org/datasets/movielens/ml-20m.zip#ml-20m/links.csv",
        limit: 1000
    }
};

var dataset = mldb.createDataset(dsConfig);

mldb.log(dataset.status);


"success"
