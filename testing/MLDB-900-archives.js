// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

var dir = mldb.ls("archive+http://public.mldb.ai/ml-20m.zip");
mldb.log(dir);

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "archive+http://public.mldb.ai/ml-20m.zip#ml-20m/links.csv",
        outputDataset: {
            id: "csv",
        },
        runOnCreation: true,
        limit: 1000
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

mldb.log(res);


"success"
