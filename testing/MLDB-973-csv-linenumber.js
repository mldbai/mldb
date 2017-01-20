// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://s3.amazonaws.com/benchm-ml--main/test.csv",
        outputDataset: {
            id: "test",
        },
        runOnCreation: true,     
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

var resp = mldb.get('/v1/query', { q: "select cast (rowName() as number) from test",
                                   format: 'table',
                                   headers: false });

var counts = {};

for (var i = 0;  i < resp.json.length;  ++i) {
    var num = resp.json[i][1];
    //mldb.log(num);
    counts[num] = counts[num] ? counts[num] + 1 : 1;
    if (counts[num] > 1) {
        throw "row number " + num + " occurred more than once";
    }
}

for (var i = 2;  i <= 100000;  ++i) {
    if (!counts[i])
        throw "row number " + i + " did not occur";
}

"success"
