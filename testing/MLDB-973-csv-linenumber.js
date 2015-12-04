// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


var dsconfig = {
    id: 'test',
    type: "text.csv.tabular",
    params: {
        //dataFileUrl: "file://test.csv"
        dataFileUrl: "https://s3.amazonaws.com/benchm-ml--main/test.csv"
    }
};

var dataset = mldb.createDataset(dsconfig);

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
