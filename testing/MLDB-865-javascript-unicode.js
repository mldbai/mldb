/**
 * MLDB-865-javascript-unicode.js
 * mldb.ai inc, 2015
 * Mich, 2016-02-10
 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(response)
{
    plugin.log(response);

    if (!succeeded(response)) {
        throw "failed: " + JSON.stringify(response);
    }
}


var ds = mldb.createDataset({"id": "hellô", "type":"embedding"});

ds.commit();

assertSucceeded(mldb.get("/v1/query", {"q": 'select * from "hellô"'}));

assertSucceeded(mldb.get("/v1/datasets"));

assertSucceeded(mldb.put("/v1/datasets/hôwdy", {"type": 'embedding'}));

assertSucceeded(mldb.get("/v1/datasets"));

assertSucceeded(mldb.post("/v1/datasets", {"type": 'embedding', "id": "hî" }));

assertSucceeded(mldb.perform("GET", "/v1/datasets"));

function executeSequence(_id) {
    var url = '/v1/datasets/' + encodeURIComponent(_id)
    mldb.log(url)

    mldb.put(url, {
        'type' : 'sparse.mutable'
    })
    res = mldb.get(url)
    assertSucceeded(res);
    if (res.json["id"] != _id) {
        throw res.json["id"] + " != " + _id;
    }

    res = mldb.del(url);
    assertSucceeded(res);
    res = mldb.get(url);
    if (res.responseCode != 404) {
        throw res.responseCode + " != 404";
    }

    mldb.post('/v1/datasets', {
        'id' : _id,
        'type' : 'sparse.mutable'
    })
    res = mldb.get(url)
    assertSucceeded(res);
    if (res.json["id"] != _id) {
        throw res.json["id"] + " != " + _id;
    }

    res = mldb.del(url);
    assertSucceeded(res);
    res = mldb.get(url);
    if (res.responseCode != 404) {
        throw res.responseCode + " != 404";
    }
}

executeSequence("françois");
executeSequence("françois/michel");
executeSequence("françois michel");
executeSequence('"françois says hello/goodbye, eh?"');

res = mldb.put('/v1/datasets/ds', {
    'type' : 'sparse.mutable'
});
assertSucceeded(res);
res = mldb.post('/v1/datasets/ds/commit');
assertSucceeded(res);

res = mldb.put('/v1/procedures/' + encodeURIComponent('françois'), {
    'type' : 'transform',
    'params' : {
        'inputData' : 'SELECT * FROM ds',
        'outputDataset' : {
            'id' : 'outDs',
            'type' : 'sparse.mutable'
        }
    }
});
assertSucceeded(res);

url = '/v1/procedures/' + encodeURIComponent('françois') + '/runs/'
      + encodeURIComponent('michêl');
res = mldb.put(url, {})
assertSucceeded(res);
res = mldb.get(url)
assertSucceeded(res);

"success"
