// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


// Put without dataset name should throw

var resp = mldb.put("/v1/datasets", {}, {});

plugin.log(resp);

if (resp.responseCode != 404) {
    throw "Expected response code 404 from put to datasets";
}

// Put of empty dataset config should throw

var resp = mldb.put("/v1/datasets/test1", {});

plugin.log(resp);

if (resp.responseCode != 400) {
    throw "Expected response code 400 from response";
}

// Put of dataset config with type should work

var resp = mldb.put("/v1/datasets/test5", {type:"sparse.mutable"});

plugin.log(resp);

if (resp.responseCode != 201) {
    throw "Expected response code 201 from response";
}

// Put of dataset config with wrong id should not work

var resp = mldb.put("/v1/datasets/test2", {id: "test1", type:"sparse.mutable"});

plugin.log(resp);

if (resp.responseCode != 400) {
    throw "Expected response code 400 from put of dataset config with wrong id";
}

// Post of dataset config with empty id should work

var resp = mldb.post("/v1/datasets", { type:"sparse.mutable"});

plugin.log(resp);

if (resp.responseCode != 201) {
    throw "Expected response code 404 from post of dataset config with empty id";
}


// Post of dataset config with id should work

var resp = mldb.post("/v1/datasets", {id: "test3", type:"sparse.mutable"});

plugin.log(resp);

if (resp.responseCode != 201) {
    throw "Expected response code 201 from put of dataset config with wrong id";
}

"success"
