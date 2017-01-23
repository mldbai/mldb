# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json, random, datetime, os

## Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy',
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(250):
    label = random.random() < 0.2
    row = []
    for j in range(1, 100):
        row.append(["feat" + str(j), random.gauss(5 if label else 15, 3), now])

    dataset.record_row("u%d" % i, row)

dataset.commit()


# train a t-SNE by setting a limit and offset and make sure the embedded dataset
# contains the right rows
conf = {
    "type": "tsne.train",
    "params": {
        "trainingData": "select * from toy limit 200",
        "rowOutputDataset": {"id": "toy_tsne", "type": "embedding" },
        "modelFileUrl": "file://tmp/MLDB-284-tsne.bin.gz",
        "functionName": "tsne_embed",
        "runOnCreation": True
    }
}

def runProcedure():
    rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
    mldb.log(json.loads(rez["response"]))
    assert rez["statusCode"] == 201

    rez = mldb.perform("GET", "/v1/query", [["q", "select * from toy_tsne"]])
    return json.loads(rez["response"])

def applyFunction(row):
    rez = mldb.perform("GET", "/v1/functions/tsne_embed/application", [['input', {'embedding':row}]])
    mldb.log(rez)
    return rez

firstRunRows = runProcedure()
assert len(firstRunRows) == 200, 'the limit to 200 did not work'

row = {}
for j in range(1, 100):
    row["feat" + str(j)] = random.gauss(5 if label else 15, 3)

assert applyFunction(row)['statusCode'] == 500, 'expected a clear failure on t-sne apply'

mldb.script.set_return("success")

