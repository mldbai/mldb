# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, random, datetime, os

def delete_keys_from_dict(dict_del, lst_keys):
    for k in lst_keys:
        try:
            del dict_del[k]
        except KeyError:
            pass
    for v in dict_del.values():
        if isinstance(v, dict):
            delete_keys_from_dict(v, lst_keys)

    return dict_del

## Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy',
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(500):
    label = random.random() < 0.2
    dataset.record_row("u%d" % i, [["feat1", random.gauss(5 if label else 15, 3), now],
                                   ["feat2", random.gauss(-5 if label else 10, 10), now],
                                   ["label", label, now]])

dataset.commit()


# train a t-SNE by setting a limit and offset and make sure the embedded dataset
# contains the right rows
conf = {
    "type": "tsne.train",
    "params": {
        "trainingData": { "from" : { "id" : "toy"},
                          "limit" : 200
                      },
        "rowOutputDataset": {"id": "toy_tsne", "type": "embedding" },
        "modelFileUrl": "file://tmp/MLDB-1081-tsne.bin.gz",
        "functionName": "tsne_embed",
        "runOnCreation": True
    }
}

def runProcedure():
    rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
    mldb.log(rez['response'])
    assert rez["statusCode"] == 201, "creation of procedure failed"

    rez = mldb.perform("GET", "/v1/query", [["q", "select * from toy_tsne"]])
    return json.loads(rez["response"])


firstRunRows = runProcedure()
assert len(firstRunRows) == 200, 'the limit to 200 did not work'

secondRunRows = runProcedure()
assert len(secondRunRows) == 200, 'the limit to 200 did not work'

# make sure the embeddings are identical
for idx, row in enumerate(firstRunRows):
    assert row['rowName'] == secondRunRows[idx]['rowName'], 'problem with the test - the test assumes row order will be the same'
    assert row['columns'][0][1] == secondRunRows[idx]['columns'][0][1], 'rows differ in x coordinate'
    assert row['columns'][1][1] == secondRunRows[idx]['columns'][1][1], 'rows differ in y coordinate'
    

# make sure that the offset works
offset = 10
conf['params']['trainingData']['offset'] = offset
thirdRunRows = runProcedure()
assert len(thirdRunRows) == 200, 'the limit to 200 did not work when an offset is set'

for idx, row in enumerate(firstRunRows[offset:]):
    assert row['rowName'] == thirdRunRows[idx]['rowName'], 'problem with the test - the test assumes row order will be the same'
    assert row['columns'][0][1] != thirdRunRows[idx]['columns'][0][1], 'rows must differ in x coordinate'
    assert row['columns'][1][1] != thirdRunRows[idx]['columns'][1][1], 'rows must differ in y coordinate'

# test specific error messages
conf['params']['trainingData']['offset'] = 1000
rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
assert rez["statusCode"] == 400, 'expected an error when offset is too large'
assert rez['response'].find('offset') != -1, 'expected a mention of offset in the error message'

conf['params']['trainingData']['offset'] = 0
conf['params']['trainingData']['limit'] = 0
rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
assert rez["statusCode"] == 400, 'expected an error when limit is 0'
assert rez['response'].find('limit') != -1, 'expected a mention of limit in the error message'

mldb.script.set_return("success")

