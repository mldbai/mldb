#
# MLDB-1081-getEmbedding_honors_limit_offset.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import random, datetime

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


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

# Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy',
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(500):
    label = random.random() < 0.2
    dataset.record_row("u%d" % i,
                       [["feat1", random.gauss(5 if label else 15, 3), now],
                        ["feat2", random.gauss(-5 if label else 10, 10), now],
                        ["label", label, now]])

dataset.commit()


# train a t-SNE by setting a limit and offset and make sure the embedded
# dataset contains the right rows
conf = {
    "type": "tsne.train",
    "params": {
        "trainingData": {
            "from" : { "id" : "toy"},
            "limit" : 200
        },
        "rowOutputDataset": {"id": "toy_tsne", "type": "embedding"},
        "modelFileUrl": "file://tmp/MLDB-1081-tsne.bin.gz",
        "functionName": "tsne_embed",
        "runOnCreation": True
    }
}


def run_procedure():
    rez = mldb.put("/v1/procedures/rocket_science", conf)
    mldb.log(rez)

    rez = mldb.get("/v1/query", q="select * from toy_tsne")
    return rez.json()


first_run_rows = run_procedure()
assert len(first_run_rows) == 200, 'the limit to 200 did not work'

secondRunRows = run_procedure()
assert len(secondRunRows) == 200, 'the limit to 200 did not work'

# make sure the embeddings are identical
for idx, row in enumerate(first_run_rows):
    assert row['rowName'] == secondRunRows[idx]['rowName'], \
        'problem with the test - the test assumes row order will be the same'
    assert row['columns'][0][1] == secondRunRows[idx]['columns'][0][1], \
        'rows differ in x coordinate'
    assert row['columns'][1][1] == secondRunRows[idx]['columns'][1][1], \
        'rows differ in y coordinate'

# make sure that the offset works
offset = 10
conf['params']['trainingData']['offset'] = offset
third_run_rows = run_procedure()
assert len(third_run_rows) == 200, \
    'the limit to 200 did not work when an offset is set'

for idx, row in enumerate(first_run_rows[offset:]):
    assert row['rowName'] == third_run_rows[idx]['rowName'], \
        'problem with the test - the test assumes row order will be the same'
    assert row['columns'][0][1] != third_run_rows[idx]['columns'][0][1], \
        'rows must differ in x coordinate'
    assert row['columns'][1][1] != third_run_rows[idx]['columns'][1][1], \
        'rows must differ in y coordinate'

# test specific error messages
conf['params']['trainingData']['offset'] = 1000
try:
    mldb.put("/v1/procedures/rocket_science", conf)
except mldb_wrapper.ResponseException as exc:
    rez = exc.response
else:
    assert False, 'Should have failed with a 400'
assert rez.text.find('offset') != -1, \
    'expected a mention of offset in the error message'

conf['params']['trainingData']['offset'] = 0
conf['params']['trainingData']['limit'] = 0
try:
    mldb.put("/v1/procedures/rocket_science", conf)
except mldb_wrapper.ResponseException as exc:
    rez = exc.response
else:
    assert False, 'Should have failed with a 400'

assert rez.text.find('limit') != -1, \
    'expected a mention of limit in the error message'

mldb.script.set_return("success")
