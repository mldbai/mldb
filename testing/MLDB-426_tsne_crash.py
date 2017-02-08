#
# MLDB-426_tsne_crash.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import random

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({
        "type": "sparse.mutable",
        "id": "x"
    })
for r in range(1000):
    dataset.record_row(
        "r%d"%r,
        [ ["c%d"%c, random.random(), 0] for c in range(100)]
        )
dataset.commit()


try:
    mldb.put("/v1/procedures/svd", {
        "type":"svd.train",
        "params" : {
            "trainingData" : "select * from x",
            "columnOutputDataset" :  {"id" : "svd", "type" : "sparse.mutable"},
            "rowOutputDataset" : {
                "id": "svd_embed",
                "type": "embedding",
                "address": "svd_embed"
            }
        }
    })
except mldb_wrapper.ResponseException as exc:
    pass

try:
    mldb.post("/v1/procedures/svd/runs")
except mldb_wrapper.ResponseException as exc:
    pass

try:
    mldb.put("/v1/procedures/tsne", {
        "type": "tsne.train",
        "params": {
            "trainingData": {"from": {"id": "svd_embed"}},
            "rowOutputDataset": {
                "id": "tsne_output",
                "type": "sparse.mutable",
                "address": "tsne_output"
            }
        }
    })
except mldb_wrapper.ResponseException as exc:
    pass

try:
    mldb.post("/v1/procedures/tsne/runs")
except mldb_wrapper.ResponseException as exc:
    pass

mldb.script.set_return("success")
