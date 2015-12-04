# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import random

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


mldb.perform("PUT", "/v1/procedures/svd", [],
    {
        "type":"svd.train", 
        "params" : {
        "trainingDataset" : {"id" : "x"},
        "columnOutputDataset" :  {"id" : "svd", "type" : "sparse.mutable"},
        "rowOutputDataset" : {"id": "svd_embed", "type": "embedding",
            "address": "svd_embed"}
        }
    }
)
mldb.perform("POST", "/v1/procedures/svd/runs", [], {})

mldb.perform("PUT", "/v1/procedures/tsne", [],
    {
        "type":"tsne.train", 
        "params":{
            "trainingDataset":{"id":"svd_embed"},
            "rowOutputDataset": {"id": "tsne_output", "type": "sparse.mutable",
            "address": "tsne_output"}
        }
    }
)
mldb.perform("POST", "/v1/procedures/tsne/runs", [], {})

mldb.script.set_return("success")
