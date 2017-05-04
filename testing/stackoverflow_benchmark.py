# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# stackoverflow_benchmark.py
# Mich, 2015-11-27
# Copyright (c) 2015 mldb.ai inc. All rights reserved.
#

import sys
import os
sys.path.append(os.getcwd() + '/pro/testing')
from benchmark_utils import log_benchmark, Timer, Benchmark, is_sparse_mode, \
                            get_name

if False:
    mldb = None

if is_sparse_mode(mldb):
    mutable = 'sparse.mutable'
else:
    mutable = 'beh.mutable'

res = mldb.perform('PUT', '/v1/procedures/so_raw', [], {
    "type": "import.text",
    "params": {
        "delimiter": '',
        "quoteChar": '',
        'outputDataset': {'id' : 'so_raw', 'type' : mutable},
        'runOnCreation': True,
        "dataFileUrl": "s3://private-mldb-ai/stackoverflow-benchmark/stackoverflow.csv.gz"
    }
})
assert res['statusCode'] == 201, str(res)

metrics = {}
prefix = 'stackoverflow_'

with Timer() as metrics[get_name(prefix + 'transform_tokenize') + '_runtime']:
    res = mldb.perform('POST', '/v1/procedures', [], {
        "id": "so_import",
        "type": "transform",
        "params": {
            "inputData": "select tokenize(lineText, {splitChars: '\"\r<>', quoteChar: ''}) as * from so_raw",
            "outputDataset": {'id' : "so_tags", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

with Timer() as metrics[get_name(prefix + 'transpose') + '_runtime']:
    res = mldb.perform('PUT', '/v1/datasets/so_transpose', [], {
        "type": "transposed",
        "params": {
            "dataset": { "id": "so_tags", 'type' : mutable},
        }
    })
    assert res['statusCode'] == 201, str(res)

with Timer() as metrics[get_name(prefix + 'transform') + '_runtime']:
    res = mldb.perform('POST', '/v1/procedures', [], {
        "id": "so_counts",
        "type": "transform",
        "params": {
            "inputData": "select columnCount() AS numQuestions named rowName() + '|1' from so_transpose",
            "outputDataset": {'id' : "so_counts", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

with Timer() as metrics[get_name(prefix + 'svd_train') + '_runtime']:
    res = mldb.perform('POST', '/v1/procedures', [], {
        "id": "so_svd",
        "type" : "svd.train",
        "params" : {
            "trainingData" : {"from" : {"id" : "so_tags"},
                              "select": "COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC, "
                              "columnName() LIMIT 6000)",
                          },
            "columnOutputDataset" : {'id' : "so_svd_embedding", 'type' : mutable},
            "numSingularValues": 100,
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

with Timer() as metrics[get_name(prefix + 'kmeans_train') + '_runtime']:
    res = mldb.perform('POST', '/v1/procedures', [], {
        "id" : "so_kmeans",
        "type" : "kmeans.train",
        "params" : {
            "trainingData" : "SELECT * FROM so_svd_embedding",
            "outputDataset" : {'id' : "so_kmeans_clusters", 'type' : mutable},
            "numClusters" : 20,
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

with Benchmark(metrics, get_name(prefix + 'tsne_train')):
    res = mldb.perform('POST', '/v1/procedures', [], {
        "id": "so_tsne",
        "type" : "tsne.train",
        "params" : {
            "trainingData" : "SELECT * FROM so_svd_embedding",
            "rowOutputDataset" : {'id' : "so_tsne_embedding", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

mldb.log(str(metrics))
log_benchmark(metrics)
mldb.script.set_return("success")
