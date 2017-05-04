# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# reddit_benchmark.py
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

res = mldb.perform('PUT', '/v1/procedures/reddit_raw', [], {
    "type": "import.text",
    "params": {
        "delimiter": '',
        "quoteChar": '',
        'runOnCreation': True,
        'outputDataset':{'id' : 'reddit_raw', 'type' : mutable},
        "dataFileUrl": "s3://private-mldb-ai/reddit-benchmark/reddit_user_posting_behavior.csv.gz"
    }
})
assert res['statusCode'] == 201, str(res)

metrics = {}
prefix = 'reddit_'

with Benchmark(metrics, get_name(prefix + 'transform_tokenize')):
    res = mldb.perform('PUT', '/v1/procedures/reddit_import', [], {
        "type": "transform",
        "params": {
            "inputData": "select tokenize(lineText, {offset: 1, value: 1}) as * from reddit_raw",
            "outputDataset": {'id' : "reddit_dataset", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)


with Benchmark(metrics, get_name(prefix + 'svd_train')):
    res= mldb.perform('PUT', '/v1/procedures/reddit_svd', [], {
        "type" : "svd.train",
        "params" : {
            "trainingData" : { "from" : {"id" : "reddit_dataset"},
                               "select": "COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC, "
                               "columnName() LIMIT 4000)"
                           },
            "columnOutputDataset" : {'id': "reddit_svd_embedding", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)


with Benchmark(metrics, get_name(prefix + 'kmeans_train')):
    res = mldb.perform('PUT', '/v1/procedures/reddit_kmeans', [], {
        "type" : "kmeans.train",
        "params" : {
            "trainingData" : "SELECT * FROM reddit_svd_embedding",
            "outputDataset" : {'id' :"reddit_kmeans_clusters", 'type' : mutable},
            "numClusters" : 20,
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)


with Timer() as metrics[get_name(prefix + 'tsne_train_runtime')]:
    res = mldb.perform('PUT', '/v1/procedures/reddit_tsne', [], {
        "type" : "tsne.train",
        "params" : {
            "trainingData" : "SELECT * FROM reddit_svd_embedding",
            "rowOutputDataset" : {'id' : "reddit_tsne_embedding", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

with Timer() as metrics[prefix + 'transpose_runtime']:
    res = mldb.perform('PUT', '/v1/datasets/reddit_dataset_transposed', [], {
        "type": "transposed",
        "params": {
            "dataset": { "id": "reddit_dataset" }
        }
    })
    assert res['statusCode'] == 201, str(res)

with Benchmark(metrics, get_name(prefix + 'transform_column_count')):
    res = mldb.perform('PUT', '/v1/procedures/reddit_count_users', [], {
        "type": "transform",
        "params": {
            "inputData": "select columnCount() AS numUsers named rowName() + '|1' from reddit_dataset_transposed",
            "outputDataset": {'id' : "reddit_user_counts", 'type' : mutable},
            "runOnCreation": True
        }
    })
    assert res['statusCode'] == 201, str(res)

mldb.log(str(metrics))
log_benchmark(metrics)
mldb.script.set_return("success")
