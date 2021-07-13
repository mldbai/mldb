#
# MLDB-1277-pooling-performance.py
# 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from mldb import mldb

import unittest

class PoolingPerformanceTest(unittest.TestCase):

    def test_it(self):

        res = mldb.put('/v1/procedures/import_reddit', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/mldb_test_data/reddit.csv.gz",
                "delimiter": "",
                "quoteChar": "",
                'outputDataset': {'id': 'reddit_raw', 'type': 'sparse.mutable'},
                'runOnCreation': True
            } 
        })

        res = mldb.put('/v1/procedures/import_reddit', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/mldb_test_data/reddit_embedding.csv.gz",
                "delimiter": ",",
                "quoteChar": "",
                'outputDataset': {'id': 'reddit_svd_embedding', 'type': 'tabular'},
                'runOnCreation': True
            } 
        })

        mldb.put("/v1/procedures/rename", {
            "type" : "transform",
            "params" : {
                "inputData" : "select * excluding(name) named name "
                              "from reddit_svd_embedding",
                "outputDataset" : {
                    "id" : "reddit_svd_embedding2",
                    "type" : "embedding"},
                "runOnCreation": True
            }
        })

        mldb.put("/v1/functions/pooler", {
            "type": "pooling",
            "params": {
                "embeddingDataset": "reddit_svd_embedding2"
            }
        })

        mldb.put("/v1/functions/wrapper", {
            "type": "sql.expression",
            "params": {
                "expression":
                    "pooler({words: tokenize(lineText)})[embedding] as x"
            }
        })

        mldb.log('start')
        mldb.query("select wrapper({lineText}) from reddit_raw limit 10000")
        mldb.log('stop')

if __name__ == '__main__':
    mldb.run_tests()
