#
# MLDB-1277-pooling-performance.py
# 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest

class PoolingPerformanceTest(unittest.TestCase):

    def test_it(self):
        mldb.create_dataset({
            "id": "reddit_raw", "type": "text.line",
            "params": {
                "dataFileUrl": "http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz"
            }
        })

        mldb.create_dataset({
            "id": "reddit_svd_embedding",
            "type": "text.csv.tabular",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/reddit_embedding.csv.gz",
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
