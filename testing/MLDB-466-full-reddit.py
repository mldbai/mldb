#
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class RedditTest(MldbUnitTest):  

    def test_pipeline(self):

        mldb.put('/v1/procedures/import_reddit', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "http://public.mldb.ai/reddit.csv.gz",
                'delimiter':'', 
                'quotechar':'',
                'outputDataset': 'reddit_raw',
                'limit': 5000,
                'runOnCreation': True
            } 
        })
        mldb.query("select * from reddit_raw limit 5")

        mldb.put('/v1/procedures/reddit_import', {
            "type": "transform",
            "params": {
                "inputData": """select tokenize(lineText, {offset: 1, value: 1}) as * 
                from reddit_raw""",
                "outputDataset": "reddit_dataset",
                "runOnCreation": True
            }
        })
        mldb.query("select * from reddit_dataset limit 5")


        mldb.put('/v1/procedures/reddit_svd', {
            "type" : "svd.train",
            "params" : {
                "trainingData" : """
                    SELECT 
                        COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC, 
                            columnName() LIMIT 4000) 
                    FROM reddit_dataset
                """,
                "columnOutputDataset" : "reddit_svd_embedding",
                "modelFileUrl": "file://tmp/MLDB-466.svd",
                "functionName": "embedder",
                "runOnCreation": True
            }
        })
        mldb.query("select * from reddit_svd_embedding limit 5")

        mldb.query("""
            select rowName(), rowPath(), rowPathElement(0) 
            from reddit_svd_embedding 
            limit 5
            """)
        
        mldb.query("""
            select embedder({ row: {HongKong: 1} })
            """)

        mldb.put("/v1/functions/nearest_subreddit", {
            "type": "embedding.neighbors",
            "params": {
                "dataset": "reddit_svd_embedding",
                "defaultNumNeighbors": 1
            }
        })

        mldb.log("querying for nearest subreddit")
        
        mldb.query("""
            select nearest_subreddit({ coords: 
                embedder({ row: {HongKong: 1} })
            })
        """)


        mldb.put('/v1/procedures/reddit_kmeans', {
            "type" : "kmeans.train",
            "params" : {
                "trainingData" : "select * from reddit_svd_embedding",
                "outputDataset" : "reddit_kmeans_clusters",
                "numClusters" : 20,
                "runOnCreation": True
            }
        })
        mldb.query("select * from reddit_kmeans_clusters limit 5")


        mldb.put('/v1/procedures/reddit_tsne', {
            "type" : "tsne.train",
            "params" : {
                "trainingData" : "select * from reddit_svd_embedding",
                "rowOutputDataset" : "reddit_tsne_embedding",
                "runOnCreation": True
            }
        })
        mldb.query("select * from reddit_tsne_embedding limit 5")


        mldb.put('/v1/procedures/reddit_count_users', {
            "type": "transform",
            "params": {
                "inputData": """select columnCount() as numUsers 
                    from transpose(reddit_dataset)""",
                "outputDataset": "reddit_user_counts",
                "runOnCreation": True
            }
        })
        mldb.query("select * from reddit_user_counts limit 5")

        mldb.query("""
            select *, quantize(x, 7) as grid_x, quantize(y, 7) as grid_y 
            named regex_replace(rowName(), '\|1', '') 
            from merge(reddit_tsne_embedding, reddit_kmeans_clusters) as r
                join reddit_user_counts as c on c.rowName() = r.rowPathElement(0)
            where cluster is not null 
            order by numUsers desc
        """)

mldb.run_tests()
