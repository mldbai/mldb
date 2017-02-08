#
# MLDB-1364_dataset_cant_be_overwritten.py
# Francois Maillet, 5 fevrier 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class MLDB1364Test(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("b",[["x", 2, 0], ["y", 25, 0]])
        ds.record_row("c",[["y", 3, 0]])
        ds.commit()
        
    def test_svd(self):
        
        # this is throwing because the not_yet_created dataset 
        # does not exist

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/datasets/training_data",{
                "type": "merged",
                "params": {
                    "datasets": [
                        {"id": "sample"},
                        {"id": "not_yet_created"} # attention
                    ]
                }
            })

        # we want to store output in 'not_yet_created'
        # the fact we tried to access 'not_yet_created' above
        # makes the first attempt to create it fail
        
        mldb.put("/v1/procedures/train_svd", {
            "type": "svd.train",
            "params": {
                "rowOutputDataset": "not_yet_created", # attention
                "outputColumn": "svd.embedding.00",
                "modelFileUrl": "file://tmp/svd.bin.test.gz",
                "trainingData": "select * from sample",
                "numSingularValues": 1,
                "runOnCreation": True
            }
        })
        

        # this should now work
        mldb.get("/v1/query", q="select x from not_yet_created")


mldb.run_tests()

