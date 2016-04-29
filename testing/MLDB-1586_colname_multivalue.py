#
# 2016-01-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1586Test(MldbUnitTest):  

    @unittest.expectedFailure
    def test_colnames(self):
        mldb.put('/v1/datasets/example', {"type":"sparse.mutable"})
        mldb.post('/v1/datasets/example/rows', { "rowName": "r1", 
            "columns": [ ["a", 1, 0], ["a", 2, 2], ["b", 2, 0] ] })
        mldb.post('/v1/datasets/example/rows', { "rowName": "r2", 
            "columns": [ ["a", 3, 0], ["b", 4, 0] ] })
        mldb.post('/v1/datasets/example/commit')
        
        self.assertTableResultEquals(
            mldb.query("select a from example"),
            [
                ["_rowName", "a"],
                [      "r1",  2 ],
                [      "r2",  3 ]
            ]
        )



mldb.run_tests()


