#
# 2016-01-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class BehLimitTest(MldbUnitTest):

    def test_limits(self):
        mldb.put('/v1/datasets/example', { "type":"beh.mutable" })
        mldb.post('/v1/datasets/example/rows', {
            "rowName": "r1", "columns": [ ["c", 1, 0], ]
        })
        mldb.post('/v1/datasets/example/rows', {
            "rowName": "r2", "columns": [ ["c", 2, 0], ]
        })
        mldb.post("/v1/datasets/example/commit")

        self.assertTableResultEquals(
            mldb.query("select * from example limit 2"), 
            [
                ["_rowName", "c"],
                [      "r1",  1 ],
                [      "r2",  2 ]
            ]
        )


mldb.run_tests()
